/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.yarn

import java.io.{FileSystem => _, _}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.security.PrivilegedExceptionAction
import java.util.{Locale, Properties, UUID}
import java.util.zip.{ZipEntry, ZipOutputStream}

import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap, ListBuffer, Map}

import com.google.common.io.Files
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.util.StringUtils
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.{YarnClient, YarnClientApplication}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records
import org.apache.spark.{KyuubiSparkUtil, SparkConf}
import org.apache.spark.deploy.yarn.KyuubiDistributedCacheManager

import yaooqinn.kyuubi.{Logging, _}

private[kyuubi] class KyuubiYarnClient(conf: SparkConf) extends Logging {
  private[this] val hadoopConf = new YarnConfiguration(KyuubiSparkUtil.newConfiguration(conf))

  private[this] val yarnClient = YarnClient.createYarnClient()
  yarnClient.init(hadoopConf)
  yarnClient.start()

  private[this] val kyuubiJar = System.getenv("KYUUBI_JAR")
  private[this] val memory = conf.getSizeAsMb(KyuubiSparkUtil.DRIVER_MEM, "1024m").toInt
  private[this] val memoryOverhead =
    conf.getSizeAsMb(KyuubiSparkUtil.DRIVER_MEM_OVERHEAD, (memory * 0.1).toInt + "m").toInt
  private[this] val cores = conf.getInt(KyuubiSparkUtil.DRIVER_CORES, 1)
  private[this] val principal = conf.get(KyuubiSparkUtil.PRINCIPAL, "")
  private[this] val keytabOrigin = conf.get(KyuubiSparkUtil.KEYTAB, "")
  private[this] val loginFromKeytab = principal.nonEmpty && keytabOrigin.nonEmpty
  private[this] val keytabForAM: String = if (loginFromKeytab) {
    new File(keytabOrigin).getName + "-" + UUID.randomUUID()
  } else {
    null
  }
  private[this] val stagingHome = FileSystem.get(hadoopConf).getHomeDirectory
  private[this] var appId: ApplicationId = _
  private[this] var appStagingDir: Path = _

  def submit(): Unit = {
    try {
      val app = yarnClient.createApplication()
      val appRes = app.getNewApplicationResponse
      appId = appRes.getApplicationId
      appStagingDir = new Path(stagingHome, buildPath(KYUUBI_STAGING, appId.toString))
      val containerContext = createContainerLaunchContext()
      val appContext = createApplicationSubmissionContext(app, containerContext)
      info(s"Submitting application $appId to ResourceManager")
      yarnClient.submitApplication(appContext)
    } catch {
      case e: Throwable =>
        if (appId != null) cleanupStagingDir()
        throw e
    }
  }

  /**
   * Cleanup application staging directory.
   */
  private[this] def cleanupStagingDir(): Unit = {
    def cleanupStagingDirInternal(): Unit = {
      try {
        val fs = appStagingDir.getFileSystem(hadoopConf)
        if (fs.delete(appStagingDir, true)) {
          info(s"Deleted staging directory $appStagingDir")
        }
      } catch {
        case ioe: IOException =>
          warn("Failed to cleanup staging dir " + appStagingDir, ioe)
      }
    }

    if (loginFromKeytab) {
      val currentUser = UserGroupInformation.getCurrentUser
      currentUser.reloginFromKeytab()
      currentUser.doAs(new PrivilegedExceptionAction[Unit] {
        override def run(): Unit = {
          cleanupStagingDirInternal()
        }
      })
    } else {
      cleanupStagingDirInternal()
    }
  }

  private[this] def createContainerLaunchContext(): ContainerLaunchContext = {
    val launchEnv = setupLaunchEnv()
    val localResources = prepareLocalResources()

    val amContainer = Records.newRecord(classOf[ContainerLaunchContext])
    amContainer.setLocalResources(localResources.asJava)
    amContainer.setEnvironment(launchEnv.asJava)

    val javaOpts = ListBuffer[String]()
    javaOpts += "-Xmx" + memory + "m"
    javaOpts += "-Djava.io.tmpdir=" +
      buildPath(Environment.PWD.$$(), YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR)

    conf.getOption(KyuubiSparkUtil.DRIVER_EXTRA_JAVA_OPTIONS).foreach { opts =>
      javaOpts ++= KyuubiSparkUtil.splitCommandString(opts)
        .map(KyuubiSparkUtil.substituteAppId(_, appId.toString))
        .map(KyuubiSparkUtil.escapeForShell)
    }

    val amClass = classOf[KyuubiAppMaster].getCanonicalName

    val amArgs = Seq(amClass) ++
      Seq("--properties-file", buildPath(Environment.PWD.$$(), SPARK_CONF_DIR, SPARK_CONF_FILE))

    // Command for the ApplicationMaster
    val commands =
      Seq(Environment.JAVA_HOME.$$() + "/bin/java", "-server") ++
      javaOpts ++ amArgs ++
      Seq(
        "1>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout",
        "2>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")

    val printableCommands = commands.map(s => if (s == null) "null" else s).toList
    amContainer.setCommands(printableCommands.asJava)
    amContainer
  }

  /**
   * Set up the context for submitting our ApplicationMaster.
   * This uses the YarnClientApplication not available in the Yarn alpha API.
   */
  private[this] def createApplicationSubmissionContext(
      app: YarnClientApplication,
      containerContext: ContainerLaunchContext): ApplicationSubmissionContext = {
    val appContext = app.getApplicationSubmissionContext
    appContext.setApplicationName(s"$KYUUBI_YARN_APP_NAME[$KYUUBI_VERSION]")
    appContext.setQueue(conf.get(KyuubiSparkUtil.QUEUE, YarnConfiguration.DEFAULT_QUEUE_NAME))
    appContext.setAMContainerSpec(containerContext)
    appContext.setApplicationType(s"$KYUUBI_YARN_APP_NAME[${KyuubiSparkUtil.SPARK_VERSION}]")
    conf.getOption(KyuubiSparkUtil.MAX_APP_ATTEMPTS) match {
      case Some(v) => appContext.setMaxAppAttempts(v.toInt)
      case None => debug(s"${KyuubiSparkUtil.MAX_APP_ATTEMPTS} is not set. " +
        "Cluster's default value will be used.")
    }
    val capability = Records.newRecord(classOf[Resource])
    capability.setMemory(memory + memoryOverhead)
    capability.setVirtualCores(cores)
    appContext.setResource(capability)
    appContext
  }
  /**
   * Set up the environment for launching our AM container runs Kyuubi Server.
   */
  private def setupLaunchEnv(): ENV = {
    info("Setting up the launch environment for our Kyuubi Server container")
    val env = new ENV
    populateClasspath(hadoopConf, conf, env)
    env("KYUUBI_YARN_STAGING_DIR") = appStagingDir.toString
    val amEnvPrefix = "spark.yarn.appMasterEnv."
    conf.getAll
      .filter(_._1.startsWith(amEnvPrefix))
      .map { case (k, v) => (k.stripPrefix(amEnvPrefix), v) }
      .foreach { case (k, v) => addPathToEnvironment(env, k, v) }
    env
  }

  private[this] def prepareLocalResources(): RESOURCES = {
    info("Preparing resources for our AM container to start Kyuubi Server")
    val fs = appStagingDir.getFileSystem(hadoopConf)
    val localResources = new RESOURCES
    FileSystem.mkdirs(fs, appStagingDir, new FsPermission(STAGING_DIR_PERMISSION))
    val symlinkCache: Map[URI, Path] = HashMap[URI, Path]()
    val statCache: Map[URI, FileStatus] = HashMap[URI, FileStatus]()

    /**
     * Distribute a file to the cluster.
     *
     * If the file's path is a "local:" URI, it's actually not distributed. Other files are copied
     * to HDFS (if not already there) and added to the application's distributed cache.
     *
     * @param path URI of the file to distribute.
     * @param resType Type of resource being distributed.
     * @param destName Name of the file in the distributed cache.
     * @param targetDir Subdirectory where to place the file.
     * @return A 2-tuple. First item is whether the file is a "local:" URI. Second item is the
     *         localized path for non-local paths, or the input `path` for local paths.
     *         The localized path will be null if the URI has already been added to the cache.
     */
    def distribute(
        path: String,
        resType: LocalResourceType = LocalResourceType.FILE,
        destName: Option[String] = None,
        targetDir: Option[String] = None): (Boolean, String) = {
      val trimmedPath = path.trim()
      val localURI = KyuubiSparkUtil.resolveURI(trimmedPath)
      if (localURI.getScheme != LOCAL_SCHEME) {
        val localPath = getQualifiedLocalPath(localURI, hadoopConf)
        val linkname = targetDir.map(_ + "/").getOrElse("") +
          destName.orElse(Option(localURI.getFragment)).getOrElse(localPath.getName)
        val destPath = copyFileToRemote(appStagingDir, localPath, symlinkCache)
        val destFs = FileSystem.get(destPath.toUri, hadoopConf)
        KyuubiDistributedCacheManager.addResource(
          destFs, hadoopConf, destPath, localResources, resType, linkname, statCache)
        (false, linkname)
      } else {
        (true, trimmedPath)
      }
    }

    if (loginFromKeytab) {
      distribute(keytabOrigin, destName = Option(keytabForAM))
    }

    // Add KYUUBI jar to the cache
    distribute(kyuubiJar)

    // Add Spark to the cache
    val jarsDir = new File(KyuubiSparkUtil.SPARK_JARS_DIR)
    val jarsArchive = File.createTempFile(SPARK_LIB_DIR, ".zip",
      new File(KyuubiSparkUtil.getLocalDir(conf)))
    val jarsStream = new ZipOutputStream(new FileOutputStream(jarsArchive))

    try {
      jarsStream.setLevel(0)
      jarsDir.listFiles().foreach { f =>
        if (f.isFile && f.getName.toLowerCase(Locale.ROOT).endsWith(".jar") && f.canRead) {
          jarsStream.putNextEntry(new ZipEntry(f.getName))
          Files.copy(f, jarsStream)
          jarsStream.closeEntry()
        }
      }
    } finally {
      jarsStream.close()
    }

    distribute(jarsArchive.toURI.getPath, resType = LocalResourceType.ARCHIVE,
      destName = Some(SPARK_LIB_DIR))
    jarsArchive.delete()

    KyuubiDistributedCacheManager.updateConfiguration(conf)

    val remoteConfArchivePath = new Path(appStagingDir, SPARK_CONF_ARCHIVE)
    val remoteFs = FileSystem.get(remoteConfArchivePath.toUri, hadoopConf)
    conf.set(KyuubiSparkUtil.CACHED_CONF_ARCHIVE, remoteConfArchivePath.toString)
    val localConfArchive = new Path(createConfArchive().toURI)
    copyFileToRemote(appStagingDir, localConfArchive, symlinkCache,
      destName = Some(SPARK_CONF_ARCHIVE))
    KyuubiDistributedCacheManager.addResource(remoteFs, hadoopConf, remoteConfArchivePath,
      localResources, LocalResourceType.ARCHIVE, SPARK_CONF_DIR, statCache)
    localResources
  }

  private[this] def createConfArchive(): File = {
    val hadoopConfFiles = new HashMap[String, File]()

    // SPARK_CONF_DIR shows up in the classpath before HADOOP_CONF_DIR/YARN_CONF_DIR
    sys.env.get("SPARK_CONF_DIR").foreach { localConfDir =>
      val dir = new File(localConfDir)
      if (dir.isDirectory) {
        val files = dir.listFiles(new FileFilter {
          override def accept(pathname: File): Boolean = {
            pathname.isFile && pathname.getName.endsWith(".xml")
          }
        })
        files.foreach { f => hadoopConfFiles(f.getName) = f }
      }
    }

    // SPARK-23630: during testing, Spark scripts filter out hadoop conf dirs so that user's
    // environments do not interfere with tests. This allows a special env variable during
    // tests so that custom conf dirs can be used by unit tests.
    val confDirs = Seq("HADOOP_CONF_DIR", "YARN_CONF_DIR")

    confDirs.foreach { envKey =>
      sys.env.get(envKey).foreach { path =>
        val dir = new File(path)
        if (dir.isDirectory) {
          val files = dir.listFiles()
          if (files == null) {
            warn("Failed to list files under directory " + dir)
          } else {
            files.foreach { file =>
              if (file.isFile && !hadoopConfFiles.contains(file getName())) {
                hadoopConfFiles(file.getName) = file
              }
            }
          }
        }
      }
    }

    val confArchive = File.createTempFile(SPARK_CONF_DIR, ".zip",
      new File(KyuubiSparkUtil.getLocalDir(conf)))
    val confStream = new ZipOutputStream(new FileOutputStream(confArchive))

    try {
      confStream.setLevel(0)

      // Upload $SPARK_CONF_DIR/log4j.properties file to the distributed cache to make sure that
      // the executors will use the latest configurations instead of the default values. This is
      // required when user changes log4j.properties directly to set the log configurations. If
      // configuration file is provided through --files then executors will be taking configurations
      // from --files instead of $SPARK_CONF_DIR/log4j.properties.

      // Also upload metrics.properties to distributed cache if exists in classpath.
      // If user specify this file using --files then executors will use the one
      // from --files instead.
      for { prop <- Seq("log4j.properties", "metrics.properties")
            url <- Option(KyuubiSparkUtil.getContextOrSparkClassLoader.getResource(prop))
            if url.getProtocol == "file" } {
        val file = new File(url.getPath)
        confStream.putNextEntry(new ZipEntry(file.getName))
        Files.copy(file, confStream)
        confStream.closeEntry()
      }

      // Save the Hadoop config files under a separate directory in the archive. This directory
      // is appended to the classpath so that the cluster-provided configuration takes precedence.
      confStream.putNextEntry(new ZipEntry(s"$HADOOP_CONF_DIR/"))
      confStream.closeEntry()
      hadoopConfFiles.foreach { case (name, file) =>
        if (file.canRead) {
          confStream.putNextEntry(new ZipEntry(s"$HADOOP_CONF_DIR/$name"))
          Files.copy(file, confStream)
          confStream.closeEntry()
        }
      }

      // Save the YARN configuration into a separate file that will be overlayed on top of the
      // cluster's Hadoop conf.
      confStream.putNextEntry(new ZipEntry(SPARK_HADOOP_CONF_FILE))
      hadoopConf.writeXml(confStream)
      confStream.closeEntry()

      // Save Spark configuration to a file in the archive, but filter out the app's secret.
      val props = new Properties()
      conf.getAll.foreach { case (k, v) =>
        props.setProperty(k, v)
      }
      // Override spark.yarn.key to point to the location in distributed cache which will be used
      // by AM.
      Option(keytabForAM).foreach { k => props.setProperty(KyuubiSparkUtil.KEYTAB, k) }
      confStream.putNextEntry(new ZipEntry(SPARK_CONF_FILE))
      val writer = new OutputStreamWriter(confStream, StandardCharsets.UTF_8)
      props.store(writer, "Spark configuration.")
      writer.flush()
      confStream.closeEntry()
    } finally {
      confStream.close()
    }
    confArchive
  }

  /**
   * Given a local URI, resolve it and return a qualified local path that corresponds to the URI.
   * This is used for preparing local resources to be included in the container launch context.
   */
  private def getQualifiedLocalPath(localURI: URI, hadoopConf: Configuration): Path = {
    val qualifiedURI =
      if (localURI.getScheme == null) {
        // If not specified, assume this is in the local filesystem to keep the behavior
        // consistent with that of Hadoop
        new URI(FileSystem.getLocal(hadoopConf).makeQualified(new Path(localURI)).toString)
      } else {
        localURI
      }
    new Path(qualifiedURI)
  }

  /**
   * Copy the given file to a remote file system (e.g. HDFS) if needed.
   * The file is only copied if the source and destination file systems are different or the source
   * scheme is "file". This is used for preparing resources for launching the ApplicationMaster
   * container. Exposed for testing.
   */
  def copyFileToRemote(destDir: Path, srcPath: Path,
      cache: Map[URI, Path], destName: Option[String] = None): Path = {
    val destFs = destDir.getFileSystem(hadoopConf)
    val srcFs = srcPath.getFileSystem(hadoopConf)
    val destPath = new Path(destDir, destName.getOrElse(srcPath.getName))
    info(s"Uploading resource $srcPath -> $destPath")
    FileUtil.copy(srcFs, srcPath, destFs, destPath, false, hadoopConf)
    destFs.setPermission(destPath, new FsPermission(APP_FILE_PERMISSION))
    // Resolve any symlinks in the URI path so using a "current" symlink to point to a specific
    // version shows the specific version in the distributed cache configuration
    val qualifiedDestPath = destFs.makeQualified(destPath)
    val qualifiedDestDir = qualifiedDestPath.getParent
    val resolvedDestDir = cache.getOrElseUpdate(qualifiedDestDir.toUri, {
      val fc = FileContext.getFileContext(qualifiedDestDir.toUri, hadoopConf)
      fc.resolvePath(qualifiedDestDir)
    })
    new Path(resolvedDestDir, qualifiedDestPath.getName)
  }

  /**
   * Joins all the path components using [[Path.SEPARATOR]].
   */
  def buildPath(components: String*): String = {
    components.mkString(Path.SEPARATOR)
  }

  /**
   * Add a path variable to the given environment map.
   * If the map already contains this key, append the value to the existing value instead.
   */
  def addPathToEnvironment(env: ENV, key: String, value: String): Unit = {
    val newValue =
      if (env.contains(key)) {
        env(key) + ApplicationConstants.CLASS_PATH_SEPARATOR + value
      } else {
        value
      }
    env.put(key, newValue)
  }

  /**
   * Add the given path to the classpath entry of the given environment map.
   * If the classpath is already set, this appends the new path to the existing classpath.
   */
  def addClasspathEntry(path: String, env: ENV): Unit =
    addPathToEnvironment(env, Environment.CLASSPATH.name, path)

  def populateClasspath(
      conf: Configuration, sparkConf: SparkConf, env: ENV): Unit = {
    addClasspathEntry(Environment.PWD.$$(), env)
    addClasspathEntry(Environment.PWD.$$() + Path.SEPARATOR + SPARK_CONF_DIR, env)
    addClasspathEntry(buildPath(Environment.PWD.$$(), KYUUBI_JAR_NAME), env)
    addClasspathEntry(buildPath(Environment.PWD.$$(), SPARK_LIB_DIR, "*"), env)
    populateHadoopClasspath(conf, env)
  }

  /**
   * Populate the classpath entry in the given environment map with any application
   * classpath specified through the Hadoop and Yarn configurations.
   */
  private[yarn] def populateHadoopClasspath(conf: Configuration, env: ENV): Unit = {
    val classPathElementsToAdd = getYarnAppClasspath(conf) ++ getMRAppClasspath(conf)
    classPathElementsToAdd.foreach { c =>
      addPathToEnvironment(env, Environment.CLASSPATH.name, c.trim)
    }
  }

  private def getYarnAppClasspath(conf: Configuration): Seq[String] =
    Option(conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH)) match {
      case Some(s) => s.toSeq
      case None => getDefaultYarnApplicationClasspath
    }

  private def getMRAppClasspath(conf: Configuration): Seq[String] =
    Option(conf.getStrings("mapreduce.application.classpath")) match {
      case Some(s) => s.toSeq
      case None => getDefaultMRApplicationClasspath
    }

  private[yarn] def getDefaultYarnApplicationClasspath: Seq[String] =
    YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH.toSeq

  private[yarn] def getDefaultMRApplicationClasspath: Seq[String] =
    StringUtils.getStrings(MRJobConfig.DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH).toSeq
}

object KyuubiYarnClient {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    new KyuubiYarnClient(conf).submit()
  }
}