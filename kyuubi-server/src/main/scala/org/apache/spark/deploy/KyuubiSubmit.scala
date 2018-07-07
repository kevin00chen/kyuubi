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

package org.apache.spark.deploy

import java.io.{File, PrintStream}

import scala.collection.mutable.{ArrayBuffer, HashMap, Map}
import scala.util.Properties

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark._
import org.apache.spark.util.{MutableURLClassLoader, Utils}

import yaooqinn.kyuubi.server.KyuubiServer
import yaooqinn.kyuubi.yarn.KyuubiYarnClient

/**
 * Kyuubi version of SparkSubmit
 */
object KyuubiSubmit {

  // scalastyle:off println
  private[spark] var exitFn: Int => Unit = (exitCode: Int) => System.exit(exitCode)
  private[spark] var printStream: PrintStream = System.err
  private[spark] def printWarning(str: String): Unit = printStream.println("Warning: " + str)
  private[spark] def printErrorAndExit(str: String): Unit = {
    printStream.println("Error: " + str)
    printStream.println("Run with --help for usage help or --verbose for debug output")
    exitFn(1)
  }
  private[spark] def printVersionAndExit(): Unit = {
    printStream.println("""Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version %s
      /_/
                        """.format(SPARK_VERSION))
    printStream.println("Using Scala %s, %s, %s".format(
      Properties.versionString, Properties.javaVmName, Properties.javaVersion))
    printStream.println("Branch %s".format(SPARK_BRANCH))
    printStream.println("Compiled by user %s on %s".format(SPARK_BUILD_USER, SPARK_BUILD_DATE))
    printStream.println("Revision %s".format(SPARK_REVISION))
    printStream.println("Url %s".format(SPARK_REPO_URL))
    printStream.println("Type --help for more information.")
    exitFn(0)
  }
  // scalastyle:on println

  def main(args: Array[String]): Unit = {
    val appArgs = new SparkSubmitArguments(args)
    if (appArgs.verbose) {
      // scalastyle:off println
      printStream.println(appArgs)
      // scalastyle:on println
    }
    val (childClasspath, sysProps) = prepareSubmitEnvironment(appArgs)
    if (appArgs.verbose) {
      // scalastyle:off println
      // sysProps may contain sensitive information, so redact before printing
      printStream.println(s"System properties:\n${Utils.redact(sysProps).mkString("\n")}")
      printStream.println(s"Classpath elements:\n${childClasspath.mkString("\n")}")
      printStream.println("\n")
    }
    // scalastyle:on println

    val loader = KyuubiSparkUtil.getAndSetKyuubiFirstClassLoader

    for (jar <- childClasspath) {
      addJarToClasspath(jar, loader)
    }

    for ((key, value) <- sysProps) {
      System.setProperty(key, value)
    }

    try {
      if (appArgs.deployMode == "cluster") {
        KyuubiYarnClient.main(null)
      } else {
        KyuubiServer.main(null)
      }
    } catch {
      case t: Throwable => throw t
    }
  }

  /**
   * Prepare the environment for submitting an application.
   * This returns a 4-tuple:
   *   (1) the arguments for the child process,
   *   (2) a list of classpath entries for the child,
   *   (3) a map of system properties, and
   *   (4) the main class for the child
   * Exposed for testing.
   */
  private[deploy] def prepareSubmitEnvironment(args: SparkSubmitArguments)
  : (Seq[String], Map[String, String]) = {
    // Return values
    val childClasspath = new ArrayBuffer[String]()
    val sysProps = new HashMap[String, String]()

    args.master match {
      case "yarn" =>
      case "yarn-client" =>
        printWarning(s"Master ${args.master} is deprecated since 2.0." +
          " Please use master \"yarn\" with specified deploy mode instead.")
        args.master = "yarn"
      case _ => printErrorAndExit("Kyuubi only supports yarn as master.")
    }

    args.deployMode match {
      case "client" =>
      case "cluster" =>
      case _ => printWarning("Kyuubi only supports client mode.")
        args.deployMode = "client"

    }

    // Make sure YARN is included in our build if we're trying to use it
    if (!Utils.classIsLoadable("org.apache.spark.deploy.yarn.Client")) {
      printErrorAndExit(
        "Could not load YARN classes. Spark may not have been compiled with YARN support.")
    }

    // Special flag to avoid deprecation warnings at the client
    sysProps("SPARK_SUBMIT") = "true"

    Seq(
      "spark.master" ->  args.master,
      "spark.submit.deployMode" -> args.deployMode,
      "spark.app.name" -> args.name,
      "spark.driver.memory" -> args.driverMemory,
      "spark.driver.extraClassPath" -> args.driverExtraClassPath,
      "spark.driver.extraJavaOptions" -> args.driverExtraJavaOptions,
      "spark.driver.extraLibraryPath" -> args.driverExtraLibraryPath,
      "spark.yarn.queue" -> args.queue,
      "spark.executor.instances" -> args.numExecutors,
      "spark.yarn.dist.jars" -> args.jars,
      "spark.yarn.dist.files" -> args.files,
      "spark.yarn.dist.archives" -> args.archives,
      "spark.yarn.principal" -> args.principal,
      "spark.yarn.keytab" -> args.keytab,
      "spark.executor.cores" -> args.executorCores,
      "spark.executor.memory" -> args.executorMemory
    ).filter(_._2 != null).foreach(o => sysProps.put(o._1, o._2))

    childClasspath += args.primaryResource
    if (args.jars != null) { childClasspath ++= args.jars.split(",") }
    val jars = sysProps.get("spark.jars").map(x => x.split(",").toSeq).getOrElse(Seq.empty) ++
      Seq(args.primaryResource)
    sysProps.put("spark.jars", jars.mkString(","))

    if (args.principal != null) {
      require(args.keytab != null, "Keytab must be specified when principal is specified")
      if (!new File(args.keytab).exists()) {
        throw new SparkException(s"Keytab file: ${args.keytab} does not exist")
      } else {
        // Add keytab and principal configurations in sysProps to make them available
        // for later use; e.g. in spark sql, the isolated class loader used to talk
        // to HiveMetastore will use these settings. They will be set as Java system
        // properties and then loaded by SparkConf
        sysProps.put("spark.yarn.keytab", args.keytab)
        sysProps.put("spark.yarn.principal", args.principal)

        UserGroupInformation.loginUserFromKeytab(args.principal, args.keytab)
      }
    }

    // Load any properties specified through --conf and the default properties file
    for ((k, v) <- args.sparkProperties) {
      sysProps.getOrElseUpdate(k, v)
    }

    // Resolve paths in certain spark properties
    val pathConfigs = Seq(
      "spark.jars",
      "spark.files",
      "spark.yarn.dist.files",
      "spark.yarn.dist.archives",
      "spark.yarn.dist.jars")
    pathConfigs.foreach { config =>
      // Replace old URIs with resolved URIs, if they exist
      sysProps.get(config).foreach { oldValue =>
        sysProps(config) = Utils.resolveURIs(oldValue)
      }
    }

    (childClasspath, sysProps)
  }

  private def runMain(
      childClasspath: Seq[String],
      sysProps: Map[String, String],
      verbose: Boolean): Unit = {
    // scalastyle:off println

  }

  private def addJarToClasspath(localJar: String, loader: MutableURLClassLoader) {
    val uri = Utils.resolveURI(localJar)
    uri.getScheme match {
      case "file" | "local" =>
        val file = new File(uri.getPath)
        if (file.exists()) {
          loader.addURL(file.toURI.toURL)
        } else {
          printWarning(s"Local jar $file does not exist, skipping.")
        }
      case _ =>
        printWarning(s"Skip remote jar $uri.")
    }
  }
}
