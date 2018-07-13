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

import java.io.IOException
import java.util.concurrent.{Executors, TimeUnit}

import scala.util.control.NonFatal

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records.{ApplicationAttemptId, FinalApplicationStatus}
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.conf.YarnConfiguration._
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.spark.{KyuubiSparkUtil, SparkConf}

import yaooqinn.kyuubi.Logging
import yaooqinn.kyuubi.server.KyuubiServer
import yaooqinn.kyuubi.service.AbstractService

class KyuubiAppMaster private(args: AppMasterArguments, name: String) extends AbstractService(name)
  with Logging {

  def this(args: AppMasterArguments) = this(args, classOf[KyuubiAppMaster].getSimpleName)
  private lazy val amClient = AMRMClient.createAMRMClient()

  private[this] var yarnConf: YarnConfiguration = _
  private[this] var server: KyuubiServer = _
  private[this] var interval: Int = _
  private[this] var amMaxAttempts: Int = _

  @volatile private[this] var amStatus = FinalApplicationStatus.SUCCEEDED
  @volatile private[this] var finalMsg = ""

  private[this] var failureCount = 0

  private[this] val heartbeatTask = new Runnable {
    override def run(): Unit = {
      try {
        amClient.allocate(0.1f)
        failureCount = 0
      } catch {
        case _: InterruptedException =>
        case e: ApplicationAttemptNotFoundException =>
          failureCount += 1
          error(s"Exception from heartbeat thread. Tried $failureCount time(s)", e)
          stop(FinalApplicationStatus.FAILED, e.getMessage)
        case e: Throwable =>
          failureCount += 1
          val msg = s"Heartbeat thread fails for $failureCount times in a row. "
          if (!NonFatal(e) || failureCount >= amMaxAttempts + 2) {
            stop(FinalApplicationStatus.FAILED, msg + e.getMessage)
          } else {
            warn(msg, e)
          }
      }
    }
  }

  private[this] val executor = Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("AM-RM-Heartbeat" + "-%d").build())

  private[this] def startKyuubiServer(): Unit = {
    try {
      server = KyuubiServer.startKyuubiServer()
      val feService = server.feService
      amClient.registerApplicationMaster(
        feService.getServerIPAddress.getHostName,
        feService.getPortNumber, "")
    } catch {
      case e: Exception =>
        error("Error starting Kyuubi Server", e)
        this.stop()
    }
  }

  private[this] def getAppAttemptId: ApplicationAttemptId = {
    val containerIdString = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name())
    ConverterUtils.toContainerId(containerIdString).getApplicationAttemptId
  }

  /**
   * Clean up the staging directory.
   */
  private[this] def cleanupStagingDir(): Unit = {
    var stagingDirPath: Path = null
    try {
      if (conf.getBoolean("spark.kyuubi.staging.cleanable", defaultValue = true)) {
        stagingDirPath = new Path(System.getenv("KYUUBI_YARN_STAGING_DIR"))
        info("Deleting staging directory " + stagingDirPath)
        val fs = stagingDirPath.getFileSystem(yarnConf)
        fs.delete(stagingDirPath, true)
      }
    } catch {
      case ioe: IOException =>
        error("Failed to cleanup staging dir " + stagingDirPath, ioe)
    }
  }

  private[this] def stop(stat: FinalApplicationStatus, msg: String): Unit = {
    amStatus = stat
    finalMsg = msg
    amClient.stop()
    executor.shutdown()
    if (server != null) {
      server.stop()
    }
    if (getAppAttemptId.getAttemptId >= amMaxAttempts) {
      amClient.unregisterApplicationMaster(amStatus, finalMsg, "")
      cleanupStagingDir()
    }
    System.exit(-1)
  }

  override def init(conf: SparkConf): Unit = {
    KyuubiSparkUtil.getAndSetKyuubiFirstClassLoader
    this.conf = conf
    args.propertiesFile.map(KyuubiSparkUtil.getPropertiesFromFile) match {
      case Some(props) => props.foreach { case (k, v) =>
        conf.set(k, v)
        sys.props(k) = v
      }
      case _ =>
    }
    yarnConf = new YarnConfiguration(KyuubiSparkUtil.newConfiguration(conf))
    amMaxAttempts = yarnConf.getInt(RM_AM_MAX_ATTEMPTS, DEFAULT_RM_AM_MAX_ATTEMPTS)
    amClient.init(yarnConf)
    super.init(conf)
    KyuubiSparkUtil.addShutdownHook(() => this.stop())
  }

  override def start(): Unit = {
    amClient.start()
    startKyuubiServer()
    val expiryInterval = yarnConf.getInt(RM_AM_EXPIRY_INTERVAL_MS, 120000)
    interval = math.max(0, math.min(expiryInterval / 2, 3000))
    executor.scheduleAtFixedRate(heartbeatTask, interval, interval, TimeUnit.MILLISECONDS)
    super.start()
  }

  override def stop(): Unit = {
    super.stop()
    System.exit(0)
  }
}

object KyuubiAppMaster extends Logging {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val appMasterArgs = AppMasterArguments(args)
    val master = new KyuubiAppMaster(appMasterArgs)
    master.init(conf)
    master.start()
  }
}