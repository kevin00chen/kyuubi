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

import java.net.URI

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.{KyuubiSparkUtil, SparkConf, SparkFunSuite}
import org.scalatest.Matchers

import yaooqinn.kyuubi.KYUUBI_JAR_NAME

class KyuubiYarnClientSuite extends SparkFunSuite with Matchers {

  private val yarnDefCP = YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH.toSeq
  private val mrDefCP = MRJobConfig.DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH.split(",").toSeq

  test("get default yarn application classpath") {
    KyuubiYarnClient.getDefaultYarnApplicationClasspath should be(yarnDefCP)
  }

  test("get default mr application classpath") {
    KyuubiYarnClient.getDefaultMRApplicationClasspath should be(mrDefCP)
  }

  test("populate hadoop classpath with yarn cp defined") {
    val conf = Map(YarnConfiguration.YARN_APPLICATION_CLASSPATH -> "/path/to/yarn")
    withConf(conf) { c =>
      val env = mutable.HashMap[String, String]()
      KyuubiYarnClient.populateHadoopClasspath(c, env)
      classpath(env) should be(conf.values.toList ++ mrDefCP)

    }
  }

  test("populate hadoop classpath with mr cp defined") {
    val conf = Map("mapreduce.application.classpath" -> "/path/to/mr")
    withConf(conf) { c =>
      val env = mutable.HashMap[String, String]()
      KyuubiYarnClient.populateHadoopClasspath(c, env)
      classpath(env) should be(yarnDefCP ++ conf.values.toList)
    }
  }

  test("populate hadoop classpath with yarn mr cp defined") {
    val conf = Map(
      YarnConfiguration.YARN_APPLICATION_CLASSPATH -> "/path/to/yarn",
      "mapreduce.application.classpath" -> "/path/to/mr")
    withConf(conf) { c =>
      val env = mutable.HashMap[String, String]()
      KyuubiYarnClient.populateHadoopClasspath(c, env)
      classpath(env) should be(conf.values.toList)
    }
  }

  test("populate classpath") {
    val conf = new Configuration()
    val env = mutable.HashMap[String, String]()
    KyuubiYarnClient.populateClasspath(conf, env)
    val cp = classpath(env)
    cp should contain("{{PWD}}")
    cp should contain(Environment.PWD.$$() + Path.SEPARATOR + SPARK_CONF_DIR)
    cp should contain(KyuubiYarnClient.buildPath(Environment.PWD.$$(), KYUUBI_JAR_NAME))
    cp should contain(KyuubiYarnClient.buildPath(Environment.PWD.$$(), SPARK_LIB_DIR, "*"))
    cp should contain(yarnDefCP.head)
    cp should contain(mrDefCP.head)
  }

  test("build path") {
    KyuubiYarnClient.buildPath("1", "2", "3") should be(
      "1" + Path.SEPARATOR + "2" + Path.SEPARATOR + "3")
  }

  test("add path to env") {
    val env = mutable.HashMap[String, String]()
    KyuubiYarnClient.addPathToEnvironment(env, Environment.CLASSPATH.name, "1")
    classpath(env) should be(List("1"))
    KyuubiYarnClient.addPathToEnvironment(env, Environment.CLASSPATH.name, "2")
    classpath(env) should be(List("1", "2"))
    KyuubiYarnClient.addPathToEnvironment(env, "JAVA_TEST_HOME", "/path/to/java")
    env("JAVA_TEST_HOME") should be("/path/to/java")
  }

  test("get qualified local path") {
    val conf = new Configuration()
    val uri1 = KyuubiSparkUtil.resolveURI("1")
    val path1 = KyuubiYarnClient.getQualifiedLocalPath(uri1, conf)
    path1.toUri should be(uri1)
    val uri2 = new URI("1")
    val path2 = KyuubiYarnClient.getQualifiedLocalPath(uri2, conf)
    path2.toUri should not be uri2
    path2.toUri should be(uri1)

    val uri3 = KyuubiSparkUtil.resolveURI("hdfs://1")
    val path3 = KyuubiYarnClient.getQualifiedLocalPath(uri3, conf)
    path3.toUri should be(uri3)

  }
  
  test("kyuubi yarn client init") {
    val conf = new SparkConf()
    new KyuubiYarnClient(conf)
  }

  def withConf(map: Map[String, String] = Map.empty)(testCode: Configuration => Any): Unit = {
    val conf = new Configuration()
    map.foreach { case (k, v) => conf.set(k, v) }
    testCode(conf)
  }

  def classpath(env: mutable.HashMap[String, String]): Array[String] =
    env(Environment.CLASSPATH.name).split(":|;|<CPS>")
}
