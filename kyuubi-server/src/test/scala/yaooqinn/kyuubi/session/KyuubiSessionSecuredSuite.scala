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

package yaooqinn.kyuubi.session

import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.KyuubiSparkUtil

import yaooqinn.kyuubi.SecurityEnabled
import yaooqinn.kyuubi.server.KyuubiServer

class KyuubiSessionSecuredSuite extends SecurityEnabled {

  test("test session ugi") {
    val server = KyuubiServer.startKyuubiServer()
    val be = server.beService
    val sessionMgr = be.getSessionManager
    val operationMgr = sessionMgr.getOperationMgr
    val user = KyuubiSparkUtil.getCurrentUserName
    val passwd = ""
    val ip = ""
    val imper = true
    val proto = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8

    val session =
      new KyuubiSession(proto, user, passwd, server.getConf, ip, imper, sessionMgr, operationMgr)
    assert(session.ugi.getAuthenticationMethod === AuthenticationMethod.PROXY)
    session.close()
    server.stop()
  }




}
