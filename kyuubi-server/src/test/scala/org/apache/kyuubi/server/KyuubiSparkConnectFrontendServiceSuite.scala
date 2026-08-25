/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.server

import org.apache.kyuubi.{KyuubiException, KyuubiFunSuite}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._

class KyuubiSparkConnectFrontendServiceSuite extends KyuubiFunSuite {

  private def baseConf: KyuubiConf = KyuubiConf(loadSysDefault = false)
    .set(FRONTEND_PROTOCOLS, Seq(FrontendProtocols.REST.toString))
    .set(FRONTEND_SPARK_CONNECT_ENABLED, true)
    .set(FRONTEND_SPARK_CONNECT_SSL_ENABLED, true)
    .set(FRONTEND_SPARK_CONNECT_SSL_KEYSTORE_PATH, "/tmp/spark-connect-keystore.jks")

  test("a fully configured Spark Connect frontend passes validation") {
    KyuubiSparkConnectFrontendService.validateConf(baseConf)
  }

  test("the frontend refuses to start without TLS") {
    // A plaintext listener is not a weaker but working setup, it is a broken one: Spark Connect
    // clients switch to a secure channel on their own once a token is set for a non-loopback
    // host, so the port would only ever answer handshakes with gRPC frames.
    val conf = baseConf.set(FRONTEND_SPARK_CONNECT_SSL_ENABLED, false)
    val e = intercept[KyuubiException](KyuubiSparkConnectFrontendService.validateConf(conf))
    assert(e.getMessage.contains(FRONTEND_SPARK_CONNECT_SSL_ENABLED.key))
  }

  test("the frontend refuses to start without a keystore") {
    val conf = baseConf.unset(FRONTEND_SPARK_CONNECT_SSL_KEYSTORE_PATH)
    val e = intercept[KyuubiException](KyuubiSparkConnectFrontendService.validateConf(conf))
    assert(e.getMessage.contains(FRONTEND_SPARK_CONNECT_SSL_KEYSTORE_PATH.key))
  }

  test("the frontend refuses to start without the REST frontend") {
    // Spark Connect has no open-session RPC, so without REST there is no way to create a session
    // and every token presented on the gRPC port would be unroutable.
    val conf = baseConf.set(FRONTEND_PROTOCOLS, Seq(FrontendProtocols.THRIFT_BINARY.toString))
    val e = intercept[KyuubiException](KyuubiSparkConnectFrontendService.validateConf(conf))
    assert(e.getMessage.contains("REST"))
  }

  test("building an SSL context fails clearly when the keystore is absent") {
    val conf = baseConf.set(FRONTEND_SPARK_CONNECT_SSL_KEYSTORE_PATH, "/nonexistent/keystore.jks")
    val e = intercept[KyuubiException](KyuubiSparkConnectFrontendService.buildSslContext(conf))
    assert(e.getMessage.contains("/nonexistent/keystore.jks"))
  }

  test("SPARK_CONNECT is a recognised frontend protocol") {
    val conf = KyuubiConf(loadSysDefault = false)
      .set(FRONTEND_PROTOCOLS, Seq("REST", "SPARK_CONNECT"))
    assert(conf.get(FRONTEND_PROTOCOLS).contains(FrontendProtocols.SPARK_CONNECT.toString))
  }

  test("Spark Connect config entries are server-side and default to off") {
    val conf = KyuubiConf(loadSysDefault = false)
    assert(!conf.get(FRONTEND_SPARK_CONNECT_ENABLED))
    assert(conf.get(FRONTEND_SPARK_CONNECT_BIND_PORT) == 15002)
    assert(conf.get(FRONTEND_SPARK_CONNECT_ENGINE_PORT) == 15002)
    assert(conf.get(FRONTEND_SPARK_CONNECT_MAX_MESSAGE_SIZE) == 128 * 1024 * 1024)
    assert(conf.get(FRONTEND_SPARK_CONNECT_ENGINE_MAX_MESSAGE_SIZE) == 128 * 1024 * 1024)
    assert(!conf.get(FRONTEND_SPARK_CONNECT_SSL_ENABLED))

    // The token must never reach the engine as a Spark conf; the SERVER audience is what keeps it
    // out of the driver command line and the Spark UI environment page.
    conf.set(SESSION_SPARK_CONNECT_TOKEN, "a-secret-token")
    conf.set(SESSION_SPARK_CONNECT_ENABLED, true)
    val engineConf = conf.getEngineConf(org.apache.kyuubi.engine.EngineType.SPARK_SQL)
    assert(!engineConf.contains(SESSION_SPARK_CONNECT_TOKEN.key))
    assert(!engineConf.contains(SESSION_SPARK_CONNECT_ENABLED.key))
  }
}
