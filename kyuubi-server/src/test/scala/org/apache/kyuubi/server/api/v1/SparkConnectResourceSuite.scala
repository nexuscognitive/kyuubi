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

package org.apache.kyuubi.server.api.v1

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.client.api.v1.dto.SparkConnectSession
import org.apache.kyuubi.config.KyuubiConf._

class SparkConnectResourceSuite extends KyuubiFunSuite {

  test("a Spark Connect session gets its own engine") {
    val conf = SparkConnectResource.serverControlledConf("a-token")
    assert(conf(SESSION_SPARK_CONNECT_ENABLED.key) == "true")
    assert(conf(SESSION_SPARK_CONNECT_TOKEN.key) == "a-token")
    assert(conf(ENGINE_TYPE.key) == "SPARK_SQL")
    // Spark Connect sessions carry state -- artifacts, temp views, cached frames -- and the token
    // is minted per engine, so the engine must not be shared with another session.
    assert(conf(ENGINE_SHARE_LEVEL.key) == "CONNECTION")
  }

  test("a client cannot declare its own token or engine sharing") {
    val requested = Map(
      SESSION_SPARK_CONNECT_TOKEN.key -> "a-token-i-chose",
      SESSION_SPARK_CONNECT_ENABLED.key -> "true",
      ENGINE_SHARE_LEVEL.key -> "USER",
      ENGINE_TYPE.key -> "FLINK_SQL",
      "spark.sql.shuffle.partitions" -> "42")

    val accepted = SparkConnectResource.clientControlledConf(requested)

    assert(!accepted.contains(SESSION_SPARK_CONNECT_TOKEN.key))
    assert(!accepted.contains(SESSION_SPARK_CONNECT_ENABLED.key))
    assert(!accepted.contains(ENGINE_SHARE_LEVEL.key))
    assert(!accepted.contains(ENGINE_TYPE.key))
    // Ordinary Spark conf is still the caller's to set.
    assert(accepted("spark.sql.shuffle.partitions") == "42")
  }

  test("server controlled conf wins over anything the client sent") {
    val requested = Map(ENGINE_SHARE_LEVEL.key -> "SERVER")
    val effective =
      SparkConnectResource.serverControlledConf("a-token") ++
        SparkConnectResource.clientControlledConf(requested)
    assert(effective(ENGINE_SHARE_LEVEL.key) == "CONNECTION")
  }

  test("the session DTO keeps the token out of its string form") {
    val session = new SparkConnectSession("a-session-id", "a-secret-token", "sc://host:15002")
    // The DTO is logged in places a token must never reach.
    assert(!session.toString.contains("a-secret-token"))
    assert(session.toString.contains("a-session-id"))
    assert(session.getToken == "a-secret-token")
  }
}
