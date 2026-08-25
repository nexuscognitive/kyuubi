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

import java.util.Collections
import javax.ws.rs.client.Entity
import javax.ws.rs.core.{GenericType, MediaType}

import org.apache.kyuubi.{KyuubiFunSuite, RestFrontendTestHelper}
import org.apache.kyuubi.client.api.v1.dto.{SessionOpenRequest, SparkConnectSession}
import org.apache.kyuubi.client.api.v1.dto.SparkConnectSessionData
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.server.http.util.HttpAuthUtils.{basicAuthorizationHeader, AUTHORIZATION_HEADER}

class SparkConnectResourceSuite extends KyuubiFunSuite with RestFrontendTestHelper {

  test("a Spark Connect session gets an engine of its user's own") {
    val conf = SparkConnectResource.serverControlledConf("an-engine-credential")
    assert(conf(SESSION_SPARK_CONNECT_ENABLED.key) == "true")
    assert(conf(SESSION_SPARK_CONNECT_TOKEN.key) == "an-engine-credential")
    assert(conf(ENGINE_TYPE.key) == "SPARK_SQL")
    // The engine serves exactly one user, so state that outlives a session -- artifacts, temp
    // views, cached frames -- is that user's own rather than someone else's leaking across.
    assert(conf(ENGINE_SHARE_LEVEL.key) == "USER")
    // Without a subdomain of its own, a Spark Connect session would be handed the same user's
    // ordinary Thrift engine, which was launched without the Spark Connect plugin.
    assert(conf(ENGINE_SHARE_LEVEL_SUBDOMAIN.key) == SparkConnectResource.ENGINE_SUBDOMAIN)
  }

  test("a client cannot declare its own engine credential or engine sharing") {
    val requested = Map(
      SESSION_SPARK_CONNECT_TOKEN.key -> "a-token-i-chose",
      SESSION_SPARK_CONNECT_ENABLED.key -> "true",
      ENGINE_SHARE_LEVEL.key -> "SERVER",
      ENGINE_SHARE_LEVEL_SUBDOMAIN.key -> "somebody-elses-engine",
      ENGINE_TYPE.key -> "FLINK_SQL",
      "spark.sql.shuffle.partitions" -> "42")

    val accepted = SparkConnectResource.clientControlledConf(requested)

    assert(!accepted.contains(SESSION_SPARK_CONNECT_TOKEN.key))
    assert(!accepted.contains(SESSION_SPARK_CONNECT_ENABLED.key))
    assert(!accepted.contains(ENGINE_SHARE_LEVEL.key))
    assert(!accepted.contains(ENGINE_SHARE_LEVEL_SUBDOMAIN.key))
    assert(!accepted.contains(ENGINE_TYPE.key))
    // Ordinary Spark conf is still the caller's to set.
    assert(accepted("spark.sql.shuffle.partitions") == "42")
  }

  test("server controlled conf wins over anything the client sent") {
    val requested = Map(
      ENGINE_SHARE_LEVEL.key -> "SERVER",
      ENGINE_SHARE_LEVEL_SUBDOMAIN.key -> "somebody-elses-engine")
    val effective =
      SparkConnectResource.serverControlledConf("an-engine-credential") ++
        SparkConnectResource.clientControlledConf(requested)
    assert(effective(ENGINE_SHARE_LEVEL.key) == "USER")
    assert(effective(ENGINE_SHARE_LEVEL_SUBDOMAIN.key) == SparkConnectResource.ENGINE_SUBDOMAIN)
  }

  test("the session DTO has no token to leak") {
    val session = new SparkConnectSession("a-session-id", "sc://host:15002")
    assert(session.toString.contains("a-session-id"))
    // Nothing to hand out: the caller authenticates the gRPC port with the credential they
    // already have, and Kyuubi's own engine credential never leaves the gateway.
    assert(!classOf[SparkConnectSession].getMethods.exists(_.getName == "getToken"))
  }

  test("a session is PENDING until its engine reports in") {
    assert(SparkConnectResource.sessionState(openedTime = -1L, endTime = -1L, failed = false) ==
      SparkConnectResource.STATE_PENDING)
    assert(SparkConnectResource.sessionState(openedTime = 1L, endTime = -1L, failed = false) ==
      SparkConnectResource.STATE_RUNNING)
    assert(SparkConnectResource.sessionState(openedTime = 1L, endTime = 2L, failed = false) ==
      SparkConnectResource.STATE_CLOSED)
    // A failed session may well have opened and closed; the failure is what the user needs to see.
    assert(SparkConnectResource.sessionState(openedTime = 1L, endTime = 2L, failed = true) ==
      SparkConnectResource.STATE_FAILED)
  }

  test("only sessions this resource opened are recognised as Spark Connect ones") {
    assert(SparkConnectResource.isSparkConnectSession(
      Map(SESSION_SPARK_CONNECT_ENABLED.key -> "true")))
    assert(!SparkConnectResource.isSparkConnectSession(
      Map(SESSION_SPARK_CONNECT_ENABLED.key -> "false")))
    // An ordinary Thrift or REST session lives in the same session manager and must not show up.
    assert(!SparkConnectResource.isSparkConnectSession(Map("spark.sql.shuffle.partitions" -> "42")))
  }

  test("the session list only shows the caller their own sessions") {
    val alice = openSparkConnectSession("alice")
    val bob = openSparkConnectSession("bob")
    try {
      val aliceSessions = listSparkConnectSessions("alice")
      assert(aliceSessions.map(_.getSessionId) == Seq(alice.getSessionId))
      assert(aliceSessions.map(_.getUser) == Seq("alice"))

      val bobSessions = listSparkConnectSessions("bob")
      assert(bobSessions.map(_.getSessionId) == Seq(bob.getSessionId))

      // Somebody with no sessions of their own sees an empty list, not everyone else's.
      assert(listSparkConnectSessions("carol").isEmpty)
    } finally {
      closeSparkConnectSession("alice", alice.getSessionId)
      closeSparkConnectSession("bob", bob.getSessionId)
    }
  }

  test("creating a session twice gives the caller the one session they have") {
    val first = openSparkConnectSession("alice")
    try {
      val second = openSparkConnectSession("alice")

      // A second session would be unreachable: the gRPC port routes on who is calling, not on
      // which session id they meant.
      assert(second.getSessionId == first.getSessionId)
      assert(listSparkConnectSessions("alice").map(_.getSessionId) == Seq(first.getSessionId))
    } finally {
      closeSparkConnectSession("alice", first.getSessionId)
    }
  }

  test("two users each get their own session") {
    val alice = openSparkConnectSession("alice")
    val bob = openSparkConnectSession("bob")
    try {
      assert(alice.getSessionId != bob.getSessionId)
    } finally {
      closeSparkConnectSession("alice", alice.getSessionId)
      closeSparkConnectSession("bob", bob.getSessionId)
    }
  }

  test("neither the create response nor the session list carries a token") {
    val alice = openSparkConnectSession("alice")
    try {
      val listBody = webTarget.path("api/v1/spark-connect/sessions")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("alice"))
        .get()
        .readEntity(classOf[String])

      // Asserted against the raw body rather than the DTO, because a credential could only leak
      // through a field the DTO does not model -- which a typed read is exactly what would hide.
      assert(!listBody.toLowerCase.contains("token"))
      assert(listBody.contains(alice.getSessionId))

      val createBody = webTarget.path("api/v1/spark-connect/sessions")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("bob"))
        .post(Entity.entity(
          new SessionOpenRequest(Collections.emptyMap[String, String]()),
          MediaType.APPLICATION_JSON_TYPE))
        .readEntity(classOf[String])
      try {
        assert(!createBody.toLowerCase.contains("token"))
      } finally {
        listSparkConnectSessions("bob").foreach(session =>
          closeSparkConnectSession("bob", session.getSessionId))
      }
    } finally {
      closeSparkConnectSession("alice", alice.getSessionId)
    }
  }

  private def openSparkConnectSession(user: String): SparkConnectSession = {
    val request = new SessionOpenRequest(Collections.emptyMap[String, String]())
    val response = webTarget.path("api/v1/spark-connect/sessions")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader(user))
      .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE))
    assert(200 == response.getStatus)
    response.readEntity(classOf[SparkConnectSession])
  }

  private def listSparkConnectSessions(user: String): Seq[SparkConnectSessionData] = {
    val response = webTarget.path("api/v1/spark-connect/sessions")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader(user))
      .get()
    assert(200 == response.getStatus)
    response.readEntity(new GenericType[Seq[SparkConnectSessionData]]() {})
  }

  private def closeSparkConnectSession(user: String, sessionId: String): Unit = {
    webTarget.path(s"api/v1/spark-connect/sessions/$sessionId")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header(AUTHORIZATION_HEADER, basicAuthorizationHeader(user))
      .delete()
  }
}
