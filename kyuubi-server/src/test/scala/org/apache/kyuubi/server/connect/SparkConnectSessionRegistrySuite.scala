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

package org.apache.kyuubi.server.connect

import java.util.UUID

import scala.collection.mutable.ListBuffer

import org.apache.kyuubi.KyuubiFunSuite

class SparkConnectSessionRegistrySuite extends KyuubiFunSuite {

  private def newRegistry(): SparkConnectSessionRegistry =
    new SparkConnectSessionRegistry(metadataManager = None)

  test("a registered token resolves to its session and engine") {
    val registry = newRegistry()
    val sessionId = UUID.randomUUID().toString
    val token = SparkConnect.generateToken()

    val registered = registry.register(token, sessionId, "connect_user", sessionId)
    assert(registered.tokenId == SparkConnect.tokenId(token))
    assert(registered.engineTag == sessionId)

    val resolved = registry.lookup(token)
    assert(resolved.map(_.sessionId).contains(sessionId))
    assert(resolved.map(_.userName).contains("connect_user"))
    assert(resolved.map(_.engineTag).contains(sessionId))
  }

  test("one session's token never resolves to another session") {
    val registry = newRegistry()
    val firstSession = UUID.randomUUID().toString
    val secondSession = UUID.randomUUID().toString
    val firstToken = SparkConnect.generateToken()
    val secondToken = SparkConnect.generateToken()
    registry.register(firstToken, firstSession, "user_a", firstSession)
    registry.register(secondToken, secondSession, "user_b", secondSession)

    assert(registry.lookup(firstToken).map(_.sessionId).contains(firstSession))
    assert(registry.lookup(secondToken).map(_.sessionId).contains(secondSession))
  }

  test("an unknown token resolves to nothing and is not cached") {
    val registry = newRegistry()
    registry.register(SparkConnect.generateToken(), UUID.randomUUID().toString, "user", "tag")
    assert(registry.cachedSessionCount == 1)

    // Anyone who can reach the port can present garbage; caching those would make the cache grow
    // without bound on demand.
    (1 to 500).foreach(_ => assert(registry.lookup(SparkConnect.generateToken()).isEmpty))
    assert(registry.cachedSessionCount == 1)
  }

  test("unregistering a session stops its token routing and fires close listeners") {
    val registry = newRegistry()
    val closed = ListBuffer[String]()
    registry.onSessionClosed(closed += _)

    val sessionId = UUID.randomUUID().toString
    val token = SparkConnect.generateToken()
    registry.register(token, sessionId, "connect_user", sessionId)

    registry.unregister(sessionId)

    assert(closed.toSeq == Seq(sessionId))
    assert(registry.lookup(token).isEmpty)
    assert(registry.cachedSessionCount == 0)
  }

  test("unregistering an unrelated session is a silent no-op") {
    val registry = newRegistry()
    val closed = ListBuffer[String]()
    registry.onSessionClosed(closed += _)

    // Every closing session passes through here, Spark Connect or not.
    registry.unregister(UUID.randomUUID().toString)

    assert(closed.isEmpty)
  }

  test("a failing close listener does not stop the others or the unregister") {
    val registry = newRegistry()
    val closed = ListBuffer[String]()
    registry.onSessionClosed(_ => throw new IllegalStateException("listener blew up"))
    registry.onSessionClosed(closed += _)

    val sessionId = UUID.randomUUID().toString
    val token = SparkConnect.generateToken()
    registry.register(token, sessionId, "connect_user", sessionId)
    registry.unregister(sessionId)

    assert(closed.toSeq == Seq(sessionId))
    assert(registry.lookup(token).isEmpty)
  }
}
