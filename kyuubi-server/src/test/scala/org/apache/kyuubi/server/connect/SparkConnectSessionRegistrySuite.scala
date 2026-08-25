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

  test("a user resolves to their own session and engine") {
    val registry = newRegistry()
    val sessionId = UUID.randomUUID().toString
    val engineToken = SparkConnect.generateToken()

    val registered = registry.register("connect_user", sessionId, sessionId, engineToken)
    assert(registered.userName == "connect_user")
    assert(registered.engineTag == sessionId)

    val resolved = registry.liveSession("connect_user")
    assert(resolved.map(_.sessionId).contains(sessionId))
    assert(resolved.map(_.engineTag).contains(sessionId))
    assert(resolved.map(_.engineToken).contains(engineToken))
  }

  test("one user's binding never resolves to another user's engine") {
    val registry = newRegistry()
    val firstSession = UUID.randomUUID().toString
    val secondSession = UUID.randomUUID().toString
    registry.register("user_a", firstSession, firstSession, SparkConnect.generateToken())
    registry.register("user_b", secondSession, secondSession, SparkConnect.generateToken())

    assert(registry.liveSession("user_a").map(_.sessionId).contains(firstSession))
    assert(registry.liveSession("user_b").map(_.sessionId).contains(secondSession))
    assert(registry.liveSession("user_a").map(_.engineTag) !=
      registry.liveSession("user_b").map(_.engineTag))
  }

  test("a user with no binding resolves to nothing and is not cached") {
    val registry = newRegistry()
    registry.register("connect_user", UUID.randomUUID().toString, "tag", "token")
    assert(registry.cachedBindingCount == 1)

    // Every miss would otherwise be an entry someone can create on demand.
    (1 to 500).foreach(index => assert(registry.lookup(s"stranger-$index").isEmpty))
    assert(registry.cachedBindingCount == 1)
  }

  test("registering again replaces the user's binding rather than adding one") {
    val registry = newRegistry()
    val firstSession = UUID.randomUUID().toString
    val secondSession = UUID.randomUUID().toString
    registry.register("connect_user", firstSession, firstSession, "first-token")
    registry.register("connect_user", secondSession, secondSession, "second-token")

    assert(registry.cachedBindingCount == 1)
    assert(registry.liveSession("connect_user").map(_.sessionId).contains(secondSession))
    assert(registry.liveSession("connect_user").map(_.engineToken).contains("second-token"))
  }

  test("closing a session stops routing but keeps the engine binding") {
    val registry = newRegistry()
    val closed = ListBuffer[String]()
    registry.onSessionClosed(closed += _)

    val sessionId = UUID.randomUUID().toString
    registry.register("connect_user", sessionId, sessionId, "engine-token")

    registry.unregister(sessionId)

    assert(closed.toSeq == Seq(sessionId))
    // Nothing routes: the gRPC port has to answer "create a session first".
    assert(registry.liveSession("connect_user").isEmpty)
    // The engine survives its session, and the next session has to inherit its tag and credential
    // -- a relaunch would tag a new pod, but a reused driver keeps the old tag and old token.
    val binding = registry.lookup("connect_user")
    assert(binding.map(_.engineTag).contains(sessionId))
    assert(binding.map(_.engineToken).contains("engine-token"))
    assert(binding.exists(!_.hasLiveSession))
  }

  test("forgetting a user drops the engine binding outright") {
    val registry = newRegistry()
    val sessionId = UUID.randomUUID().toString
    registry.register("connect_user", sessionId, sessionId, "engine-token")

    registry.forget("connect_user")

    assert(registry.lookup("connect_user").isEmpty)
    assert(registry.cachedBindingCount == 0)
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
    registry.register("connect_user", sessionId, sessionId, "engine-token")
    registry.unregister(sessionId)

    assert(closed.toSeq == Seq(sessionId))
    assert(registry.liveSession("connect_user").isEmpty)
  }

  test("a binding never prints its engine credential") {
    val registry = newRegistry()
    val sessionId = UUID.randomUUID().toString
    val binding = registry.register("connect_user", sessionId, sessionId, "a-secret-token")

    // The record reaches logs through ordinary string interpolation, so toString is the boundary.
    assert(!binding.toString.contains("a-secret-token"))
    assert(binding.toString.contains("connect_user"))
  }
}
