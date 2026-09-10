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

import java.net.{InetAddress, ServerSocket}

import org.scalatest.concurrent.{Signaler, ThreadSignaler, TimeLimits}
import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.server.connect.SparkConnectTestHelper.SESSION_NOT_FOUND_REPLY

/**
 * The liveness probe against real sockets: a stand-in engine that serves, one that refuses the
 * credential, a port nothing listens on, and one that accepts connections and never answers.
 */
class SparkConnectEngineProbeSuite extends KyuubiFunSuite with TimeLimits {

  // Interrupts a probe that overruns, so that a probe which is not bounded fails the test rather
  // than hanging the build.
  implicit private val signaler: Signaler = ThreadSignaler

  private val engineToken = SparkConnect.generateToken()

  private def withEngine(requiredToken: Option[String])(f: FakeSparkConnectEngine => Unit): Unit = {
    val engine = new FakeSparkConnectEngine(SESSION_NOT_FOUND_REPLY, requiredToken)
    try f(engine)
    finally engine.stop()
  }

  test("an engine that authenticates the call and answers it is serving") {
    withEngine(Some(engineToken)) { engine =>
      val probe = new GrpcSparkConnectEngineProbe(timeoutMillis = 5000)
      // The failure Spark answers with is the expected answer: it means a handler ran.
      assert(probe.isServing(engine.address, engineToken))
      assert(engine.callCount == 1)
      // Made the way the relay makes calls: with the engine's credential, not anybody else's.
      assert(SparkConnect.bearerToken(engine.receivedHeaders).contains(engineToken))
    }
  }

  test("an engine that refuses the binding's credential is not serving") {
    withEngine(Some(SparkConnect.generateToken())) { engine =>
      val probe = new GrpcSparkConnectEngineProbe(timeoutMillis = 5000)
      // Something answers on the port, but not the driver the binding was launched with -- the
      // relay could not route a single call there.
      assert(!probe.isServing(engine.address, engineToken))
      assert(engine.callCount == 1)
    }
  }

  test("a port nothing listens on is not serving") {
    val closedPort = {
      val socket = new ServerSocket(0)
      try socket.getLocalPort
      finally socket.close()
    }
    val probe = new GrpcSparkConnectEngineProbe(timeoutMillis = 5000)
    assert(!probe.isServing(SparkConnectEngineAddress("127.0.0.1", closedPort), engineToken))
  }

  test("a port that accepts a connection and never answers is abandoned at the timeout") {
    // The kernel completes the TCP handshake from the backlog, but nothing ever reads or writes:
    // a wedged driver, as far as a client can tell.
    val hungPort = new ServerSocket(0, 50, InetAddress.getLoopbackAddress)
    try {
      val timeoutMillis = 500L
      val probe = new GrpcSparkConnectEngineProbe(timeoutMillis)
      val address = SparkConnectEngineAddress("127.0.0.1", hungPort.getLocalPort)
      val started = System.nanoTime()
      val serving = failAfter(10.seconds) {
        probe.isServing(address, engineToken)
      }
      val elapsedMillis = (System.nanoTime() - started) / 1000000
      assert(!serving)
      // Bounded by the timeout, not by the far end: this runs on a REST request thread.
      assert(elapsedMillis >= timeoutMillis, s"gave up after ${elapsedMillis}ms")
      assert(elapsedMillis < timeoutMillis + 3000, s"took ${elapsedMillis}ms")
    } finally {
      hungPort.close()
    }
  }
}
