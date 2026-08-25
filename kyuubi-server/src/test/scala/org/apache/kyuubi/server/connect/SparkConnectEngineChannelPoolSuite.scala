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
import java.util.concurrent.{Callable, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import io.grpc.ManagedChannel
import io.grpc.netty.NettyChannelBuilder

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.server.metadata.api.SparkConnectSessionInfo

class SparkConnectEngineChannelPoolSuite extends KyuubiFunSuite {

  private val channelConf = SparkConnectEngineChannelConf(
    maxInboundMessageSize = 1024,
    keepAliveTimeMillis = 60000,
    keepAliveTimeoutMillis = 20000)

  private val created = new AtomicInteger(0)

  /**
   * gRPC channels connect lazily, so a channel to an address nothing is listening on is a
   * perfectly good stand-in as long as no call is made on it.
   */
  private def countingFactory(
      address: SparkConnectEngineAddress,
      conf: SparkConnectEngineChannelConf): ManagedChannel = {
    created.incrementAndGet()
    NettyChannelBuilder.forAddress(address.host, address.port).usePlaintext().build()
  }

  private def newPool(): SparkConnectEngineChannelPool = {
    created.set(0)
    new SparkConnectEngineChannelPool(channelConf, countingFactory)
  }

  private def sessionInfo(
      sessionId: String = UUID.randomUUID().toString,
      engineToken: String = SparkConnect.generateToken()): SparkConnectSessionInfo =
    SparkConnectSessionInfo(
      userName = "connect_user",
      sessionId = sessionId,
      engineTag = sessionId,
      engineToken = engineToken,
      createTime = System.currentTimeMillis())

  test("one session reuses a single connection across calls") {
    val pool = newPool()
    try {
      val session = sessionInfo()
      val address = SparkConnectEngineAddress("10.0.0.1", 15002)
      val first = pool.acquire(session, address)
      val second = pool.acquire(session, address)
      assert(first eq second)
      assert(created.get() == 1)
      assert(pool.pooledSessionCount == 1)
    } finally {
      pool.shutdown()
    }
  }

  test("concurrent acquires for one session still yield a single connection") {
    val pool = newPool()
    val executor = Executors.newFixedThreadPool(16)
    try {
      val session = sessionInfo()
      val address = SparkConnectEngineAddress("10.0.0.1", 15002)
      val tasks = (1 to 32).map { _ =>
        new Callable[ManagedChannel] {
          override def call(): ManagedChannel = pool.acquire(session, address)
        }
      }
      val channels = executor.invokeAll(tasks.asJava).asScala.map(_.get(30, TimeUnit.SECONDS))
      assert(channels.toSet.size == 1)
      assert(created.get() == 1)
    } finally {
      executor.shutdownNow()
      pool.shutdown()
    }
  }

  test("separate sessions never share a connection") {
    val pool = newPool()
    try {
      val address = SparkConnectEngineAddress("10.0.0.1", 15002)
      val first = pool.acquire(sessionInfo(), address)
      val second = pool.acquire(sessionInfo(), address)
      assert(first ne second)
      assert(pool.pooledSessionCount == 2)
    } finally {
      pool.shutdown()
    }
  }

  test("a pooled connection is discarded when the engine credential no longer matches") {
    val pool = newPool()
    try {
      val sessionId = UUID.randomUUID().toString
      val address = SparkConnectEngineAddress("10.0.0.1", 15002)
      val original = pool.acquire(sessionInfo(sessionId), address)
      // Same session id, different engine credential. Whatever produced that, the connection
      // authenticated to the first engine must not carry traffic meant for the second.
      val replacement = pool.acquire(sessionInfo(sessionId), address)

      assert(original ne replacement)
      assert(original.isShutdown)
      assert(created.get() == 2)
      assert(pool.pooledSessionCount == 1)
    } finally {
      pool.shutdown()
    }
  }

  test("a pooled connection is rebuilt when the engine moves") {
    val pool = newPool()
    try {
      val sessionId = UUID.randomUUID().toString
      val token = SparkConnect.generateToken()
      val original = pool.acquire(
        sessionInfo(sessionId, token),
        SparkConnectEngineAddress("10.0.0.1", 15002))
      val relocated = pool.acquire(
        sessionInfo(sessionId, token),
        SparkConnectEngineAddress("10.0.0.2", 15002))

      assert(original ne relocated)
      assert(original.isShutdown)
    } finally {
      pool.shutdown()
    }
  }

  test("releasing a session shuts its connection down") {
    val pool = newPool()
    try {
      val session = sessionInfo()
      val channel = pool.acquire(session, SparkConnectEngineAddress("10.0.0.1", 15002))
      pool.release(session.sessionId)

      assert(channel.isShutdown)
      assert(pool.pooledSessionCount == 0)
      // Releasing an unknown session is a no-op, since close runs for every session.
      pool.release(UUID.randomUUID().toString)
    } finally {
      pool.shutdown()
    }
  }

  test("shutdown closes every pooled connection") {
    val pool = newPool()
    val channels = (1 to 5).map { index =>
      pool.acquire(sessionInfo(), SparkConnectEngineAddress(s"10.0.0.$index", 15002))
    }
    pool.shutdown()

    assert(channels.forall(_.isShutdown))
    assert(pool.pooledSessionCount == 0)
  }
}
