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

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import scala.util.control.NonFatal

import io.grpc.{CallOptions, ClientCall, ManagedChannel, Metadata, MethodDescriptor, Status}
import io.grpc.netty.NettyChannelBuilder

import org.apache.kyuubi.Logging

/**
 * Answers whether an engine the informer says is running is actually serving Spark Connect.
 *
 * The informer knows a pod is `Running`; it does not know whether the driver inside it has bound
 * its Spark Connect port, is wedged, or is a different driver from the one whose credential the
 * binding holds. Only a call to the port can tell, and creating a session must not hand back an
 * engine on anything less.
 */
trait SparkConnectEngineProbe {

  /**
   * Whether the engine at `address` answers a Spark Connect call authenticated with
   * `engineToken`, within a bounded time.
   *
   * Must return rather than throw, and must return within its own timeout however the far end
   * behaves: it runs on a REST request thread.
   */
  def isServing(address: SparkConnectEngineAddress, engineToken: String): Boolean
}

object SparkConnectEngineProbe {

  /**
   * The outcomes that mean the relay could not use this engine: nothing answered, nothing
   * answered in time, or something answered and refused the engine's credential -- a driver that
   * is not the one the binding was launched with. Every other status is a Spark Connect server
   * that authenticated the call and ran a handler, which is what serving means.
   */
  val NOT_SERVING: Set[Status.Code] = Set(
    Status.Code.UNAVAILABLE,
    Status.Code.DEADLINE_EXCEEDED,
    Status.Code.UNAUTHENTICATED,
    Status.Code.CANCELLED)
}

/**
 * Probes with one real Spark Connect call, made the way the relay makes them: plaintext HTTP/2 to
 * the pod, carrying the engine's own credential.
 *
 * A TCP connect would only prove that something holds the port. This proves the driver answers
 * gRPC and -- because Spark's pre-shared-key interceptor checks the credential before any handler
 * runs -- that it accepts the credential the binding carries, which is what the relay will present
 * on every call it routes there. It costs one channel and one round trip, on a path taken once
 * per session create, and needs nothing beyond the gRPC transport the relay already uses.
 *
 * The call is `FetchErrorDetails` with an empty request. It looks a Spark session up by an empty
 * session id and fails, having created nothing; the failure is the expected answer, and only the
 * statuses in [[SparkConnectEngineProbe.NOT_SERVING]] mean the engine is not usable.
 */
class GrpcSparkConnectEngineProbe(timeoutMillis: Long) extends SparkConnectEngineProbe
  with Logging {

  import GrpcSparkConnectEngineProbe._

  override def isServing(address: SparkConnectEngineAddress, engineToken: String): Boolean = {
    var channel: ManagedChannel = null
    try {
      channel = NettyChannelBuilder.forAddress(address.host, address.port)
        .usePlaintext()
        .build()
      val status = call(channel, SparkConnect.upstreamHeaders(new Metadata(), engineToken))
      val serving = !SparkConnectEngineProbe.NOT_SERVING.contains(status.getCode)
      if (!serving) {
        info(s"The Spark Connect engine at $address is not serving: ${status.getCode}" +
          Option(status.getDescription).map(description => s" ($description)").getOrElse(""))
      }
      serving
    } catch {
      case NonFatal(e) =>
        warn(s"Failed to probe the Spark Connect engine at $address", e)
        false
    } finally {
      if (channel != null) {
        channel.shutdownNow()
      }
    }
  }

  /** Make the probe call and wait for its status, which the deadline guarantees will come. */
  private def call(channel: ManagedChannel, headers: Metadata): Status = {
    val clientCall = channel.newCall(
      PROBE_METHOD,
      // The deadline covers connecting as well as the call, so a port that accepts a TCP
      // connection and then never speaks HTTP/2 is bounded by it too.
      CallOptions.DEFAULT.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS))
    val closed = new CountDownLatch(1)
    val outcome = new AtomicReference[Status]()
    clientCall.start(
      new ClientCall.Listener[Array[Byte]] {
        override def onClose(status: Status, trailers: Metadata): Unit = {
          outcome.set(status)
          closed.countDown()
        }
      },
      headers)
    clientCall.request(1)
    clientCall.sendMessage(EMPTY_REQUEST)
    clientCall.halfClose()
    // Belt and braces: gRPC closes the call at the deadline, but a request thread must not rely
    // on a library timer to get back.
    if (closed.await(timeoutMillis + CLOSE_GRACE_MILLIS, TimeUnit.MILLISECONDS)) {
      outcome.get()
    } else {
      clientCall.cancel("the Spark Connect engine probe timed out", null)
      Status.DEADLINE_EXCEEDED
    }
  }
}

private[connect] object GrpcSparkConnectEngineProbe {

  private val PROBE_METHOD: MethodDescriptor[Array[Byte], Array[Byte]] =
    MethodDescriptor.newBuilder(PassThroughMarshaller, PassThroughMarshaller)
      .setFullMethodName(SparkConnect.SERVICE_PATH_PREFIX + "FetchErrorDetails")
      .setType(MethodDescriptor.MethodType.UNARY)
      .build()

  /** The empty protobuf message: every field of the request at its default. */
  private val EMPTY_REQUEST = Array.emptyByteArray

  private val CLOSE_GRACE_MILLIS = 1000L
}
