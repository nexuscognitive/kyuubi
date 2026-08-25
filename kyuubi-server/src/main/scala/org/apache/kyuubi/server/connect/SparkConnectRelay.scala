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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NonFatal

import io.grpc.{CallOptions, ClientCall, Grpc, HandlerRegistry, Metadata, MethodDescriptor, ServerCall, ServerCallHandler, ServerMethodDefinition, Status}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.server.metadata.api.SparkConnectSessionInfo

/**
 * Hands every message through as opaque bytes.
 *
 * This is what makes the frontend a byte-level proxy: gRPC never learns what a Spark Connect
 * `Plan` looks like, so nothing here has to change when Spark adds a field, adds an RPC, or ships
 * a new protobuf schema.
 *
 * The bytes are copied out on ingest rather than the inbound stream being forwarded as-is. That
 * looks like a wasted copy and is not: gRPC reclaims a message's buffer as soon as the delivering
 * callback returns, while `sendMessage` on the far call does not always consume its argument
 * before returning -- if that call's transport is still connecting, gRPC queues the write on a
 * `DelayedStream` and replays it later, by which point a borrowed buffer has been freed and the
 * relay dies with "Failed executing read operation". Flow control keeps one message in flight per
 * direction, so this costs one message-sized array, not a backlog of them.
 */
private[connect] object PassThroughMarshaller extends MethodDescriptor.Marshaller[Array[Byte]] {

  override def stream(value: Array[Byte]): InputStream = new ByteArrayInputStream(value)

  override def parse(stream: InputStream): Array[Byte] = {
    val buffer = new ByteArrayOutputStream(math.max(stream.available(), 32))
    val chunk = new Array[Byte](8192)
    var read = stream.read(chunk)
    while (read >= 0) {
      buffer.write(chunk, 0, read)
      read = stream.read(chunk)
    }
    buffer.toByteArray
  }
}

/**
 * Claims every method under `spark.connect.SparkConnectService`.
 *
 * Registered as gRPC's fallback registry, so the frontend answers RPCs it has no generated stub
 * for -- which is all of them. Methods are treated as bidirectional streams regardless of what
 * they really are; that superset is safe for unary and server-streaming calls alike, and it means
 * no RPC has to be named anywhere in this file. Naming them individually is precisely how a proxy
 * like this ends up breaking `ReleaseExecute`, which PySpark calls after every response batch.
 */
private[kyuubi] class SparkConnectHandlerRegistry(relay: SparkConnectRelay)
  extends HandlerRegistry {

  override def lookupMethod(
      methodName: String,
      authority: String): ServerMethodDefinition[_, _] = {
    if (!methodName.startsWith(SparkConnect.SERVICE_PATH_PREFIX)) {
      return null
    }
    val descriptor = MethodDescriptor
      .newBuilder(PassThroughMarshaller, PassThroughMarshaller)
      .setFullMethodName(methodName)
      .setType(MethodDescriptor.MethodType.UNKNOWN)
      .build()
    ServerMethodDefinition.create(descriptor, relay)
  }
}

/**
 * Relays one Spark Connect RPC to the engine that serves the authenticated caller, in both
 * directions, without interpreting the payload.
 *
 * The caller presents the platform credential they already hold; there is no Spark Connect token
 * to obtain, and no token to lose. That credential is resolved to a user through Kyuubi's own
 * authentication chain and then stops here -- the upstream hop carries Kyuubi's per-engine
 * credential instead, so nothing the caller sent reaches a JVM they control.
 */
private[kyuubi] class SparkConnectRelay(
    authenticator: SparkConnectAuthenticator,
    sessionRegistry: SparkConnectSessionRegistry,
    engineLocator: SparkConnectEngineLocator,
    channelPool: SparkConnectEngineChannelPool)
  extends ServerCallHandler[Array[Byte], Array[Byte]] with Logging {

  import SparkConnectRelay._

  override def startCall(
      serverCall: ServerCall[Array[Byte], Array[Byte]],
      headers: Metadata): ServerCall.Listener[Array[Byte]] = {
    val method = serverCall.getMethodDescriptor.getFullMethodName
    SparkConnect.bearerToken(headers) match {
      case None =>
        debug(s"Rejecting $method: no bearer credential")
        reject(serverCall, Status.UNAUTHENTICATED.withDescription(MISSING_CREDENTIAL_MESSAGE))
      case Some(credential) =>
        authenticator.authenticate(credential, clientIpAddress(serverCall)) match {
          case None =>
            debug(s"Rejecting $method: the bearer credential did not resolve to a user")
            reject(
              serverCall,
              Status.UNAUTHENTICATED.withDescription(UNRESOLVED_CREDENTIAL_MESSAGE))
          case Some(userName) => route(serverCall, headers, userName, method)
        }
    }
  }

  private def route(
      serverCall: ServerCall[Array[Byte], Array[Byte]],
      headers: Metadata,
      userName: String,
      method: String): ServerCall.Listener[Array[Byte]] =
    sessionRegistry.liveSession(userName) match {
      case None =>
        debug(s"Rejecting $method: $userName has no live Spark Connect session")
        // Not UNAVAILABLE: that is what a Spark Connect client retries with backoff for minutes,
        // and no amount of retrying creates a session. FAILED_PRECONDITION surfaces the message
        // to the user on the first call instead.
        reject(serverCall, Status.FAILED_PRECONDITION.withDescription(NO_SESSION_MESSAGE))
      case Some(sessionInfo) =>
        engineLocator.locate(sessionInfo.engineTag) match {
          case None =>
            debug(s"Deferring $method for session ${sessionInfo.sessionId}:" +
              s" engine ${sessionInfo.engineTag} is not serving yet")
            reject(serverCall, Status.UNAVAILABLE.withDescription(ENGINE_NOT_READY_MESSAGE))
          case Some(address) => relay(serverCall, headers, sessionInfo, address)
        }
    }

  /**
   * The caller's address, for the authentication provider that wants to see it.
   *
   * Taken from the transport rather than from a forwarded header: a header is the caller's to
   * write, and an authorisation decision must not turn on something they choose.
   */
  private def clientIpAddress(serverCall: ServerCall[Array[Byte], Array[Byte]]): Option[String] =
    Option(serverCall.getAttributes.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR)).collect {
      case inetAddress: InetSocketAddress => inetAddress.getAddress
    }.flatMap(Option(_)).map(_.getHostAddress)

  /**
   * Answer with a trailers-only response: HTTP 200, `content-type: application/grpc`, and the
   * status in `grpc-status`/`grpc-message`, with no message frames at all.
   *
   * Closing before `sendHeaders` is what makes gRPC emit that shape. It matters for
   * `UNAVAILABLE`: Spark Connect clients treat it as retryable and back off for minutes, so an
   * engine that is still starting looks like a slow connect rather than a failure.
   */
  private def reject(
      serverCall: ServerCall[Array[Byte], Array[Byte]],
      status: Status): ServerCall.Listener[Array[Byte]] = {
    serverCall.close(status, new Metadata())
    new ServerCall.Listener[Array[Byte]] {}
  }

  private def relay(
      serverCall: ServerCall[Array[Byte], Array[Byte]],
      headers: Metadata,
      sessionInfo: SparkConnectSessionInfo,
      address: SparkConnectEngineAddress): ServerCall.Listener[Array[Byte]] = {
    val clientCall =
      try {
        channelPool.acquire(sessionInfo, address)
          .newCall(serverCall.getMethodDescriptor, CallOptions.DEFAULT)
      } catch {
        case NonFatal(e) =>
          error(s"Failed to open a Spark Connect channel to $address", e)
          return reject(serverCall, Status.UNAVAILABLE.withDescription(ENGINE_NOT_READY_MESSAGE))
      }
    val relayState = new RelayState(serverCall, clientCall)
    // The engine's own credential, not the caller's: the caller's terminates at this hop.
    val upstreamHeaders = SparkConnect.upstreamHeaders(headers, sessionInfo.engineToken)
    clientCall.start(relayState.clientListener, upstreamHeaders)
    relayState.start()
    relayState.serverListener
  }
}

private[connect] object SparkConnectRelay {

  private val MISSING_CREDENTIAL_MESSAGE =
    "Spark Connect requires a bearer credential; pass the same credential you use for the" +
      " Kyuubi REST API"
  private val UNRESOLVED_CREDENTIAL_MESSAGE =
    "The bearer credential was not accepted"
  private[connect] val NO_SESSION_MESSAGE =
    "You have no Spark Connect session. Create one with POST /api/v1/spark-connect/sessions, or" +
      " from the Spark Connect page of the Kyuubi web UI, and connect again once it is running."
  private val ENGINE_NOT_READY_MESSAGE =
    "The Spark engine for this session is still starting; retrying shortly will succeed"

  /**
   * Paces one direction of the relay so that at most one message is in flight beyond what gRPC
   * itself buffers.
   *
   * Without this, a `collect()` of a large result would be read from the engine as fast as the
   * engine can produce it and queued in the gateway's heap while a slow client drains it. Instead
   * the next message is requested only once the far side reports it can accept one, which pushes
   * the backlog back onto the engine where the data already lives.
   */
  private class FlowControlPacer(requestOne: () => Unit, isDestinationReady: () => Boolean) {
    private var deferred = false

    def demandNext(): Unit = synchronized {
      if (isDestinationReady()) requestOne() else deferred = true
    }

    def onDestinationReady(): Unit = synchronized {
      if (deferred) {
        deferred = false
        requestOne()
      }
    }
  }

  /**
   * The two half-duplex pipes that make up one relayed RPC, plus the bookkeeping that keeps their
   * termination consistent.
   */
  private class RelayState(
      serverCall: ServerCall[Array[Byte], Array[Byte]],
      clientCall: ClientCall[Array[Byte], Array[Byte]]) extends Logging {

    private val closed = new AtomicBoolean(false)
    private val headersSent = new AtomicBoolean(false)

    /** Client to engine: pull the next request only when the engine can take it. */
    private val requestPacer =
      new FlowControlPacer(() => serverCall.request(1), () => clientCall.isReady)

    /** Engine to client: pull the next response only when the client can take it. */
    private val responsePacer =
      new FlowControlPacer(() => clientCall.request(1), () => serverCall.isReady)

    /**
     * Prime both directions with one outstanding message.
     *
     * The first request is unconditional rather than gated on readiness: a call is not reported
     * ready until its transport has settled, and waiting for that before asking for anything
     * would stall every RPC on its own first message. One message per direction is still bounded;
     * from the second onwards the pacers take over.
     */
    def start(): Unit = {
      serverCall.request(1)
      clientCall.request(1)
    }

    val serverListener: ServerCall.Listener[Array[Byte]] =
      new ServerCall.Listener[Array[Byte]] {

        override def onMessage(message: Array[Byte]): Unit = {
          clientCall.sendMessage(message)
          requestPacer.demandNext()
        }

        override def onHalfClose(): Unit = clientCall.halfClose()

        override def onCancel(): Unit = {
          closed.set(true)
          clientCall.cancel("The Spark Connect client cancelled the call", null)
        }

        override def onReady(): Unit = responsePacer.onDestinationReady()
      }

    val clientListener: ClientCall.Listener[Array[Byte]] =
      new ClientCall.Listener[Array[Byte]] {

        override def onHeaders(headers: Metadata): Unit = {
          if (headersSent.compareAndSet(false, true)) {
            serverCall.sendHeaders(headers)
          }
        }

        override def onMessage(message: Array[Byte]): Unit = {
          // gRPC delivers onHeaders before any message, but sendMessage throws if headers have
          // not gone out, so make the ordering explicit rather than depend on it.
          if (headersSent.compareAndSet(false, true)) {
            serverCall.sendHeaders(new Metadata())
          }
          serverCall.sendMessage(message)
          responsePacer.demandNext()
        }

        /**
         * Pass the engine's outcome straight through -- code, message and trailers alike. Kyuubi
         * neither rewrites the status nor synthesises one of its own, so a Spark analysis error
         * still reaches the client as the exact `INTERNAL` plus `grpc-status-details-bin` that
         * Spark Connect clients unpack into a typed exception.
         */
        override def onClose(status: Status, trailers: Metadata): Unit = {
          if (closed.compareAndSet(false, true)) {
            try {
              serverCall.close(status, trailers)
            } catch {
              case NonFatal(e) =>
                warn("Failed to complete a relayed Spark Connect call", e)
            }
          }
        }

        override def onReady(): Unit = requestPacer.onDestinationReady()
      }
  }
}
