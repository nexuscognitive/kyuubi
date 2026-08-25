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
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ListBuffer

import io.grpc.{CallOptions, ClientCall, HandlerRegistry, ManagedChannel, Metadata, MethodDescriptor, Server, ServerCall, ServerCallHandler, ServerMethodDefinition, Status}
import io.grpc.netty.{NettyChannelBuilder, NettyServerBuilder}

/**
 * Marshals messages as raw bytes so tests can assert on the exact payload that
 * crossed the wire.
 */
object ByteArrayMarshaller extends MethodDescriptor.Marshaller[Array[Byte]] {

  override def stream(value: Array[Byte]): InputStream = new ByteArrayInputStream(value)

  override def parse(stream: InputStream): Array[Byte] = {
    val buffer = new ByteArrayOutputStream()
    val chunk = new Array[Byte](4096)
    var read = stream.read(chunk)
    while (read >= 0) {
      buffer.write(chunk, 0, read)
      read = stream.read(chunk)
    }
    buffer.toByteArray
  }
}

/** What the engine should do with one call, once the client has finished sending. */
case class FakeEngineReply(
    responses: Seq[Array[Byte]] = Seq.empty,
    status: Status = Status.OK,
    trailers: Metadata = new Metadata())

/**
 * A stand-in for a Spark driver's Spark Connect server.
 *
 * A real gRPC server on a real port rather than an in-process one, so the tests exercise genuine
 * HTTP/2 framing, headers and trailers -- which is the whole of what the proxy under test is
 * responsible for preserving.
 */
class FakeSparkConnectEngine(reply: (String, Seq[Array[Byte]]) => FakeEngineReply) {

  @volatile private var lastHeaders: Metadata = _
  private val callCounter = new AtomicInteger(0)

  private val handler = new ServerCallHandler[Array[Byte], Array[Byte]] {
    override def startCall(
        call: ServerCall[Array[Byte], Array[Byte]],
        headers: Metadata): ServerCall.Listener[Array[Byte]] = {
      lastHeaders = headers
      callCounter.incrementAndGet()
      call.request(1)
      val received = ListBuffer[Array[Byte]]()
      new ServerCall.Listener[Array[Byte]] {
        override def onMessage(message: Array[Byte]): Unit = {
          received += message
          call.request(1)
        }

        override def onHalfClose(): Unit = {
          val outcome = reply(call.getMethodDescriptor.getFullMethodName, received.toSeq)
          if (outcome.status.isOk) {
            call.sendHeaders(new Metadata())
            outcome.responses.foreach(call.sendMessage)
          }
          call.close(outcome.status, outcome.trailers)
        }
      }
    }
  }

  private val server: Server = NettyServerBuilder.forPort(0)
    .fallbackHandlerRegistry(new HandlerRegistry {
      override def lookupMethod(
          methodName: String,
          authority: String): ServerMethodDefinition[_, _] =
        ServerMethodDefinition.create(
          SparkConnectTestHelper.methodDescriptor(methodName),
          handler)
    })
    .build()
    .start()

  def port: Int = server.getPort

  def address: SparkConnectEngineAddress = SparkConnectEngineAddress("127.0.0.1", port)

  def receivedHeaders: Metadata = lastHeaders

  def callCount: Int = callCounter.get()

  def stop(): Unit = {
    server.shutdownNow()
    server.awaitTermination(5, TimeUnit.SECONDS)
  }
}

/** Everything a client observed about one completed call. */
case class ProxyCallResult(
    headersReceived: Boolean,
    responses: Seq[Array[Byte]],
    status: Status,
    trailers: Metadata) {

  /**
   * A trailers-only response carries the status with no message frames and no separate headers
   * frame -- the shape a Spark Connect client reads as a clean, retryable rejection.
   */
  def isTrailersOnly: Boolean = !headersReceived && responses.isEmpty
}

object SparkConnectTestHelper {

  val EXECUTE_PLAN_METHOD: String = SparkConnect.SERVICE_PATH_PREFIX + "ExecutePlan"
  val RELEASE_EXECUTE_METHOD: String = SparkConnect.SERVICE_PATH_PREFIX + "ReleaseExecute"

  def methodDescriptor(methodName: String): MethodDescriptor[Array[Byte], Array[Byte]] =
    MethodDescriptor.newBuilder(ByteArrayMarshaller, ByteArrayMarshaller)
      .setFullMethodName(methodName)
      .setType(MethodDescriptor.MethodType.UNKNOWN)
      .build()

  def clientChannel(port: Int): ManagedChannel =
    NettyChannelBuilder.forAddress("127.0.0.1", port).usePlaintext().build()

  /** Drive one call to completion and report everything the client saw. */
  def call(
      channel: ManagedChannel,
      methodName: String,
      requests: Seq[Array[Byte]],
      headers: Metadata): ProxyCallResult = {
    val clientCall: ClientCall[Array[Byte], Array[Byte]] =
      channel.newCall(methodDescriptor(methodName), CallOptions.DEFAULT)
    val completed = new CountDownLatch(1)
    val responses = ListBuffer[Array[Byte]]()
    var headersReceived = false
    var status: Status = null
    var trailers: Metadata = null

    clientCall.start(
      new ClientCall.Listener[Array[Byte]] {
        override def onHeaders(receivedHeaders: Metadata): Unit = headersReceived = true

        override def onMessage(message: Array[Byte]): Unit = {
          responses += message
          clientCall.request(1)
        }

        override def onClose(closeStatus: Status, closeTrailers: Metadata): Unit = {
          status = closeStatus
          trailers = closeTrailers
          completed.countDown()
        }
      },
      headers)
    clientCall.request(1)
    // A rejected call is closed by the proxy before the client has finished sending, so losing
    // the race to send is an ordinary outcome here rather than a failure.
    try {
      requests.foreach(clientCall.sendMessage)
      clientCall.halfClose()
    } catch {
      case _: IllegalStateException =>
    }

    assert(completed.await(30, TimeUnit.SECONDS), s"call to $methodName did not complete")
    ProxyCallResult(headersReceived, responses.toSeq, status, trailers)
  }

  def bearerHeaders(token: String, extra: Map[String, String] = Map.empty): Metadata = {
    val headers = new Metadata()
    headers.put(SparkConnect.AUTHORIZATION_HEADER, s"Bearer $token")
    extra.foreach { case (name, value) =>
      headers.put(Metadata.Key.of(name, Metadata.ASCII_STRING_MARSHALLER), value)
    }
    headers
  }
}
