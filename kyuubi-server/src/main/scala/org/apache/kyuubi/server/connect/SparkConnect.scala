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

import java.nio.charset.StandardCharsets
import java.security.{MessageDigest, SecureRandom}
import java.util.Base64

import io.grpc.Metadata

/**
 * Shared names and small helpers for the Spark Connect frontend.
 */
object SparkConnect {

  /**
   * HTTP/2 `:path` prefix of every RPC on Spark Connect's `SparkConnectService`.
   *
   * The frontend dispatches on this prefix alone and forwards the request and response bodies
   * without decoding them, which is what keeps it independent of the Spark version -- and of the
   * protobuf schema -- on either side of the hop.
   */
  val SERVICE_PATH_PREFIX = "spark.connect.SparkConnectService/"

  /**
   * Environment variable that carries the per-engine token to the Spark driver.
   *
   * Spark reads it into `spark.connect.authenticate.token`. Passing it as an environment variable
   * rather than `--conf` keeps it out of the driver's command line and out of the Spark UI
   * environment page.
   */
  val AUTHENTICATE_TOKEN_ENV = "SPARK_CONNECT_AUTHENTICATE_TOKEN"

  /** Spark conf key that turns the Spark Connect gRPC server on inside the driver. */
  val SPARK_PLUGINS_KEY = "spark.plugins"

  /** The plugin that starts Spark Connect inside an otherwise ordinary Spark application. */
  val SPARK_CONNECT_PLUGIN = "org.apache.spark.sql.connect.SparkConnectPlugin"

  /** Spark conf key for the port the driver's Spark Connect gRPC server binds to. */
  val SPARK_CONNECT_BINDING_PORT_KEY = "spark.connect.grpc.binding.port"

  val AUTHORIZATION_HEADER: Metadata.Key[String] =
    Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER)

  private val BEARER_PREFIX = "Bearer "

  /**
   * Headers that must not reach the engine.
   *
   * The engine authenticates the hop with the per-engine token that Kyuubi itself sets, so
   * whatever credential the caller used to reach Kyuubi -- which is now their long-lived platform
   * credential, with far more authority than one Spark session -- is dropped here rather than
   * relayed into a user-controlled JVM.
   */
  private val STRIPPED_HEADER_NAMES =
    Set("authorization", "cookie", "proxy-authorization", "www-authenticate", "set-cookie")

  private val TOKEN_BYTES = 32

  private val secureRandom = new SecureRandom()

  /**
   * Mint the credential Kyuubi presents to one engine: 256 bits of entropy, URL-safe so it
   * survives an HTTP header. It is Kyuubi's own and is never handed to a client.
   */
  def generateToken(): String = {
    val bytes = new Array[Byte](TOKEN_BYTES)
    secureRandom.nextBytes(bytes)
    Base64.getUrlEncoder.withoutPadding().encodeToString(bytes)
  }

  /**
   * The SHA-256 hex digest of a credential.
   *
   * Used wherever a credential has to be named without being kept -- as a cache key, or as the
   * identity of a pooled connection -- so that neither a heap dump nor a log line yields
   * something presentable.
   */
  def tokenId(token: String): String = {
    val digest = MessageDigest.getInstance("SHA-256")
      .digest(token.getBytes(StandardCharsets.UTF_8))
    digest.map(byte => f"${byte & 0xFF}%02x").mkString
  }

  /** Compare two token digests without leaking their prefix length through timing. */
  def tokenIdsMatch(left: String, right: String): Boolean =
    MessageDigest.isEqual(
      left.getBytes(StandardCharsets.UTF_8),
      right.getBytes(StandardCharsets.UTF_8))

  /** The bearer token from an `Authorization` header, if one is present and well formed. */
  def bearerToken(headers: Metadata): Option[String] =
    Option(headers.get(AUTHORIZATION_HEADER))
      .map(_.trim)
      .filter(_.regionMatches(true, 0, BEARER_PREFIX, 0, BEARER_PREFIX.length))
      .map(_.substring(BEARER_PREFIX.length).trim)
      .filter(_.nonEmpty)

  /**
   * Copy the caller's headers for the upstream hop, replacing their credential with ours.
   *
   * Everything the client sent is passed through untouched apart from [[STRIPPED_HEADER_NAMES]],
   * so Spark Connect headers the frontend has never heard of still reach the engine.
   */
  def upstreamHeaders(headers: Metadata, token: String): Metadata = {
    val forwarded = new Metadata()
    forwarded.merge(headers)
    STRIPPED_HEADER_NAMES.foreach { name =>
      forwarded.removeAll(Metadata.Key.of(name, Metadata.ASCII_STRING_MARSHALLER))
    }
    forwarded.put(AUTHORIZATION_HEADER, BEARER_PREFIX + token)
    forwarded
  }
}
