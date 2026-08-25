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

import io.grpc.Metadata

import org.apache.kyuubi.KyuubiFunSuite

class SparkConnectSuite extends KyuubiFunSuite {

  private def asciiKey(name: String): Metadata.Key[String] =
    Metadata.Key.of(name, Metadata.ASCII_STRING_MARSHALLER)

  test("engine credentials are unique and URL safe") {
    val tokens = (1 to 200).map(_ => SparkConnect.generateToken()).toSet
    assert(tokens.size == 200)
    tokens.foreach { token =>
      assert(token.matches("[A-Za-z0-9_-]+"), s"$token is not URL safe")
      // 32 random bytes, base64url without padding.
      assert(token.length == 43)
    }
  }

  test("a token id is a stable SHA-256 hex digest") {
    val token = SparkConnect.generateToken()
    val id = SparkConnect.tokenId(token)
    assert(id.length == 64)
    assert(id.matches("[0-9a-f]+"))
    assert(id == SparkConnect.tokenId(token))
    assert(id != SparkConnect.tokenId(SparkConnect.generateToken()))
    // The digest must not be reversible to the token by simple inspection.
    assert(!id.contains(token))
  }

  test("token ids compare without leaking a prefix match") {
    val id = SparkConnect.tokenId("a-token")
    assert(SparkConnect.tokenIdsMatch(id, SparkConnect.tokenId("a-token")))
    assert(!SparkConnect.tokenIdsMatch(id, SparkConnect.tokenId("another-token")))
    assert(!SparkConnect.tokenIdsMatch(id, id.dropRight(1)))
  }

  test("bearer credentials are extracted from the Authorization header") {
    def headersWith(value: String): Metadata = {
      val headers = new Metadata()
      headers.put(SparkConnect.AUTHORIZATION_HEADER, value)
      headers
    }

    assert(SparkConnect.bearerToken(headersWith("Bearer abc123")).contains("abc123"))
    // Clients differ on capitalisation, and RFC 7235 makes the scheme case-insensitive.
    assert(SparkConnect.bearerToken(headersWith("bearer abc123")).contains("abc123"))
    assert(SparkConnect.bearerToken(headersWith("  Bearer   abc123  ")).contains("abc123"))
    assert(SparkConnect.bearerToken(headersWith("Basic abc123")).isEmpty)
    assert(SparkConnect.bearerToken(headersWith("Bearer ")).isEmpty)
    assert(SparkConnect.bearerToken(headersWith("abc123")).isEmpty)
    assert(SparkConnect.bearerToken(new Metadata()).isEmpty)
  }

  test("upstream headers carry Kyuubi's engine credential, not the caller's") {
    val headers = new Metadata()
    headers.put(SparkConnect.AUTHORIZATION_HEADER, "Bearer the-platform-credential")
    headers.put(asciiKey("cookie"), "session=platform-secret")
    headers.put(asciiKey("proxy-authorization"), "Basic anything")
    headers.put(asciiKey("x-spark-connect-client"), "pyspark/4.2.0")
    headers.put(asciiKey("user-agent"), "grpc-python/1.76")

    val upstream = SparkConnect.upstreamHeaders(headers, "the-engine-credential")

    assert(upstream.get(SparkConnect.AUTHORIZATION_HEADER) == "Bearer the-engine-credential")
    assert(upstream.get(asciiKey("cookie")) == null)
    assert(upstream.get(asciiKey("proxy-authorization")) == null)
    // Anything else passes through untouched, which is what keeps the proxy protocol-agnostic.
    assert(upstream.get(asciiKey("x-spark-connect-client")) == "pyspark/4.2.0")
    assert(upstream.get(asciiKey("user-agent")) == "grpc-python/1.76")
  }

  test("the caller's headers are not mutated when building upstream headers") {
    val headers = new Metadata()
    headers.put(SparkConnect.AUTHORIZATION_HEADER, "Bearer original")
    SparkConnect.upstreamHeaders(headers, "replacement")
    assert(headers.get(SparkConnect.AUTHORIZATION_HEADER) == "Bearer original")
  }

  test("the service path prefix matches Spark Connect's gRPC service") {
    assert(SparkConnect.SERVICE_PATH_PREFIX == "spark.connect.SparkConnectService/")
  }
}
