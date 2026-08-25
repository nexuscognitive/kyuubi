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

import java.security.Principal
import java.util.concurrent.atomic.AtomicInteger

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.authentication.{BasicPrincipal, TokenAuthenticationProvider, TokenCredential}

class SparkConnectAuthenticatorSuite extends KyuubiFunSuite {

  private val conf = KyuubiConf(loadSysDefault = false)

  private def authenticatorFor(provider: TokenAuthenticationProvider): SparkConnectAuthenticator =
    new SparkConnectAuthenticator(conf, () => provider)

  test("a known credential resolves to the user the provider names") {
    val authenticator = authenticatorFor(
      new FakeTokenAuthenticationProvider(Map("alice-credential" -> "alice")))

    assert(authenticator.authenticate("alice-credential", None).contains("alice"))
  }

  test("a credential the provider rejects resolves to nobody") {
    val authenticator = authenticatorFor(
      new FakeTokenAuthenticationProvider(Map("alice-credential" -> "alice")))

    assert(authenticator.authenticate("someone-elses-credential", None).isEmpty)
    assert(authenticator.authenticate("", None).isEmpty)
    assert(authenticator.authenticate("   ", None).isEmpty)
  }

  test("a resolved principal is reused rather than re-resolved") {
    val provider = new FakeTokenAuthenticationProvider(Map("alice-credential" -> "alice"))
    val authenticator = authenticatorFor(provider)

    (1 to 50).foreach(_ =>
      assert(authenticator.authenticate("alice-credential", None).contains("alice")))

    assert(provider.callCount == 1)
  }

  test("a rejected credential is asked about every time and never cached") {
    // Anyone who can reach the port can present garbage. Caching those answers would let them
    // fill the cache on demand, and would keep a credential working after it started to fail.
    val provider = new FakeTokenAuthenticationProvider(Map.empty)
    val authenticator = authenticatorFor(provider)

    (1 to 10).foreach(_ => assert(authenticator.authenticate("garbage", None).isEmpty))

    assert(provider.callCount == 10)
    assert(authenticator.cachedPrincipalCount == 0)
  }

  test("distinct credentials get distinct users") {
    val authenticator = authenticatorFor(new FakeTokenAuthenticationProvider(
      Map("alice-credential" -> "alice", "bob-credential" -> "bob")))

    assert(authenticator.authenticate("alice-credential", None).contains("alice"))
    assert(authenticator.authenticate("bob-credential", None).contains("bob"))
    assert(authenticator.cachedPrincipalCount == 2)
  }

  test("the provider is built once rather than per call") {
    // AuthenticationProviderFactory builds a fresh instance every time it is asked, which is
    // affordable once per HTTP request and is not once per gRPC message.
    val builds = new AtomicInteger(0)
    val authenticator = new SparkConnectAuthenticator(
      conf,
      () => {
        builds.incrementAndGet()
        new FakeTokenAuthenticationProvider(Map("alice-credential" -> "alice"))
      })

    (1 to 5).foreach(index =>
      assert(authenticator.authenticate(s"credential-$index", None).isEmpty))
    assert(authenticator.authenticate("alice-credential", None).contains("alice"))

    assert(builds.get() == 1)
  }

  test("the client address reaches the provider that wants it") {
    var seenExtraInfo = Map.empty[String, String]
    val authenticator = authenticatorFor(new TokenAuthenticationProvider {
      override def authenticate(credential: TokenCredential): Principal = {
        seenExtraInfo = credential.extraInfo
        new BasicPrincipal("alice")
      }
    })

    authenticator.authenticate("alice-credential", Some("10.42.0.7"))

    assert(seenExtraInfo.get("clientIp").contains("10.42.0.7"))
  }

  test("a principal with no name is not a user") {
    val authenticator = authenticatorFor(new TokenAuthenticationProvider {
      override def authenticate(credential: TokenCredential): Principal = new BasicPrincipal("")
    })

    assert(authenticator.authenticate("alice-credential", None).isEmpty)
  }

  test("a provider that cannot be built rejects rather than propagates") {
    // A provider whose own dependencies are not up yet must look like a failed authentication,
    // not like a broken gRPC server.
    val authenticator = new SparkConnectAuthenticator(
      conf,
      () => throw new IllegalStateException("the identity service is not up"))

    assert(authenticator.authenticate("alice-credential", None).isEmpty)
  }
}
