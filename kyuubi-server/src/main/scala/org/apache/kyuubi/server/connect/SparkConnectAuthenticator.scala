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

import java.util.concurrent.TimeUnit

import scala.util.control.NonFatal

import com.google.common.cache.{Cache, CacheBuilder}
import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.AUTHENTICATION_CUSTOM_BEARER_CLASS
import org.apache.kyuubi.service.authentication.{AuthenticationProviderFactory, Credential, DefaultTokenCredential, TokenAuthenticationProvider}

/**
 * Resolves the bearer credential on a Spark Connect gRPC call to the user who presented it.
 *
 * The credential goes through the same provider the HTTP frontend uses -- whatever
 * `kyuubi.authentication.custom.bearer.class` names -- so a deployment configures Spark Connect
 * authentication exactly once, alongside REST and Thrift HTTP, and nothing about the shape of a
 * particular platform's credentials is known here.
 */
private[kyuubi] class SparkConnectAuthenticator(
    conf: KyuubiConf,
    providerBuilder: () => TokenAuthenticationProvider)
  extends Logging {

  import SparkConnectAuthenticator._

  def this(conf: KyuubiConf) = this(
    conf,
    () =>
      AuthenticationProviderFactory.getHttpBearerAuthenticationProvider(
        conf.get(AUTHENTICATION_CUSTOM_BEARER_CLASS).orNull,
        conf))

  /**
   * One provider for the life of the frontend.
   *
   * [[AuthenticationProviderFactory.getHttpBearerAuthenticationProvider]] builds a fresh instance
   * on every call, which is affordable once per HTTP request and is not once per gRPC message:
   * PySpark issues a `ReleaseExecute` after every response batch, and a streamed `ExecutePlan`
   * would otherwise construct one provider per RPC on the same connection. Rebuilt on the next
   * call if construction fails, so a provider whose own dependencies were not ready at startup
   * still recovers.
   */
  @volatile private var cachedProvider: TokenAuthenticationProvider = _

  /**
   * Principals by credential digest, never by the credential itself.
   *
   * The digest is what makes a per-RPC resolution unnecessary without keeping anything
   * presentable in the gateway's heap. The entry is short-lived because it is the window in which
   * a credential the platform has already revoked still opens a call: [[CACHE_TTL_SECONDS]]
   * seconds. Failures are not cached -- a rejected credential costs a provider round trip every
   * time, which is the right way round for something anyone can attempt.
   */
  private val principalsByCredentialId: Cache[String, String] = CacheBuilder.newBuilder()
    .maximumSize(MAX_CACHE_SIZE)
    .expireAfterWrite(CACHE_TTL_SECONDS, TimeUnit.SECONDS)
    .build[String, String]()

  /**
   * The user `credential` belongs to, or [[None]] if it belongs to nobody.
   *
   * Every failure -- an unconfigured provider, a provider that threw, a principal with no name --
   * collapses to [[None]]. The caller answers all of them with `UNAUTHENTICATED` and no detail,
   * because telling an unauthenticated caller which of those it hit is telling them how to
   * probe.
   */
  def authenticate(credential: String, clientIpAddress: Option[String]): Option[String] = {
    if (StringUtils.isBlank(credential)) {
      return None
    }
    val credentialId = SparkConnect.tokenId(credential)
    Option(principalsByCredentialId.getIfPresent(credentialId)).orElse {
      val resolved = resolve(credential, clientIpAddress)
      resolved.foreach(principalsByCredentialId.put(credentialId, _))
      resolved
    }
  }

  private def resolve(credential: String, clientIpAddress: Option[String]): Option[String] =
    try {
      val extraInfo = clientIpAddress.map(Credential.CLIENT_IP_KEY -> _).toMap
      val principal = provider.authenticate(DefaultTokenCredential(credential, extraInfo))
      Option(principal).map(_.getName).filter(StringUtils.isNotBlank)
    } catch {
      case NonFatal(e) =>
        debug("A Spark Connect bearer credential did not resolve to a user", e)
        None
    }

  private def provider: TokenAuthenticationProvider = {
    val existing = cachedProvider
    if (existing != null) {
      return existing
    }
    // A benign race builds the provider twice and keeps one; taking a lock on the RPC path to
    // avoid that would cost more than the duplicate construction it prevents.
    val built = providerBuilder()
    cachedProvider = built
    built
  }

  private[connect] def cachedPrincipalCount: Long = principalsByCredentialId.size()
}

private[connect] object SparkConnectAuthenticator {

  /**
   * How long a resolved principal is trusted without asking the provider again.
   *
   * Short enough that a revoked credential stops working promptly, long enough that a busy
   * streaming session does not call the provider on every message.
   */
  private val CACHE_TTL_SECONDS = 60L

  private val MAX_CACHE_SIZE = 10000L
}
