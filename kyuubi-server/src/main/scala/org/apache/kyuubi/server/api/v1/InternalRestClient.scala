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

package org.apache.kyuubi.server.api.v1

import java.util.Base64

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.kyuubi.client.{AdminRestApi, BaseRestApi, BatchRestApi, KyuubiRestClient}
import org.apache.kyuubi.client.api.v1.dto.{Batch, CloseBatchResponse, OperationData, OperationLog, SessionData}
import org.apache.kyuubi.client.auth.AuthHeaderGenerator
import org.apache.kyuubi.server.http.authentication.AuthSchemes
import org.apache.kyuubi.service.authentication.InternalSecurityAccessor

/**
 * The internal rest client for request redirection and it is shared for all the requests
 * redirected to the same Kyuubi instance.
 *
 * @param kyuubiInstance the kyuubi instance host:port.
 * @param socketTimeout the socket timeout for http client.
 * @param connectTimeout the connect timeout for http client.
 * @param securityEnabled if enable secure access.
 * @param requestMaxAttempts the request max attempts for http client.
 * @param requestAttemptWait the request attempt wait for http client.
 */
class InternalRestClient(
    kyuubiInstance: String,
    proxyClientIpHeader: String,
    socketTimeout: Int,
    connectTimeout: Int,
    securityEnabled: Boolean,
    requestMaxAttempts: Int,
    requestAttemptWait: Int) {
  if (securityEnabled) {
    require(
      InternalSecurityAccessor.get() != null,
      "Internal secure access across Kyuubi instances is not enabled")
  }

  private val internalBatchRestApi = new BatchRestApi(initKyuubiRestClient())
  private val internalBaseRestApi = new BaseRestApi(initKyuubiRestClient())
  private val internalAdminRestApi = new AdminRestApi(initKyuubiRestClient())

  def pingAble(): Boolean = Try(internalBaseRestApi.ping()).isSuccess

  /**
   * List the sessions held in the peer instance's local memory only.
   *
   * Uses the non-aggregating `listSessions()` form (no clusterWide flag) so the peer
   * never fans out again - this is the recursion guard for cluster-wide reads.
   */
  def listLocalSessions(user: String): Seq[SessionData] = withAuthUser(user) {
    internalAdminRestApi.listSessions().asScala.toSeq
  }

  /**
   * List the operations held in the peer instance's local memory only.
   * Non-aggregating form, see [[listLocalSessions]] for the recursion guard rationale.
   */
  def listLocalOperations(user: String): Seq[OperationData] = withAuthUser(user) {
    internalAdminRestApi.listOperations().asScala.toSeq
  }

  /**
   * Close a session on the peer instance. The forwarded close carries no kyuubiInstance
   * hint, so the peer acts locally and never re-forwards (recursion guard for cancels).
   */
  def closeSession(user: String, sessionHandle: String): String = withAuthUser(user) {
    internalAdminRestApi.closeSession(sessionHandle)
  }

  /**
   * Close an operation on the peer instance. No kyuubiInstance hint => peer acts locally.
   */
  def closeOperation(user: String, operationHandle: String): String = withAuthUser(user) {
    internalAdminRestApi.closeOperation(operationHandle)
  }

  def getBatch(user: String, clientIp: String, batchId: String): Batch = {
    withAuthUser(user) {
      internalBatchRestApi.getBatchById(batchId, Map(proxyClientIpHeader -> clientIp).asJava)
    }
  }

  def getBatchLocalLog(
      user: String,
      clientIp: String,
      batchId: String,
      from: Int,
      size: Int): OperationLog = {
    withAuthUser(user) {
      internalBatchRestApi.getBatchLocalLog(
        batchId,
        from,
        size,
        Map(proxyClientIpHeader -> clientIp).asJava)
    }
  }

  def deleteBatch(user: String, clientIp: String, batchId: String): CloseBatchResponse = {
    withAuthUser(user) {
      internalBatchRestApi.deleteBatch(batchId, Map(proxyClientIpHeader -> clientIp).asJava)
    }
  }

  private def initKyuubiRestClient(): KyuubiRestClient = {
    val builder = KyuubiRestClient.builder(s"http://$kyuubiInstance")
      .apiVersion(KyuubiRestClient.ApiVersion.V1)
      .socketTimeout(socketTimeout)
      .connectionTimeout(connectTimeout)
      .maxAttempts(requestMaxAttempts)
      .attemptWaitTime(requestAttemptWait)
    if (securityEnabled) {
      builder.authHeaderGenerator(InternalRestClient.internalAuthHeaderGenerator)
    }
    builder.build()
  }

  private def withAuthUser[T](user: String)(f: => T): T = {
    try {
      InternalRestClient.AUTH_USER.set(user)
      f
    } finally {
      InternalRestClient.AUTH_USER.remove()
    }
  }
}

object InternalRestClient {
  final val AUTH_USER = new ThreadLocal[String]() {
    override def initialValue(): String = null
  }

  final lazy val internalAuthHeaderGenerator = new AuthHeaderGenerator {
    override def generateAuthHeader(): String = {
      val authUser = AUTH_USER.get()
      require(authUser != null, "The auth user shall be not null")
      val encodedAuthorization = new String(
        Base64.getEncoder.encode(
          s"$authUser:${InternalSecurityAccessor.get().issueToken()}".getBytes()),
        "UTF-8")
      s"${AuthSchemes.KYUUBI_INTERNAL.toString} $encodedAuthorization"
    }
  }
}
