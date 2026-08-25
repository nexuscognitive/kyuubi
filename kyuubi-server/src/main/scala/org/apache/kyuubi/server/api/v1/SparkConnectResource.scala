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

import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}

import scala.collection.JavaConverters._

import io.swagger.v3.oas.annotations.media.{Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag

import org.apache.kyuubi.Logging
import org.apache.kyuubi.client.api.v1.dto.{SessionOpenRequest, SparkConnectSession}
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys._
import org.apache.kyuubi.server.api.ApiRequestContext
import org.apache.kyuubi.server.connect.SparkConnect
import org.apache.kyuubi.session.{KyuubiSessionManager, SessionHandle}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion

/**
 * Creates the Spark Connect sessions that the Spark Connect gRPC frontend then attaches to.
 *
 * Spark Connect has no open-session RPC of its own, so provisioning happens here instead of being
 * triggered by the first gRPC call. That is not merely tidier: an engine takes a minute or two to
 * come up, and a client that discovered this by having its first `ExecutePlan` block for that long
 * would look hung. Creating the session out of band lets the caller be told immediately that the
 * engine is on its way, while the gRPC port answers `UNAVAILABLE` -- which Spark Connect clients
 * retry with backoff -- until it is serving.
 */
@Tag(name = "SparkConnect")
@Produces(Array(MediaType.APPLICATION_JSON))
@Consumes(Array(MediaType.APPLICATION_JSON))
private[v1] class SparkConnectResource extends ApiRequestContext with Logging {

  import SparkConnectResource._

  private def sessionManager = fe.be.sessionManager.asInstanceOf[KyuubiSessionManager]

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[SparkConnectSession]))),
    description = "Create a Spark Connect session and return its bearer token")
  @POST
  @Path("sessions")
  def openSession(request: SessionOpenRequest): SparkConnectSession = {
    val requestedConf =
      Option(request).map(_.getConfigs.asScala.toMap).getOrElse(Map.empty[String, String])
    // The caller is authenticated by the REST frontend's own auth chain before reaching here;
    // getSessionUser additionally resolves any permitted proxy-user request.
    val userName = fe.getSessionUser(requestedConf)
    val ipAddress = fe.getIpAddress

    val token = SparkConnect.generateToken()
    val handle = fe.be.openSession(
      Option(request).flatMap(r => Option(r.getProtocolVersion))
        .map(version => TProtocolVersion.findByValue(version.intValue))
        .getOrElse(DEFAULT_SESSION_PROTOCOL_VERSION),
      userName,
      "",
      ipAddress,
      serverControlledConf(token) ++ Map(
        KYUUBI_CLIENT_IP_KEY -> ipAddress,
        KYUUBI_SERVER_IP_KEY -> fe.host,
        KYUUBI_SESSION_CONNECTION_URL_KEY -> fe.connectionUrl,
        KYUUBI_SESSION_REAL_USER_KEY -> fe.getRealUser()) ++
        clientControlledConf(requestedConf))

    val sessionId = handle.identifier.toString
    // For a CONNECTION-level engine -- which is forced above -- the `kyuubi-unique-tag` pod label
    // is the session id, because that is the engine reference id Kyuubi tags the driver with.
    sessionManager.sparkConnectSessionRegistry.register(
      token = token,
      sessionId = sessionId,
      userName = userName,
      engineTag = sessionId)
    info(s"Created Spark Connect session $sessionId for $userName")

    new SparkConnectSession(sessionId, token, connectUrl)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(mediaType = MediaType.APPLICATION_JSON)),
    description = "Close a Spark Connect session and stop its engine")
  @DELETE
  @Path("sessions/{sessionId}")
  def closeSession(@PathParam("sessionId") sessionId: String): Response = {
    val sessionHandle =
      try {
        SessionHandle.fromUUID(sessionId)
      } catch {
        case _: IllegalArgumentException =>
          throw new WebApplicationException("invalid sessionId", 400)
      }
    val session = sessionManager.getSessionOption(sessionHandle).getOrElse {
      throw new WebApplicationException("session not found", 404)
    }
    val userName = fe.getSessionUser(Map.empty[String, String])
    if (!fe.isAdministrator(userName) && session.user != userName) {
      throw new ForbiddenException(s"$userName is not allowed to close session $sessionId")
    }
    // Drops the routing record and the upstream connection as part of the close path.
    sessionManager.closeSession(sessionHandle)
    Response.ok().build()
  }

  /**
   * The Spark Connect URL for this instance's gRPC port.
   *
   * The scheme is always `sc://` and the connection is always TLS: the frontend refuses to start
   * without it, and Spark Connect clients upgrade to TLS on their own once a token is set.
   */
  private def connectUrl: String = {
    val conf = fe.getConf
    val advertisedHost = conf.get(FRONTEND_ADVERTISED_HOST)
      .orElse(conf.get(FRONTEND_SPARK_CONNECT_BIND_HOST))
      .getOrElse(fe.host)
    s"sc://$advertisedHost:${conf.get(FRONTEND_SPARK_CONNECT_BIND_PORT)}"
  }
}

private[v1] object SparkConnectResource {

  private val DEFAULT_SESSION_PROTOCOL_VERSION =
    SessionsResource.DEFAULT_SESSION_PROTOCOL_VERSION

  /**
   * Conf that only Kyuubi may set.
   *
   * The engine share level is pinned to `CONNECTION` for two reasons that both matter. A Spark
   * Connect session is stateful -- artifacts, temporary views, cached frames -- so sharing an
   * engine between sessions would leak that state across users. And the token is minted per
   * engine, so a shared engine would have to accept several tokens, which would turn the token
   * from a routing key into a set membership test.
   */
  private[v1] def serverControlledConf(token: String): Map[String, String] = Map(
    SESSION_SPARK_CONNECT_ENABLED.key -> "true",
    SESSION_SPARK_CONNECT_TOKEN.key -> token,
    ENGINE_TYPE.key -> "SPARK_SQL",
    ENGINE_SHARE_LEVEL.key -> "CONNECTION")

  private[v1] val SERVER_CONTROLLED_KEYS: Set[String] = Set(
    SESSION_SPARK_CONNECT_ENABLED.key,
    SESSION_SPARK_CONNECT_TOKEN.key,
    ENGINE_TYPE.key,
    ENGINE_SHARE_LEVEL.key)

  /**
   * The caller's conf, minus anything only Kyuubi may set.
   *
   * Stripping rather than rejecting keeps a client that echoes a previous session's conf working,
   * and a self-declared token would be inert anyway -- routing goes through the token store, which
   * only this endpoint writes -- but letting one through would still be a needless surprise.
   */
  private[v1] def clientControlledConf(requestedConf: Map[String, String]): Map[String, String] =
    requestedConf -- SERVER_CONTROLLED_KEYS
}
