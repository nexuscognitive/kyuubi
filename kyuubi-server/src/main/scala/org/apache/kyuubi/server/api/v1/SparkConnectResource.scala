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

import java.util.Collections
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}

import scala.collection.JavaConverters._

import io.swagger.v3.oas.annotations.media.{ArraySchema, Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag

import org.apache.kyuubi.Logging
import org.apache.kyuubi.client.api.v1.dto.{OperationLog, SessionOpenRequest, SparkConnectDriverContainer, SparkConnectDriverEvent, SparkConnectDriverEvents, SparkConnectDriverInfo, SparkConnectSession}
import org.apache.kyuubi.client.api.v1.dto.SparkConnectSessionData
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys._
import org.apache.kyuubi.engine.{KubernetesApplicationOperation, KubernetesDriverContainer, KubernetesDriverPodEvent}
import org.apache.kyuubi.engine.spark.SparkProcessBuilder
import org.apache.kyuubi.server.api.ApiRequestContext
import org.apache.kyuubi.server.connect.SparkConnect
import org.apache.kyuubi.session.{KyuubiSession, KyuubiSessionImpl, KyuubiSessionManager, SessionHandle}
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
 *
 * Nothing here issues a credential. A caller reaches both this endpoint and the gRPC port with the
 * platform credential they already hold, so the only thing a session gives them is an engine.
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
    description = "Create the caller's Spark Connect session, or return the one they already have")
  @POST
  @Path("sessions")
  def openSession(request: SessionOpenRequest): SparkConnectSession = {
    val requestedConf =
      Option(request).map(_.getConfigs.asScala.toMap).getOrElse(Map.empty[String, String])
    // The caller is authenticated by the REST frontend's own auth chain before reaching here;
    // getSessionUser additionally resolves any permitted proxy-user request.
    val userName = fe.getSessionUser(requestedConf)
    val registry = sessionManager.sparkConnectSessionRegistry

    // One session per user. The gRPC port routes on the caller's identity, so a second session
    // would be unreachable -- and the conf on this request cannot be applied to an engine that is
    // already running anyway, which is why it is dropped rather than quietly half-honoured.
    registry.liveSession(userName) match {
      case Some(existing) =>
        info(s"Returning the existing Spark Connect session ${existing.sessionId} for $userName")
        new SparkConnectSession(existing.sessionId, connectUrl)
      case None => createSession(userName, requestedConf, request)
    }
  }

  private def createSession(
      userName: String,
      requestedConf: Map[String, String],
      request: SessionOpenRequest): SparkConnectSession = {
    val registry = sessionManager.sparkConnectSessionRegistry

    // The engine is shared at USER level, so one left running by a previous session of this
    // user's is handed straight back by engine discovery instead of being relaunched. It keeps
    // the `kyuubi-unique-tag` and the credential it was launched with, both of which the new
    // session has to inherit: the tag is what the frontend routes on, and the token in the
    // driver's environment cannot be changed from out here.
    val reusableEngine = registry.lookup(userName)
      .filter(engine => sessionManager.sparkConnectEngineLocator.locate(engine.engineTag).nonEmpty)
    val engineToken = reusableEngine.map(_.engineToken).getOrElse(SparkConnect.generateToken())

    val ipAddress = fe.getIpAddress
    val handle = fe.be.openSession(
      Option(request).flatMap(r => Option(r.getProtocolVersion))
        .map(version => TProtocolVersion.findByValue(version.intValue))
        .getOrElse(DEFAULT_SESSION_PROTOCOL_VERSION),
      userName,
      "",
      ipAddress,
      serverControlledConf(engineToken) ++ Map(
        KYUUBI_CLIENT_IP_KEY -> ipAddress,
        KYUUBI_SERVER_IP_KEY -> fe.host,
        KYUUBI_SESSION_CONNECTION_URL_KEY -> fe.connectionUrl,
        KYUUBI_SESSION_REAL_USER_KEY -> fe.getRealUser()) ++
        clientControlledConf(requestedConf))

    val sessionId = handle.identifier.toString
    // A newly launched engine carries this session's id as its `kyuubi-unique-tag` pod label,
    // because that is the engine reference id Kyuubi tags the driver with.
    val engineTag = reusableEngine.map(_.engineTag).getOrElse(sessionId)
    registry.register(
      userName = userName,
      sessionId = sessionId,
      engineTag = engineTag,
      engineToken = engineToken)
    info(s"Created Spark Connect session $sessionId for $userName on engine $engineTag")

    new SparkConnectSession(sessionId, connectUrl)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      array = new ArraySchema(
        schema = new Schema(implementation = classOf[SparkConnectSessionData])))),
    description = "List the caller's live Spark Connect sessions")
  @GET
  @Path("sessions")
  def listSessions(): Seq[SparkConnectSessionData] = {
    val userName = fe.getSessionUser(Map.empty[String, String])
    // Scoped to the caller rather than to administrators as well: unlike the close path, which an
    // administrator has to be able to reach to clear up a stuck engine, listing someone else's
    // sessions buys nothing that the ordinary session list does not already offer.
    sessionManager.allSessions()
      .collect { case session: KyuubiSession if isSparkConnectSession(session.conf) => session }
      .filter(_.user == userName)
      .map(sessionData)
      .toSeq
      .sortBy(session => -session.getCreateTime.longValue())
  }

  private def sessionData(session: KyuubiSession): SparkConnectSessionData = {
    val event = session.getSessionEvent
    new SparkConnectSessionData(
      session.handle.identifier.toString,
      session.user,
      session.createTime,
      sessionState(
        openedTime = event.map(_.openedTime).getOrElse(-1L),
        endTime = event.map(_.endTime).getOrElse(-1L),
        failed = event.exists(_.exception.isDefined)),
      event.map(_.engineId).getOrElse(""),
      event.map(_.engineUrl).getOrElse(""),
      connectUrl)
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
   * The caller's own Spark Connect session, or a 4xx.
   *
   * Scoped to the caller alone, the way the session list is, and deliberately without the
   * administrator exemption the close path carries: an engine someone else has to clear up is a
   * different thing from that user's submit log, driver log and Kubernetes events, all of which
   * can carry their query text, their table names and their data.
   */
  private def resolveOwnSession(sessionId: String): KyuubiSessionImpl = {
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
    if (session.user != userName) {
      throw new ForbiddenException(s"$userName is not allowed to access session $sessionId")
    }
    session match {
      case kyuubiSession: KyuubiSessionImpl if isSparkConnectSession(kyuubiSession.conf) =>
        kyuubiSession
      case _ =>
        // Reachable through this path only for a session opened on another frontend, which has
        // its own endpoints; answering 404 keeps this resource about Spark Connect sessions.
        throw new WebApplicationException("not a Spark Connect session", 404)
    }
  }

  /**
   * The `kyuubi-unique-tag` labelling this session's driver pod.
   *
   * Taken from the routing record rather than assumed to be the session id, because a session
   * handed a reusable engine inherits that engine's older tag -- which is the one the pod carries.
   */
  private def engineTag(session: KyuubiSessionImpl): String =
    sessionManager.sparkConnectSessionRegistry.lookup(session.user)
      .map(_.engineTag)
      .getOrElse(session.handle.identifier.toString)

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[OperationLog]))),
    description = "Get the engine submit log for the caller's own Spark Connect session")
  @GET
  @Path("sessions/{sessionId}/log")
  def getSubmitLog(
      @PathParam("sessionId") sessionId: String,
      @QueryParam("from") @DefaultValue("-1") from: Int,
      @QueryParam("size") @DefaultValue("100") size: Int): OperationLog = {
    val session = resolveOwnSession(sessionId)
    // The launch operation's log is the `spark-submit` output Kyuubi captures in its work
    // directory -- the only place a launch that never produced a driver pod says anything.
    val logRowSet = Option(session.launchEngineOp).flatMap(_.getOperationLog) match {
      case Some(operationLog) =>
        val columns = operationLog.read(from, size).getColumns
        if (columns == null || columns.isEmpty) {
          Collections.emptyList[String]()
        } else {
          columns.get(0).getStringVal.getValues
        }
      case None =>
        // Operation logging can be switched off deployment-wide, and a session restored on a
        // peer never had a local log to begin with. Neither is an error.
        List(NO_SUBMIT_LOG_MESSAGE).asJava
    }
    new OperationLog(logRowSet, logRowSet.size)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[SparkConnectDriverInfo]))),
    description = "Get the driver pod of the caller's own Spark Connect session")
  @GET
  @Path("sessions/{sessionId}/driver")
  def getDriverInfo(@PathParam("sessionId") sessionId: String): SparkConnectDriverInfo = {
    val session = resolveOwnSession(sessionId)
    val event = session.getSessionEvent
    val engineId = event.map(_.engineId).getOrElse("")
    val engineUrl = event.map(_.engineUrl).getOrElse("")
    kubernetesOperation match {
      case None =>
        unavailableDriverInfo(sessionId, NOT_KUBERNETES_MESSAGE, engineId, engineUrl)
      case Some(operation) =>
        operation.getDriverPodDetailByTag(engineTag(session)) match {
          case None =>
            unavailableDriverInfo(sessionId, NO_DRIVER_POD_MESSAGE, engineId, engineUrl)
          case Some(pod) =>
            new SparkConnectDriverInfo(
              sessionId,
              true,
              null,
              engineId,
              engineUrl,
              pod.name,
              pod.namespace,
              pod.nodeName.orNull,
              pod.phase,
              pod.reason.orNull,
              pod.startTime.orNull,
              pod.podIp.orNull,
              pod.containers.map(driverContainer).asJava)
        }
    }
  }

  private def unavailableDriverInfo(
      sessionId: String,
      message: String,
      engineId: String,
      engineUrl: String): SparkConnectDriverInfo =
    new SparkConnectDriverInfo(
      sessionId,
      false,
      message,
      engineId,
      engineUrl,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      Collections.emptyList[SparkConnectDriverContainer]())

  private def driverContainer(container: KubernetesDriverContainer)
      : SparkConnectDriverContainer =
    new SparkConnectDriverContainer(
      container.name,
      container.state,
      container.stateReason.orNull,
      container.ready,
      container.restartCount,
      container.exitCode.map(Int.box).orNull,
      container.lastTerminationReason.orNull,
      container.lastTerminationExitCode.map(Int.box).orNull,
      container.requests.asJava,
      container.limits.asJava)

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[OperationLog]))),
    description = "Get the driver pod log of the caller's own Spark Connect session")
  @GET
  @Path("sessions/{sessionId}/driver/log")
  def getDriverLog(
      @PathParam("sessionId") sessionId: String,
      @QueryParam("lines") @DefaultValue("100") lines: Int): OperationLog = {
    val session = resolveOwnSession(sessionId)
    val logLines = kubernetesOperation match {
      case None => Seq(NOT_KUBERNETES_MESSAGE)
      case Some(operation) => operation.getDriverLogByTag(engineTag(session), lines)
    }
    new OperationLog(logLines.asJava, logLines.size)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[SparkConnectDriverEvents]))),
    description = "Get the driver pod's Kubernetes events for the caller's own session, newest " +
      "first")
  @GET
  @Path("sessions/{sessionId}/driver/events")
  def getDriverEvents(
      @PathParam("sessionId") sessionId: String,
      @QueryParam("size") @DefaultValue("100") size: Int): SparkConnectDriverEvents = {
    val session = resolveOwnSession(sessionId)
    kubernetesOperation match {
      case None =>
        unavailableDriverEvents(sessionId, NOT_KUBERNETES_MESSAGE)
      case Some(operation) =>
        operation.getDriverPodEventDetailsByTag(engineTag(session), size) match {
          case None =>
            unavailableDriverEvents(sessionId, NO_DRIVER_POD_MESSAGE)
          case Some(events) =>
            // A driver pod with no events is normal once it has settled, so this is available
            // with an empty list rather than unavailable.
            new SparkConnectDriverEvents(
              sessionId,
              true,
              if (events.isEmpty) NO_DRIVER_POD_EVENTS_MESSAGE else null,
              events.map(driverEvent).asJava)
        }
    }
  }

  private def unavailableDriverEvents(
      sessionId: String,
      message: String): SparkConnectDriverEvents =
    new SparkConnectDriverEvents(
      sessionId,
      false,
      message,
      Collections.emptyList[SparkConnectDriverEvent]())

  private def driverEvent(event: KubernetesDriverPodEvent): SparkConnectDriverEvent =
    new SparkConnectDriverEvent(
      event.eventType,
      event.reason,
      event.message,
      event.count,
      event.firstTimestamp.orNull,
      event.lastTimestamp.orNull)

  /** The Kubernetes integration, or [[None]] on a deployment that launches engines elsewhere. */
  private def kubernetesOperation: Option[KubernetesApplicationOperation] =
    sessionManager.applicationManager.getKubernetesApplicationOperation

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

  /** The engine has been asked for but has not reported in yet. */
  private[v1] val STATE_PENDING = "PENDING"

  /** The engine is up and the session is usable. */
  private[v1] val STATE_RUNNING = "RUNNING"

  private[v1] val STATE_CLOSED = "CLOSED"

  private[v1] val STATE_FAILED = "FAILED"

  /**
   * What the driver endpoints say instead of failing.
   *
   * A 500 or an empty record would both read as "something is broken here", which is exactly the
   * wrong signal while an engine is still coming up -- the state a user is most likely to be
   * looking at these endpoints in.
   */
  private[v1] val NOT_KUBERNETES_MESSAGE =
    "Driver diagnostics are only available when engines run on Kubernetes."

  private[v1] val NO_DRIVER_POD_MESSAGE =
    "No driver pod for this session yet. It may still be starting, or it may have been cleaned " +
      "up after the engine exited."

  private[v1] val NO_DRIVER_POD_EVENTS_MESSAGE =
    "The driver pod has recorded no events. Kubernetes expires events after a few hours."

  private[v1] val NO_SUBMIT_LOG_MESSAGE =
    "No submit log for this session. Engine operation logging may be disabled, or this Kyuubi " +
      "instance may not be the one that launched the engine."

  /**
   * Whether a session belongs to the Spark Connect frontend.
   *
   * Keyed off the conf this resource itself pins, so a session opened through any other frontend
   * -- Thrift, the ordinary REST session API -- is never listed here even though it lives in the
   * same session manager.
   */
  private[v1] def isSparkConnectSession(sessionConf: Map[String, String]): Boolean =
    sessionConf.get(SESSION_SPARK_CONNECT_ENABLED.key).contains("true")

  /**
   * The lifecycle stage of a session, from the timestamps its event carries.
   *
   * `PENDING` is the interesting one: an engine takes a minute or two to come up, and until it
   * does the gRPC port answers `UNAVAILABLE`. A user staring at a client that is quietly retrying
   * needs to be able to tell "still starting" from "broken".
   */
  private[v1] def sessionState(openedTime: Long, endTime: Long, failed: Boolean): String = {
    if (failed) STATE_FAILED
    else if (endTime > 0) STATE_CLOSED
    else if (openedTime > 0) STATE_RUNNING
    else STATE_PENDING
  }

  /**
   * The subdomain that keeps a Spark Connect engine to itself.
   *
   * At `USER` share level the engine space is keyed by user and subdomain, so without this a
   * Spark Connect session would be handed whatever ordinary Thrift or REST engine the same user
   * already had -- a driver launched without the Spark Connect plugin, which answers nothing on
   * the gRPC port. It also keeps the reverse from happening to an unsuspecting Thrift session.
   */
  private[v1] val ENGINE_SUBDOMAIN = "spark-connect"

  /**
   * Conf that only Kyuubi may set.
   *
   * The engine share level is `USER`, with a subdomain of its own. A Spark Connect session is
   * stateful -- artifacts, temporary views, cached frames -- so the engine cannot be shared
   * across users; within one user it can be, because there is at most one Spark Connect session
   * per user and the state that survives between two of their own sessions is their own. Sharing
   * it that far is what lets a user who closes a session and opens another get their engine back
   * in a second rather than waiting out another cold start.
   *
   * The token is Kyuubi's credential for the engine, minted when the engine is launched and
   * reused for as long as that driver lives. It is never returned to a client: callers
   * authenticate with their own platform credential, which terminates at the frontend.
   *
   * The deploy mode is pinned to `cluster` for Spark Connect alone -- other engine types keep
   * whatever the deployment configures. In client mode the driver JVM runs inside the Kyuubi
   * pod, where `SparkConnectPlugin` tries to bind [[FRONTEND_SPARK_CONNECT_ENGINE_PORT]] in the
   * same network namespace that the gateway's own Spark Connect frontend already listens on, and
   * fails outright because `spark.port.maxRetries` defaults to 0. Even on a free port a
   * client-mode engine would be unreachable: [[SparkConnectEngineLocator]] resolves an engine by
   * finding the driver pod that carries its `kyuubi-unique-tag`, and in client mode there is no
   * such pod.
   */
  private[v1] def serverControlledConf(engineToken: String): Map[String, String] = Map(
    SESSION_SPARK_CONNECT_ENABLED.key -> "true",
    SESSION_SPARK_CONNECT_TOKEN.key -> engineToken,
    ENGINE_TYPE.key -> "SPARK_SQL",
    ENGINE_SHARE_LEVEL.key -> "USER",
    ENGINE_SHARE_LEVEL_SUBDOMAIN.key -> ENGINE_SUBDOMAIN,
    SparkProcessBuilder.DEPLOY_MODE_KEY -> DEPLOY_MODE_CLUSTER)

  /** The only deploy mode a Spark Connect engine works in -- see [[serverControlledConf]]. */
  private[v1] val DEPLOY_MODE_CLUSTER = "cluster"

  private[v1] val SERVER_CONTROLLED_KEYS: Set[String] = Set(
    SESSION_SPARK_CONNECT_ENABLED.key,
    SESSION_SPARK_CONNECT_TOKEN.key,
    ENGINE_TYPE.key,
    ENGINE_SHARE_LEVEL.key,
    ENGINE_SHARE_LEVEL_SUBDOMAIN.key,
    SparkProcessBuilder.DEPLOY_MODE_KEY)

  /**
   * The caller's conf, minus anything only Kyuubi may set.
   *
   * Stripping rather than rejecting keeps a client that echoes a previous session's conf working,
   * and a self-declared engine token would be inert anyway -- the frontend presents the one from
   * the routing record, which only this endpoint writes -- but letting one through would still be
   * a needless surprise.
   */
  private[v1] def clientControlledConf(requestedConf: Map[String, String]): Map[String, String] =
    requestedConf -- SERVER_CONTROLLED_KEYS
}
