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
import org.apache.kyuubi.client.api.v1.dto.{OperationLog, SessionOpenRequest, SparkConnectDriverContainer, SparkConnectDriverContainerExit, SparkConnectDriverEvent, SparkConnectDriverEvents, SparkConnectDriverInfo, SparkConnectDriverPostMortem, SparkConnectSession}
import org.apache.kyuubi.client.api.v1.dto.SparkConnectSessionData
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.{KubernetesApplicationOperation, KubernetesDriverContainer, KubernetesDriverPodEvent}
import org.apache.kyuubi.server.api.ApiRequestContext
import org.apache.kyuubi.server.connect.{SparkConnect, SparkConnectEngineConf, SparkConnectEngineRequest, SparkConnectRecoveryOutcome, SparkConnectSessionSupervisor}
import org.apache.kyuubi.server.metadata
import org.apache.kyuubi.session.{KyuubiSession, KyuubiSessionImpl, KyuubiSessionManager, SessionHandle}

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
      case Some(existing) if isUsable(userName, existing.sessionId) =>
        info(s"Returning the existing Spark Connect session ${existing.sessionId} for $userName")
        new SparkConnectSession(existing.sessionId, connectUrl)
      case _ => createSession(userName, requestedConf)
    }
  }

  /**
   * Whether the caller's existing session is one they can use, rather than one whose driver has
   * died under it.
   *
   * A `POST` from a client that found its session broken is the clearest possible statement that
   * somebody wants this session, so it is also where lazy recovery is triggered. The caller is
   * handed the session they already have while the replacement engine comes up, and their Spark
   * Connect calls are answered `UNAVAILABLE` until it is serving -- the same experience as a cold
   * start, which is what a relaunch is.
   */
  private def isUsable(userName: String, sessionId: String): Boolean =
    sessionManager.sparkConnectSessionSupervisor
      .recoverIfDead(userName, recordState(sessionId)) match {
      case SparkConnectRecoveryOutcome.Healthy => true
      case SparkConnectRecoveryOutcome.Recovering => true
      // Abandoned, or no binding at all: a new session is what the caller needs and asked for.
      case _ => false
    }

  /** What Kyuubi's own session record says, before it is reconciled against the driver. */
  private def recordState(sessionId: String): String =
    sessionManager.getSessionOption(SessionHandle.fromUUID(sessionId))
      .collect { case session: KyuubiSession => recordStateOf(session) }
      .getOrElse(SparkConnectSessionSupervisor.STATE_CLOSED)

  private def recordStateOf(session: KyuubiSession): String = {
    val event = session.getSessionEvent
    sessionState(
      openedTime = event.map(_.openedTime).getOrElse(-1L),
      endTime = event.map(_.endTime).getOrElse(-1L),
      failed = event.exists(_.exception.isDefined))
  }

  private def createSession(
      userName: String,
      requestedConf: Map[String, String]): SparkConnectSession = {
    val registry = sessionManager.sparkConnectSessionRegistry

    // The engine is shared at USER level, so one left running by a previous session of this
    // user's is handed straight back by engine discovery instead of being relaunched. It keeps
    // the `kyuubi-unique-tag` and the credential it was launched with, both of which the new
    // session has to inherit: the tag is what the frontend routes on, and the token in the
    // driver's environment cannot be changed from out here.
    val reusableEngine = registry.lookup(userName)
      .filter(engine => sessionManager.sparkConnectEngineLocator.locate(engine.engineTag).nonEmpty)
    val engineToken = reusableEngine.map(_.engineToken).getOrElse(SparkConnect.generateToken())

    // The one provisioning path, shared with recovery: an engine relaunched under a dead session
    // has to come up the way the original did, and a second copy of this call is how the two
    // would drift apart.
    val sessionId = sessionManager.openSparkConnectEngineSession(SparkConnectEngineRequest(
      userName = userName,
      engineToken = engineToken,
      requestedConf = requestedConf))

    // A newly launched engine carries this session's id as its `kyuubi-unique-tag` pod label,
    // because that is the engine reference id Kyuubi tags the driver with.
    val engineTag = reusableEngine.map(_.engineTag).getOrElse(sessionId)
    registry.register(
      userName = userName,
      sessionId = sessionId,
      engineTag = engineTag,
      engineToken = engineToken,
      // Kept so that an engine relaunched by recovery comes up shaped the way this caller asked
      // for, rather than on whatever the deployment defaults to.
      engineConf = SparkConnectEngineConf.clientControlledConf(requestedConf))
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

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[SparkConnectSessionData]))),
    description = "Get the caller's own Spark Connect session, with driver-derived state")
  @GET
  @Path("sessions/{sessionId}")
  def getSession(@PathParam("sessionId") sessionId: String): SparkConnectSessionData =
    sessionData(resolveOwnSession(sessionId))

  /**
   * One session, with its state reconciled against the driver that is supposed to be serving it.
   *
   * The session record on its own says `RUNNING` from the moment the engine reported in and goes
   * on saying it after the driver pod has been OOM-killed, evicted or deleted -- which is the
   * defect this reconciliation exists to fix. The record still decides the two things a pod
   * cannot contradict, a session the user closed and a launch that threw.
   */
  private def sessionData(session: KyuubiSession): SparkConnectSessionData = {
    val event = session.getSessionEvent
    val status =
      sessionManager.sparkConnectSessionSupervisor.sessionStatus(
        session.user,
        recordStateOf(session))
    val binding = status.binding
    new SparkConnectSessionData(
      session.handle.identifier.toString,
      session.user,
      session.createTime,
      status.state,
      event.map(_.engineId).getOrElse(""),
      event.map(_.engineUrl).getOrElse(""),
      connectUrl,
      binding.map(_.generation).getOrElse(0),
      binding.map(_.restartCount).getOrElse(0),
      Long.box(binding.map(_.lastRestartTime).getOrElse(0L)),
      binding.flatMap(_.recoveryMessage).orNull,
      // Only a session that has actually been through a restart carries the warning; saying it
      // on every session would train an operator to ignore it on the one where it is true.
      if (binding.exists(_.wasRestarted)) STATE_LOSS_MESSAGE else null,
      binding.map(_.driverPostMortems).getOrElse(Nil).map(driverPostMortem).asJava)
  }

  private def driverPostMortem(
      postMortem: metadata.api.SparkConnectDriverPostMortem): SparkConnectDriverPostMortem =
    new SparkConnectDriverPostMortem(
      postMortem.capturedTime,
      postMortem.driverName,
      postMortem.location,
      postMortem.finalState,
      postMortem.applicationState,
      postMortem.summary,
      postMortem.oomKilled,
      postMortem.reason.orNull,
      postMortem.message.orNull,
      postMortem.containers.map(container =>
        new SparkConnectDriverContainerExit(
          container.name,
          container.reason.orNull,
          container.message.orNull,
          container.exitCode.map(Int.box).orNull,
          container.signal.map(Int.box).orNull,
          container.oomKilled,
          container.restartCount,
          container.finishedAt.orNull)).asJava,
      postMortem.events.map(event =>
        new SparkConnectDriverEvent(
          event.eventType,
          event.reason,
          event.message,
          event.count,
          event.firstTimestamp.orNull,
          event.lastTimestamp.orNull)).asJava)

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
        unavailableDriverInfo(sessionId, NO_KUBERNETES_CLIENT_MESSAGE, engineId, engineUrl)
      case Some(operation) =>
        operation.getDriverPodDetailByTag(engineTag(session)) match {
          case None =>
            // A dead session's driver pod has usually been reclaimed by the time anyone comes
            // looking, so answer from the post-mortem taken while it still existed rather than
            // with "not found", which is what sent the operator to the cluster in the first place.
            unavailableDriverInfo(
              sessionId,
              storedPostMortem(session).map(deadDriverMessage).getOrElse(NO_DRIVER_POD_MESSAGE),
              engineId,
              engineUrl)
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
      case None => Seq(NO_KUBERNETES_CLIENT_MESSAGE)
      case Some(operation) => operation.getDriverLogByTag(engineTag(session), lines)
    }
    new OperationLog(logLines.asJava, logLines.size)
  }

  /**
   * What Kyuubi captured when this session's driver died, if it ever did.
   *
   * Read from the session binding rather than from the cluster, which is the point: the pod and
   * its events are long gone by the time most people look.
   */
  private def storedPostMortem(
      session: KyuubiSessionImpl): Option[metadata.api.SparkConnectDriverPostMortem] =
    sessionManager.sparkConnectSessionRegistry.lookup(session.user).flatMap(_.latestPostMortem)

  private def deadDriverMessage(
      postMortem: metadata.api.SparkConnectDriverPostMortem): String =
    s"The driver pod ${postMortem.driverName} is gone. Kyuubi recorded why it died while the pod" +
      s" still existed: ${postMortem.summary}. Its Kubernetes events, as of that moment, are on" +
      " the driver events endpoint."

  private def postMortemEventsMessage(
      postMortem: metadata.api.SparkConnectDriverPostMortem): String =
    s"The driver pod ${postMortem.driverName} is gone and Kubernetes has collected its events." +
      " These are the ones Kyuubi copied out as it died, newest first."

  private def storedDriverEvent(
      event: metadata.api.SparkConnectDriverEventRecord): SparkConnectDriverEvent =
    new SparkConnectDriverEvent(
      event.eventType,
      event.reason,
      event.message,
      event.count,
      event.firstTimestamp.orNull,
      event.lastTimestamp.orNull)

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
        unavailableDriverEvents(sessionId, NO_KUBERNETES_CLIENT_MESSAGE)
      case Some(operation) =>
        operation.getDriverPodEventDetailsByTag(engineTag(session), size) match {
          case None =>
            // The pod is gone, and Kubernetes collected its events with it. What is left is what
            // Kyuubi copied out while the pod was dying, which is the whole reason it did so.
            storedPostMortem(session) match {
              case Some(postMortem) =>
                new SparkConnectDriverEvents(
                  sessionId,
                  true,
                  postMortemEventsMessage(postMortem),
                  postMortem.events.take(math.max(size, 0)).map(storedDriverEvent).asJava)
              case None => unavailableDriverEvents(sessionId, NO_DRIVER_POD_MESSAGE)
            }
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
      .filter(_.hasKubernetesClient)

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

  import SparkConnectSessionSupervisor.{STATE_CLOSED, STATE_FAILED, STATE_PENDING, STATE_RUNNING}

  /**
   * What the driver endpoints say instead of failing.
   *
   * A 500 or an empty record would both read as "something is broken here", which is exactly the
   * wrong signal while an engine is still coming up -- the state a user is most likely to be
   * looking at these endpoints in.
   */
  private[v1] val NO_KUBERNETES_CLIENT_MESSAGE =
    "Driver diagnostics are unavailable: this Kyuubi instance has no Kubernetes client, so there " +
      "is no driver pod for it to inspect."

  private[v1] val NO_DRIVER_POD_MESSAGE =
    "No driver pod for this session yet. It may still be starting, or it may have been cleaned " +
      "up after the engine exited."

  private[v1] val NO_DRIVER_POD_EVENTS_MESSAGE =
    "The driver pod has recorded no events. Kubernetes expires events after a few hours."

  private[v1] val NO_SUBMIT_LOG_MESSAGE =
    "No submit log for this session. Engine operation logging may be disabled, or this Kyuubi " +
      "instance may not be the one that launched the engine."

  /**
   * What a session that has been through a restart has to say for itself.
   *
   * Shown on the session rather than only at the moment of the restart, because the client that
   * needs to hear it is the one that reconnected afterwards and is about to discover its
   * temporary views missing.
   */
  private[v1] val STATE_LOSS_MESSAGE =
    "The Spark driver for this session was replaced after it died. The replacement is a new " +
      "Spark session: temporary views, cached DataFrames, registered artifacts and any " +
      "session-level Spark conf set over the connection are gone. A client still holding the " +
      "previous Spark Connect session id will be answered INVALID_HANDLE.SESSION_NOT_FOUND and " +
      "should build a new SparkSession."

  /** @see [[org.apache.kyuubi.server.connect.SparkConnectEngineConf.isSparkConnectSession]] */
  private[v1] def isSparkConnectSession(sessionConf: Map[String, String]): Boolean =
    SparkConnectEngineConf.isSparkConnectSession(sessionConf)

  /**
   * The lifecycle stage of a session as Kyuubi's own record has it, from the timestamps its event
   * carries.
   *
   * This is only half the answer, and the half that knows nothing about the driver: the record
   * goes on saying `RUNNING` after the driver pod has been OOM-killed or deleted, because nothing
   * about that death travels back to it. [[SparkConnectSessionSupervisor.reconcileState]]
   * reconciles it against the pod before anyone is shown it.
   */
  private[v1] def sessionState(openedTime: Long, endTime: Long, failed: Boolean): String = {
    if (failed) STATE_FAILED
    else if (endTime > 0) STATE_CLOSED
    else if (openedTime > 0) STATE_RUNNING
    else STATE_PENDING
  }
}
