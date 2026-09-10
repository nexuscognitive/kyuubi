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
import org.apache.kyuubi.server.connect.{SparkConnectEngineConf, SparkConnectSessionSupervisor}
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

    // One session per user. The gRPC port routes on the caller's identity, so a second session
    // would be unreachable -- and the conf on this request cannot be applied to an engine that is
    // already running anyway, which is why it is dropped rather than quietly half-honoured
    // whenever an existing session or engine is what the caller gets. Which one that is, and
    // whether it is live, is the resolver's to establish: the persisted binding survives a
    // restart that the session it names does not.
    val resolution = sessionManager.sparkConnectSessionResolver
      .openSession(userName, requestedConf, heldSessionState)
    info(s"Answered the Spark Connect create of $userName with session" +
      s" ${resolution.sessionId} (${resolution.outcome})")
    new SparkConnectSession(resolution.sessionId, connectUrl)
  }

  /**
   * What Kyuubi's own record says of the session `sessionId`, if this instance holds it.
   *
   * [[None]] for an id this instance has no session for -- which after a restart is every id the
   * store names, and with several instances is every id a peer holds.
   */
  private def heldSessionState(sessionId: String): Option[String] =
    parseSessionHandle(sessionId)
      .flatMap(sessionManager.getSessionOption)
      .collect { case session: KyuubiSession => recordStateOf(session) }

  private def recordStateOf(session: KyuubiSession): String = {
    val event = session.getSessionEvent
    sessionState(
      openedTime = event.map(_.openedTime).getOrElse(-1L),
      endTime = event.map(_.endTime).getOrElse(-1L),
      failed = event.exists(_.exception.isDefined))
  }

  private def parseSessionHandle(sessionId: String): Option[SessionHandle] =
    try {
      Some(SessionHandle.fromUUID(sessionId))
    } catch {
      case _: IllegalArgumentException => None
    }

  private def requireSessionHandle(sessionId: String): SessionHandle =
    parseSessionHandle(sessionId).getOrElse {
      throw new WebApplicationException("invalid sessionId", 400)
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
    val held = sessionManager.allSessions()
      .collect { case session: KyuubiSession if isSparkConnectSession(session.conf) => session }
      .filter(_.user == userName)
      .map(sessionData)
      .toSeq
    // The session a create answers with is always the bound one, and after a restart -- or when a
    // peer holds it -- it is not in this instance's memory. Listing memory alone is how a page
    // came to show its create form again straight after a create had succeeded.
    val bound = sessionManager.sparkConnectSessionRegistry.lookup(userName)
      .filter(_.hasLiveSession)
    val unheld = bound
      .filterNot(binding => held.exists(_.getSessionId == binding.sessionId))
      .map(unheldSessionData)
    val boundSessionId = bound.map(_.sessionId)
    // The bound session first: it is the one the gRPC port routes to, and the one a client that
    // shows a single session has to show. The rest -- a session left on an engine a relaunch
    // replaced, say -- follow, newest first.
    (held ++ unheld).sortBy(session =>
      (!boundSessionId.contains(session.getSessionId), -session.getCreateTime.longValue()))
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
    resolveOwnSession(sessionId).fold(unheldSessionData, sessionData)

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
    toSessionData(
      session.handle.identifier.toString,
      session.user,
      session.createTime,
      status.state,
      event.map(_.engineId).getOrElse(""),
      event.map(_.engineUrl).getOrElse(""),
      status.binding)
  }

  /**
   * A bound session this instance does not hold, the way the list and the session view show it.
   *
   * There is no Kyuubi record here for it, so the engine id and URL are unknown, and the state is
   * derived from the binding and the driver alone -- the same way a create would decide on it.
   */
  private def unheldSessionData(
      binding: metadata.api.SparkConnectSessionInfo): SparkConnectSessionData =
    toSessionData(
      binding.sessionId,
      binding.userName,
      binding.createTime,
      sessionManager.sparkConnectSessionResolver.unheldSessionState(binding),
      "",
      "",
      Some(binding))

  private def toSessionData(
      sessionId: String,
      userName: String,
      createTime: Long,
      state: String,
      engineId: String,
      engineUrl: String,
      binding: Option[metadata.api.SparkConnectSessionInfo]): SparkConnectSessionData = {
    new SparkConnectSessionData(
      sessionId,
      userName,
      Long.box(createTime),
      state,
      engineId,
      engineUrl,
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
    val sessionHandle = requireSessionHandle(sessionId)
    sessionManager.getSessionOption(sessionHandle) match {
      case Some(session) =>
        val userName = fe.getSessionUser(Map.empty[String, String])
        if (!fe.isAdministrator(userName) && session.user != userName) {
          throw new ForbiddenException(s"$userName is not allowed to close session $sessionId")
        }
        // Drops the routing record and the upstream connection as part of the close path.
        sessionManager.closeSession(sessionHandle)
      case None =>
        // Not held here, but it may be the caller's bound session -- left behind by a restart,
        // or held by a peer -- which the session list shows them, so it has to be closable from
        // there. Closing it detaches the binding: the gRPC port stops routing to it and the
        // caller's next create starts afresh. A peer's Kyuubi session on it, if there is one,
        // closes on its idle timeout. Only the caller's own binding is reachable this way.
        val userName = fe.getSessionUser(Map.empty[String, String])
        val registry = sessionManager.sparkConnectSessionRegistry
        registry.lookup(userName).filter(_.sessionId == sessionId).getOrElse {
          throw new WebApplicationException("session not found", 404)
        }
        registry.detach(userName, sessionId)
    }
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
  private def resolveOwnSession(sessionId: String): OwnSparkConnectSession = {
    val sessionHandle = requireSessionHandle(sessionId)
    val userName = fe.getSessionUser(Map.empty[String, String])
    val session = sessionManager.getSessionOption(sessionHandle).getOrElse {
      // Not held here, but it may be the caller's bound session -- after a restart, or on a peer
      // -- which is the session the list shows them. Someone else's is as unknown as a made-up
      // id, rather than forbidden: an id this instance does not hold says nothing about whose it
      // is.
      return sessionManager.sparkConnectSessionRegistry.lookup(userName)
        .filter(_.sessionId == sessionId)
        .map(Left(_))
        .getOrElse(throw new WebApplicationException("session not found", 404))
    }
    if (session.user != userName) {
      throw new ForbiddenException(s"$userName is not allowed to access session $sessionId")
    }
    session match {
      case kyuubiSession: KyuubiSessionImpl if isSparkConnectSession(kyuubiSession.conf) =>
        Right(kyuubiSession)
      case _ =>
        // Reachable through this path only for a session opened on another frontend, which has
        // its own endpoints; answering 404 keeps this resource about Spark Connect sessions.
        throw new WebApplicationException("not a Spark Connect session", 404)
    }
  }

  private def engineTagOf(own: OwnSparkConnectSession): String = own.fold(_.engineTag, engineTag)

  private def userNameOf(own: OwnSparkConnectSession): String = own.fold(_.userName, _.user)

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
    // The launch operation's log is the `spark-submit` output Kyuubi captures in its work
    // directory -- the only place a launch that never produced a driver pod says anything.
    val launchEngineOp =
      resolveOwnSession(sessionId).toOption.flatMap(session => Option(session.launchEngineOp))
    val logRowSet = launchEngineOp.flatMap(_.getOperationLog) match {
      case Some(operationLog) =>
        val columns = operationLog.read(from, size).getColumns
        if (columns == null || columns.isEmpty) {
          Collections.emptyList[String]()
        } else {
          columns.get(0).getStringVal.getValues
        }
      case None =>
        // Operation logging can be switched off deployment-wide, and a session this instance
        // does not hold never had a local log to begin with. Neither is an error.
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
    val own = resolveOwnSession(sessionId)
    val event = own.toOption.flatMap(_.getSessionEvent)
    val engineId = event.map(_.engineId).getOrElse("")
    val engineUrl = event.map(_.engineUrl).getOrElse("")
    kubernetesOperation match {
      case None =>
        unavailableDriverInfo(sessionId, NO_KUBERNETES_CLIENT_MESSAGE, engineId, engineUrl)
      case Some(operation) =>
        operation.getDriverPodDetailByTag(engineTagOf(own)) match {
          case None =>
            // A dead session's driver pod has usually been reclaimed by the time anyone comes
            // looking, so answer from the post-mortem taken while it still existed rather than
            // with "not found", which is what sent the operator to the cluster in the first place.
            val message = storedPostMortem(userNameOf(own))
              .map(deadDriverMessage)
              .getOrElse(NO_DRIVER_POD_MESSAGE)
            unavailableDriverInfo(sessionId, message, engineId, engineUrl)
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
    val own = resolveOwnSession(sessionId)
    val logLines = kubernetesOperation match {
      case None => Seq(NO_KUBERNETES_CLIENT_MESSAGE)
      case Some(operation) => operation.getDriverLogByTag(engineTagOf(own), lines)
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
      userName: String): Option[metadata.api.SparkConnectDriverPostMortem] =
    sessionManager.sparkConnectSessionRegistry.lookup(userName).flatMap(_.latestPostMortem)

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
    val own = resolveOwnSession(sessionId)
    kubernetesOperation match {
      case None =>
        unavailableDriverEvents(sessionId, NO_KUBERNETES_CLIENT_MESSAGE)
      case Some(operation) =>
        operation.getDriverPodEventDetailsByTag(engineTagOf(own), size) match {
          case None =>
            // The pod is gone, and Kubernetes collected its events with it. What is left is what
            // Kyuubi copied out while the pod was dying, which is the whole reason it did so.
            storedPostMortem(userNameOf(own)) match {
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
   * The caller's own Spark Connect session: [[Right]] for one this instance holds, [[Left]] for
   * the binding of one it does not -- left by a restart, or held by a peer.
   */
  private[v1] type OwnSparkConnectSession =
    Either[metadata.api.SparkConnectSessionInfo, KyuubiSessionImpl]

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
