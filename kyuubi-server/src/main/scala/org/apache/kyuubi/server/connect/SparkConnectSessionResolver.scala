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

import java.util.concurrent.locks.Lock

import com.google.common.util.concurrent.Striped

import org.apache.kyuubi.Logging
import org.apache.kyuubi.server.metadata.api.SparkConnectSessionInfo

/**
 * Decides which session a create request is answered with, having verified that everything it
 * hands back is live.
 *
 * The binding is persisted; the Kyuubi session it names lives in one instance's memory, and the
 * engine lives in a pod. Any of the three can outlive the others, and a restart is the commonest
 * way for them to part: the binding survives it and the session does not. So nothing here is
 * inferred from the binding, or from Kyuubi's own session record. Before an existing session or
 * engine is reused, each link is checked:
 *
 *  - the binding exists and recovery has not given up on it;
 *  - it is not `RECOVERING` on the strength of a relaunch nothing is running any more (see
 *    [[SparkConnectSessionSupervisor.isStaleRecovery]]) -- a stale flag is cleared first;
 *  - the Kyuubi session it names is open on this instance;
 *  - the engine is live: the informer has its driver pod running, which is what
 *    [[SparkConnectEngineLocator]] answers, and its Spark Connect port answers a call made with the
 *    engine's credential within a bounded time, which is what [[SparkConnectEngineProbe]] answers.
 *
 * What is found decides what is done:
 *
 * {{{
 *   session held here, engine live        -> that session
 *   session held here, engine not live    -> SparkConnectSessionSupervisor.recoverIfDead: that
 *                                            session while its engine starts or is replaced
 *   session not held here, engine live    -> a session on that engine, the binding moved to it
 *   session not held here, relaunch or
 *     engine starting elsewhere           -> the bound session; nothing is launched
 *   session not held here, engine gone    -> a replacement engine now, within the recovery bounds
 *   no binding, closed, or abandoned      -> a new session
 * }}}
 *
 * A `POST` from a client that found its session broken is the clearest statement there is that
 * somebody wants the session, which is why this is also where lazy recovery happens.
 *
 * ==Several instances==
 *
 * A session this instance does not hold is not necessarily gone: with several instances behind
 * HA it may be open on a peer. That is why nothing is detached at startup, and why every decision
 * above is made lazily, on a create that names the user. When a create lands on an instance that
 * does not hold the bound session:
 *
 *  - If the engine is live, the create is answered with a session on that engine held here, and
 *    the binding moves to it. The engine is untouched -- same driver, same tag, same credential,
 *    same Spark session state -- so the gRPC port on every instance routes exactly as before. The
 *    peer's session is no longer bound; it is still listed on the peer, it closes on its idle
 *    timeout or when the user closes it, and closing it does not disturb the binding, because a
 *    detach only clears a binding that still names the session being closed. A session this
 *    instance already holds on that engine is reused rather than joined by another, so creates
 *    alternating between instances move the binding back and forth instead of opening a session
 *    per create.
 *  - If the engine is starting, or a relaunch is under way, the create is answered with the bound
 *    session and nothing is launched: the peer that holds it is bringing the engine up, and a
 *    second launch would put two drivers behind one user. The session list shows it from the
 *    binding. Once the engine serves, the next create here moves the binding as above.
 *  - Only when the engine is gone -- no driver pod, and past the point where a launch could still
 *    produce one -- is a replacement launched from here. The peer's session, if there is one, is
 *    left on an engine that no longer exists and closes the way any such session does.
 */
class SparkConnectSessionResolver(
    registry: SparkConnectSessionRegistry,
    supervisor: SparkConnectSessionSupervisor,
    engineLocator: SparkConnectEngineLocator,
    engineProbe: SparkConnectEngineProbe,
    provisionEngine: SparkConnectEngineRequest => String)
  extends Logging {

  import SparkConnectSessionResolver._
  import SparkConnectSessionSupervisor._

  /**
   * Serialises the creates of one user on this instance.
   *
   * A web page and a client retrying at once would otherwise both find the same dead engine and
   * launch two, or both reattach and leave one session unbound. Striped, so the set of locks is
   * bounded however many users there are; two users sharing a stripe only wait for each other's
   * probe and provisioning, both of which are bounded.
   */
  private val creationLocks: Striped[Lock] = Striped.lazyWeakLock(CREATION_LOCK_STRIPES)

  /**
   * The session to answer `userName`'s create with, opening or relaunching whatever that takes.
   *
   * @param requestedConf the conf on the request, honoured only where a new engine is launched
   *                      for it: an engine that is already running cannot take it.
   * @param heldSessionState the state Kyuubi's own record gives a session, by id, if this
   *                         instance holds it; [[None]] when it does not.
   */
  def openSession(
      userName: String,
      requestedConf: Map[String, String],
      heldSessionState: String => Option[String]): SparkConnectSessionResolution = {
    val lock = creationLocks.get(userName)
    lock.lock()
    try {
      resolve(userName, requestedConf, heldSessionState)
    } finally {
      lock.unlock()
    }
  }

  /**
   * The state of a bound session that this instance does not hold, for the session list.
   *
   * There is no Kyuubi record here to start from, so it is derived the same way a create would
   * decide on the session: the list must not show a session create would treat as gone as
   * `RUNNING`, or the reverse.
   */
  def unheldSessionState(binding: SparkConnectSessionInfo): String = {
    if (binding.isRecoveryAbandoned) STATE_FAILED
    else if (binding.isRecovering && !supervisor.isStaleRecovery(binding)) STATE_RECOVERING
    else if (engineIsLive(binding)) STATE_RUNNING
    else if (supervisor.engineIsStarting(binding)) STATE_PENDING
    else STATE_DEAD
  }

  private def resolve(
      userName: String,
      requestedConf: Map[String, String],
      heldSessionState: String => Option[String]): SparkConnectSessionResolution =
    registry.lookup(userName) match {
      case Some(binding) if binding.hasLiveSession && !binding.isRecoveryAbandoned =>
        val current = supervisor.clearIfStale(binding)
        heldSessionState(current.sessionId) match {
          case Some(recordState) => resolveHeld(current, recordState, requestedConf)
          case None => resolveUnheld(current, requestedConf, heldSessionState)
        }
      // No binding, one whose session was closed, or one recovery gave up on: a new session is
      // what the caller asked for and what they get.
      case _ => create(userName, requestedConf)
    }

  private def resolveHeld(
      binding: SparkConnectSessionInfo,
      recordState: String,
      requestedConf: Map[String, String]): SparkConnectSessionResolution = {
    if (engineIsLive(binding)) {
      return SparkConnectSessionResolution(binding.sessionId, SparkConnectCreateOutcome.Reused)
    }
    // The session is real and held here, so what remains is its engine, and the supervisor's
    // rules already say what to do about that: wait for one that is starting, replace one that
    // died -- paced and bounded -- and give up on one that has died too often.
    supervisor.recoverIfDead(binding.userName, recordState) match {
      case SparkConnectRecoveryOutcome.Healthy | SparkConnectRecoveryOutcome.Recovering =>
        SparkConnectSessionResolution(binding.sessionId, SparkConnectCreateOutcome.AwaitingEngine)
      case _ => create(binding.userName, requestedConf)
    }
  }

  private def resolveUnheld(
      binding: SparkConnectSessionInfo,
      requestedConf: Map[String, String],
      heldSessionState: String => Option[String]): SparkConnectSessionResolution = {
    val userName = binding.userName
    if (binding.isRecovering) {
      // Not stale -- that was cleared already -- so a relaunch is under way, here or on a peer,
      // and will rebind the user when it lands. Starting another would put two drivers behind
      // them.
      return SparkConnectSessionResolution(
        binding.sessionId,
        SparkConnectCreateOutcome.AwaitingEngine)
    }
    if (engineIsLive(binding)) {
      return reattach(binding, heldSessionState)
    }
    if (supervisor.engineIsStarting(binding)) {
      return SparkConnectSessionResolution(
        binding.sessionId,
        SparkConnectCreateOutcome.AwaitingEngine)
    }
    supervisor.relaunchNow(userName) match {
      case SparkConnectRecoveryOutcome.Relaunched(sessionId) =>
        SparkConnectSessionResolution(sessionId, SparkConnectCreateOutcome.Relaunched)
      case SparkConnectRecoveryOutcome.Recovering =>
        // Another thread of this instance got there first; its relaunch rebinds the user.
        SparkConnectSessionResolution(
          registry.lookup(userName).map(_.sessionId).getOrElse(binding.sessionId),
          SparkConnectCreateOutcome.AwaitingEngine)
      // Recovery is switched off or its attempts are spent. Either way the user asked for a
      // session and a new one is what they get, as they would on a session recovery gave up on.
      case _ => create(userName, requestedConf)
    }
  }

  /**
   * Answer with a session on the engine the binding names, held by this instance, and move the
   * binding to it.
   *
   * The session is opened with the credential and conf the engine was launched with, so engine
   * discovery -- which shares the engine at `USER` level -- hands it the running driver, and the
   * binding keeps that driver's tag and credential: the relay goes on routing to the same pod
   * with the same token, and the client's Spark session on it is untouched.
   */
  private def reattach(
      binding: SparkConnectSessionInfo,
      heldSessionState: String => Option[String]): SparkConnectSessionResolution = {
    val userName = binding.userName
    val heldOnEngine = registry.localSessionIds(userName, binding.engineTag)
      .find(sessionId => heldSessionState(sessionId).isDefined)
    val sessionId = heldOnEngine.getOrElse(provisionEngine(SparkConnectEngineRequest(
      userName = userName,
      engineToken = binding.engineToken,
      requestedConf = binding.engineConf)))
    registry.reattach(userName, binding.engineTag, sessionId) match {
      case Some(_) =>
        info(s"Reattached $userName to the live Spark Connect engine ${binding.engineTag} with" +
          s" session $sessionId: the session it was bound to, ${binding.sessionId}, is not held" +
          " by this Kyuubi instance")
      case None =>
        warn(s"The Spark Connect engine of $userName was replaced while session $sessionId was" +
          s" being attached to ${binding.engineTag}; the session is open but not bound")
    }
    SparkConnectSessionResolution(sessionId, SparkConnectCreateOutcome.Reattached)
  }

  private def create(
      userName: String,
      requestedConf: Map[String, String]): SparkConnectSessionResolution = {
    // The engine is shared at USER level, so one left running by a previous session of this
    // user's is handed straight back by engine discovery instead of being relaunched. It keeps
    // the `kyuubi-unique-tag` and the credential it was launched with, both of which the new
    // session has to inherit: the tag is what the frontend routes on, and the token in the
    // driver's environment cannot be changed from out here. Reused only once it has answered a
    // call made with that token -- a pod the informer calls running is not yet an engine.
    val reusableEngine = registry.lookup(userName).filter(engineIsLive)
    val engineToken = reusableEngine.map(_.engineToken).getOrElse(SparkConnect.generateToken())

    // The one provisioning path, shared with recovery: an engine relaunched under a dead session
    // has to come up the way the original did, and a second copy of this call is how the two
    // would drift apart.
    val sessionId = provisionEngine(SparkConnectEngineRequest(
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
    SparkConnectSessionResolution(sessionId, SparkConnectCreateOutcome.Created)
  }

  /**
   * Whether the binding's engine can serve the relay right now: its driver pod is running, and
   * its port answers a call made with the credential the relay would present.
   */
  private def engineIsLive(binding: SparkConnectSessionInfo): Boolean =
    engineLocator.locate(binding.engineTag)
      .exists(address => engineProbe.isServing(address, binding.engineToken))
}

object SparkConnectSessionResolver {

  /** Enough stripes that two users rarely share one, few enough to be a fixed, small cost. */
  private val CREATION_LOCK_STRIPES = 1024
}

/** What a create request was answered with, and how it came to be that session. */
case class SparkConnectSessionResolution(sessionId: String, outcome: SparkConnectCreateOutcome)

/** How [[SparkConnectSessionResolver]] arrived at the session it answered a create with. */
sealed trait SparkConnectCreateOutcome

object SparkConnectCreateOutcome {

  /** The bound session, held here, on an engine that answered the probe. */
  case object Reused extends SparkConnectCreateOutcome

  /**
   * The bound session, on an engine that is not serving and that nothing new was launched for:
   * it is starting, or being replaced, or -- for a session held here whose launch failed -- the
   * session's own state reports why.
   */
  case object AwaitingEngine extends SparkConnectCreateOutcome

  /** A session held here on the bound engine, which was live; the binding moved to it. */
  case object Reattached extends SparkConnectCreateOutcome

  /** A replacement engine launched for a bound session nothing held; counted as a recovery. */
  case object Relaunched extends SparkConnectCreateOutcome

  /** A new session, on a new engine unless a live one was left from the user's last session. */
  case object Created extends SparkConnectCreateOutcome
}
