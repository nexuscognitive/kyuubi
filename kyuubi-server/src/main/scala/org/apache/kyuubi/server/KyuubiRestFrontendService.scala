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

package org.apache.kyuubi.server

import java.util.EnumSet
import java.util.concurrent.{Future, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.locks.ReentrantLock
import javax.servlet.DispatcherType
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import javax.ws.rs.WebApplicationException
import javax.ws.rs.core.Response.Status

import com.google.common.annotations.VisibleForTesting
import com.google.common.io.ByteStreams
import org.apache.hadoop.conf.Configuration
import org.eclipse.jetty.servlet.{ErrorPageErrorHandler, FilterHolder, ServletHolder}

import org.apache.kyuubi.{KyuubiException, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_NAMESPACE
import org.apache.kyuubi.ha.client.DiscoveryClientProvider.withDiscoveryClient
import org.apache.kyuubi.ha.client.DiscoveryPaths
import org.apache.kyuubi.ha.client.ServiceDiscovery
import org.apache.kyuubi.metrics.{MetricsConstants, MetricsSystem}
import org.apache.kyuubi.metrics.MetricsConstants.OPERATION_BATCH_PENDING_MAX_ELAPSE
import org.apache.kyuubi.operation.OperationState
import org.apache.kyuubi.server.api.v1.{ApiRootResource, DataAgentResource}
import org.apache.kyuubi.server.http.authentication.{AuthenticationFilter, KyuubiHttpAuthenticationFactory}
import org.apache.kyuubi.server.ui.{JettyServer, JettyUtils}
import org.apache.kyuubi.service.{AbstractFrontendService, Serverable, Service, ServiceUtils}
import org.apache.kyuubi.service.authentication.{AuthTypes, AuthUtils}
import org.apache.kyuubi.session.{KyuubiBatchSession, KyuubiSessionManager, SessionHandle}
import org.apache.kyuubi.util.{JavaUtils, ThreadUtils}
import org.apache.kyuubi.util.ThreadUtils.scheduleTolerableRunnableWithFixedDelay

/**
 * A frontend service based on RESTful api via HTTP protocol.
 * Note: Currently, it only be used in the Kyuubi Server side.
 */
class KyuubiRestFrontendService(override val serverable: Serverable)
  extends AbstractFrontendService("KyuubiRestFrontendService") {

  private var server: JettyServer = _

  private val isStarted = new AtomicBoolean(false)

  private def hadoopConf: Configuration = KyuubiServer.getHadoopConf()

  private[kyuubi] def sessionManager = be.sessionManager.asInstanceOf[KyuubiSessionManager]

  private val batchChecker = ThreadUtils.newDaemonSingleThreadScheduledExecutor("batch-checker")

  private[kyuubi] lazy val batchService: Option[KyuubiBatchService] =
    if (conf.get(BATCH_SUBMITTER_ENABLED)) {
      Some(new KyuubiBatchService(this, sessionManager))
    } else {
      None
    }

  // Reconciles batch ownership against live discovery membership so a dead/removed instance's
  // batches are taken over automatically. Needs both a metadata store (batches) and discovery
  // (liveness), i.e. the same prerequisites as batch HA.
  private[kyuubi] lazy val batchTakeoverService: Option[BatchTakeoverService] =
    if (conf.get(BATCH_SUBMITTER_ENABLED) && ServiceDiscovery.supportServiceDiscovery(conf)) {
      Some(new BatchTakeoverService(this, sessionManager))
    } else {
      None
    }

  lazy val host: String = conf.get(FRONTEND_REST_BIND_HOST)
    .getOrElse {
      if (JavaUtils.isWindows || JavaUtils.isMac) {
        warn(s"Kyuubi Server run in Windows or Mac environment, binding $getName to 0.0.0.0")
        "0.0.0.0"
      } else if (conf.get(KyuubiConf.FRONTEND_CONNECTION_URL_USE_HOSTNAME)) {
        JavaUtils.findLocalInetAddress.getCanonicalHostName
      } else {
        JavaUtils.findLocalInetAddress.getHostAddress
      }
    }

  private lazy val port: Int = conf.get(FRONTEND_REST_BIND_PORT)

  private[kyuubi] lazy val securityEnabled = {
    val authTypes = conf.get(AUTHENTICATION_METHOD).map(AuthTypes.withName)
    AuthUtils.kerberosEnabled(authTypes) ||
    !AuthUtils.effectivePlainAuthType(authTypes).contains(AuthTypes.NONE)
  }

  private lazy val administrators: Set[String] =
    conf.get(KyuubiConf.SERVER_ADMINISTRATORS) + Utils.currentUser

  def isAdministrator(userName: String): Boolean =
    if (securityEnabled) administrators.contains("*") || administrators.contains(userName)
    else true

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    this.conf = conf
    server = JettyServer(
      getName,
      host,
      port,
      conf.get(FRONTEND_REST_MAX_WORKER_THREADS),
      conf.get(FRONTEND_REST_JETTY_STOP_TIMEOUT),
      conf.get(FRONTEND_JETTY_SEND_VERSION_ENABLED))
    batchService.foreach(addService)
    batchTakeoverService.foreach(addService)
    super.initialize(conf)
  }

  override def connectionUrl: String = {
    checkInitialized()
    conf.get(FRONTEND_ADVERTISED_HOST) match {
      case Some(advertisedHost) => s"$advertisedHost:$port"
      case None => server.getServerUri
    }
  }

  private def startInternal(): Unit = {
    val contextHandler = ApiRootResource.getServletHandler(this)
    val holder = new FilterHolder(new AuthenticationFilter(conf))
    contextHandler.addFilter(holder, "/v1/*", EnumSet.allOf(classOf[DispatcherType]))
    val authenticationFactory = new KyuubiHttpAuthenticationFactory(conf)
    server.addHandler(authenticationFactory.httpHandlerWrapperFactory.wrapHandler(
      contextHandler,
      Some(MetricsConstants.JETTY_API_V1)))

    val proxyHandler = ApiRootResource.getEngineUIProxyHandler(this)
    server.addHandler(authenticationFactory.httpHandlerWrapperFactory.wrapHandler(proxyHandler))
    if (conf.get(FRONTEND_REST_UI_ENABLED)) {
      installWebUI()
    }
  }

  private def installWebUI(): Unit = {
    // redirect root path to Web UI home page
    server.addRedirectHandler("/", "/ui")

    val servletHandler = JettyUtils.createStaticHandler("dist", "/ui")

    // HTML5 history mode (vue-router): a deep link or refresh on /ui/<route> reaches the server
    // for a path that is not a real file, so DefaultServlet returns 404. Serve index.html with
    // HTTP 200 for those so the SPA can resolve the route client-side.
    // See https://router.vuejs.org/guide/essentials/history-mode.html#html5-mode
    // Note: mapping the 404 page to "/" relies on welcome-file resolution during an ERROR
    // dispatch (unreliable, and keeps the 404 status); a dedicated servlet is deterministic.
    val indexHtml: Array[Byte] = {
      val url = Thread.currentThread().getContextClassLoader.getResource("dist/index.html")
      if (url == null) throw new KyuubiException("Could not find dist/index.html for Web UI")
      val in = url.openStream()
      try ByteStreams.toByteArray(in)
      finally in.close()
    }
    val spaFallbackServlet = new HttpServlet {
      override def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
        resp.setStatus(HttpServletResponse.SC_OK)
        resp.setContentType("text/html; charset=utf-8")
        resp.getOutputStream.write(indexHtml)
      }
    }
    servletHandler.addServlet(new ServletHolder(spaFallbackServlet), "/spa-fallback")

    val errorHandler = new ErrorPageErrorHandler
    errorHandler.addErrorPage(HttpServletResponse.SC_NOT_FOUND, "/spa-fallback")
    servletHandler.setErrorHandler(errorHandler)
    server.addHandler(servletHandler)
  }

  private def startBatchChecker(): Unit = {
    val interval = conf.get(KyuubiConf.BATCH_CHECK_INTERVAL)
    val task = new Runnable {
      override def run(): Unit = {
        try {
          sessionManager.getPeerInstanceClosedBatchSessions(connectionUrl).foreach { batch =>
            Utils.tryLogNonFatalError {
              val sessionHandle = SessionHandle.fromUUID(batch.identifier)
              sessionManager.getBatchSession(sessionHandle).foreach(_.close())
            }
          }
        } catch {
          case e: Throwable => error("Error checking batch sessions", e)
        }
      }
    }

    scheduleTolerableRunnableWithFixedDelay(
      batchChecker,
      task,
      interval,
      interval,
      TimeUnit.MILLISECONDS)
  }

  private val batchRecoveryLock: ReentrantLock = new ReentrantLock()
  private def withBatchRecoveryLockRequired[T](block: => T): T = {
    batchRecoveryLock.lock()
    try {
      block
    } finally {
      batchRecoveryLock.unlock()
    }
  }

  @VisibleForTesting
  private[kyuubi] def recoverBatchSessions(): Unit = withBatchRecoveryLockRequired {
    val recoveryNumThreads = conf.get(METADATA_RECOVERY_THREADS)
    val recoveryWaitEngineSubmission = conf.get(METADATA_RECOVERY_WAIT_ENGINE_SUBMISSION)
    val batchRecoveryExecutor =
      ThreadUtils.newDaemonFixedThreadPool(recoveryNumThreads, "batch-recovery-executor")
    try {
      val batchSessionsToRecover = sessionManager.getBatchSessionsToRecover(connectionUrl)
      val pendingRecoveryTasksCount = new AtomicInteger(0)
      val tasks = batchSessionsToRecover.flatMap { batchSession =>
        val batchId = batchSession.batchJobSubmissionOp.batchId
        try {
          val task: Future[Unit] = batchRecoveryExecutor.submit(() =>
            Utils.tryLogNonFatalError {
              sessionManager.openBatchSession(batchSession)
              if (recoveryWaitEngineSubmission) {
                info(s"Waiting for batch[$batchId] engine submission during recovery")
                val batchOp = batchSession.batchJobSubmissionOp
                while (batchSession.getSessionEvent.forall(_.exception.isEmpty) &&
                  !batchOp.appStarted &&
                  !OperationState.isTerminal(batchOp.getStatus.state)) {
                  Thread.sleep(300)
                }
              }
            })
          Some(task -> batchId)
        } catch {
          case e: Throwable =>
            error(s"Error while submitting batch[$batchId] for recovery", e)
            None
        }
      }

      pendingRecoveryTasksCount.addAndGet(tasks.size)

      tasks.foreach { case (task, batchId) =>
        try {
          task.get()
        } catch {
          case e: Throwable =>
            error(s"Error while recovering batch[$batchId]", e)
        } finally {
          val pendingTasks = pendingRecoveryTasksCount.decrementAndGet()
          info(s"Batch[$batchId] recovery task terminated, current pending tasks $pendingTasks")
        }
      }
    } finally {
      ThreadUtils.shutdown(batchRecoveryExecutor)
    }
  }

  private[kyuubi] def recoverBatchSessionsFromReassign(batchIds: Seq[String]): Seq[String] =
    withBatchRecoveryLockRequired {
      val recoveryNumThreads = conf.get(METADATA_RECOVERY_THREADS)
      val batchRecoveryExecutor =
        ThreadUtils.newDaemonFixedThreadPool(recoveryNumThreads, "batch-reassign-recovery-executor")
      try {
        val batchSessionsToRecover =
          sessionManager.getSpecificBatchSessionsToRecover(batchIds, connectionUrl)
        val pendingRecoveryTasksCount = new AtomicInteger(0)
        val tasks = batchSessionsToRecover.flatMap { batchSession =>
          val batchId = batchSession.batchJobSubmissionOp.batchId
          try {
            val task: Future[Unit] = batchRecoveryExecutor.submit(() =>
              Utils.tryLogNonFatalError {
                info(s"Recovering batch[$batchId] from reassign")
                sessionManager.openBatchSession(batchSession)
              })
            Some(task -> batchId)
          } catch {
            case e: Throwable =>
              error(s"Error while submitting batch[$batchId] for recovery", e)
              None
          }
        }

        pendingRecoveryTasksCount.addAndGet(tasks.size)

        val finishedBatchIds: Seq[String] = tasks.flatMap { case (task, batchId) =>
          try {
            task.get()
            val pendingTasks = pendingRecoveryTasksCount.decrementAndGet()
            info(s"Batch[$batchId] recovery task terminated, current pending tasks $pendingTasks")
            Some(batchId)
          } catch {
            case e: Throwable =>
              error(s"Error while recovering batch[$batchId]", e)
              val pendingTasks = pendingRecoveryTasksCount.decrementAndGet()
              info(s"Batch[$batchId] recovery task terminated, current pending tasks $pendingTasks")
              None
          }
        }
        finishedBatchIds
      } finally {
        ThreadUtils.shutdown(batchRecoveryExecutor)
      }
    }

  /**
   * The REST connection URLs (host:restPort) of all currently-registered Kyuubi servers,
   * including self. Discovery registers the THRIFT_BINARY frontend, so we remap each discovered
   * host to this node's REST port (homogeneous deployment). Used by batch takeover to decide
   * which batch owners are dead (an owner not in this set has lost its discovery lease).
   */
  private[kyuubi] def liveRestInstances(): Set[String] = {
    val self = connectionUrl
    val restPort = self.substring(self.lastIndexOf(":") + 1)
    val serverSpec = DiscoveryPaths.makePath(null, conf.get(HA_NAMESPACE))
    val peers = withDiscoveryClient(conf) { discoveryClient =>
      discoveryClient.getServiceNodesInfo(serverSpec).map(node => s"${node.host}:$restPort")
    }
    (peers :+ self).toSet
  }

  private def getBatchPendingMaxElapse(): Long = {
    val batchPendingElapseTimes = sessionManager.allSessions().map {
      case session: KyuubiBatchSession => session.batchJobSubmissionOp.getPendingElapsedTime
      case _ => 0L
    }
    if (batchPendingElapseTimes.isEmpty) 0L else batchPendingElapseTimes.max
  }

  def waitForServerStarted(): Unit = {
    // block until the HTTP server is started, otherwise, we may get
    // the wrong HTTP server port -1
    while (!server.isStarted) {
      info(s"Waiting for $getName's HTTP server getting started")
      Thread.sleep(1000)
    }
  }

  override def start(): Unit = synchronized {
    if (!isStarted.get) {
      try {
        server.start()
        startInternal()
        waitForServerStarted()
        isStarted.set(true)
        startBatchChecker()
        recoverBatchSessions()
        MetricsSystem.tracing { ms =>
          ms.registerGauge(OPERATION_BATCH_PENDING_MAX_ELAPSE, getBatchPendingMaxElapse, 0)
        }
      } catch {
        case e: Exception => throw new KyuubiException(s"Cannot start $getName", e)
      }
    }
    super.start()
    info(s"Exposing REST endpoint at: http://${server.getServerUri}")
  }

  override def stop(): Unit = synchronized {
    ThreadUtils.shutdown(batchChecker)
    DataAgentResource.shutdown()
    if (isStarted.getAndSet(false)) {
      handoffBatchSessionsToPeers()
      server.stop()
    }
    super.stop()
  }

  /**
   * On graceful shutdown, proactively hand this instance's live batch sessions to surviving peers
   * by reassigning ownership in the metadata store (atomic CAS). Session close on shutdown does not
   * kill the batch drivers (they run detached in cluster mode), so this only moves who tracks them;
   * each peer's takeover sweep then re-attaches. If handoff is partial or skipped, the sweep still
   * reclaims them once this instance's discovery lease expires - so it is best-effort acceleration,
   * not a correctness dependency.
   */
  private def handoffBatchSessionsToPeers(): Unit = {
    if (batchTakeoverService.isEmpty || sessionManager.metadataManager.isEmpty) return
    try {
      val peers = (liveRestInstances() - connectionUrl).toSeq
      val localBatchIds = sessionManager.allSessions().collect {
        case s: KyuubiBatchSession
            if !OperationState.isTerminal(s.batchJobSubmissionOp.getStatus.state) =>
          s.batchJobSubmissionOp.batchId
      }.toSeq
      if (peers.isEmpty) {
        if (localBatchIds.nonEmpty) {
          warn(
            s"No live peers to hand off ${localBatchIds.size} batch session(s) to; they will be" +
              s" reclaimed by a peer's takeover sweep after this instance leaves discovery")
        }
        return
      }
      localBatchIds.zipWithIndex.foreach { case (batchId, idx) =>
        val peer = peers(idx % peers.size)
        try {
          if (sessionManager.metadataManager.exists(
              _.transferBatchOwnership(batchId, connectionUrl, peer))) {
            info(s"Handed off batch $batchId to peer $peer for graceful shutdown")
          }
        } catch {
          case e: Throwable => warn(s"Failed to hand off batch $batchId to $peer", e)
        }
      }
    } catch {
      case e: Throwable => warn("Error during batch handoff on shutdown", e)
    }
  }

  def getRealUser(): String = {
    ServiceUtils.getShortName(
      Option(AuthenticationFilter.getUserName).filter(_.nonEmpty).getOrElse("anonymous"))
  }

  def getSessionUser(proxyUser: String): String = {
    // Internally, we use kyuubi.session.proxy.user to unify the key as proxyUser
    val sessionConf = Option(proxyUser).filter(_.nonEmpty).map(proxyUser =>
      Map(PROXY_USER.key -> proxyUser)).getOrElse(Map())
    getSessionUser(sessionConf)
  }

  def getSessionUser(sessionConf: Map[String, String]): String = {
    // using the remote ip address instead of that in proxy http header for authentication
    val ipAddress = AuthenticationFilter.getUserIpAddress
    val realUser: String = getRealUser()
    try {
      getProxyUser(sessionConf, ipAddress, realUser)
    } catch {
      case t: Throwable => throw new WebApplicationException(
          t.getMessage,
          Status.FORBIDDEN)
    }
  }

  def getIpAddress: String = {
    Option(AuthenticationFilter.getUserProxyHeaderIpAddress).getOrElse(
      AuthenticationFilter.getUserIpAddress)
  }

  private def getProxyUser(
      sessionConf: Map[String, String],
      ipAddress: String,
      realUser: String): String = {
    if (sessionConf == null) {
      realUser
    } else {
      val proxyUser = sessionConf.getOrElse(
        PROXY_USER.key,
        sessionConf.getOrElse(AuthUtils.HS2_PROXY_USER, realUser))
      if (!proxyUser.equals(realUser) && !isAdministrator(realUser)) {
        AuthUtils.verifyProxyAccess(realUser, proxyUser, ipAddress, hadoopConf)
      }
      proxyUser
    }
  }

  override val discoveryService: Option[Service] = None
}
