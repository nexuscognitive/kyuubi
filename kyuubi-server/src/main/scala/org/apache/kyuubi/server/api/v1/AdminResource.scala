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
import java.util.concurrent.ConcurrentHashMap
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

import io.swagger.v3.oas.annotations.media.{ArraySchema, Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.{KYUUBI_VERSION, Logging}
import org.apache.kyuubi.client.api.v1.dto._
import org.apache.kyuubi.config.{KyuubiConf, KyuubiReservedKeys}
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.ApplicationManagerInfo
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_NAMESPACE
import org.apache.kyuubi.ha.client.{DiscoveryPaths, ServiceNodeInfo}
import org.apache.kyuubi.ha.client.DiscoveryClientProvider.withDiscoveryClient
import org.apache.kyuubi.operation.{KyuubiOperation, OperationHandle, OperationState}
import org.apache.kyuubi.server.KyuubiServer
import org.apache.kyuubi.server.api.{ApiRequestContext, ApiUtils}
import org.apache.kyuubi.server.metadata.api.MetadataFilter
import org.apache.kyuubi.session.{KyuubiSession, KyuubiSessionManager, SessionHandle, SessionType}

@Tag(name = "Admin")
@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class AdminResource extends ApiRequestContext with Logging {

  // Shared internal rest clients keyed by peer instance (host:port), reused across requests.
  // Mirrors the cache BatchesResource uses for batch redirection.
  private val internalRestClients = new ConcurrentHashMap[String, InternalRestClient]()
  // The admin fan-out / redirect is a best-effort, partial-on-failure operation. It must fail fast
  // on a stale or dead peer (e.g. a restarted pod whose old address lingers in discovery until its
  // lease expires), NOT block on the batch client's 20s-connect x 3-attempt budget (~69s/peer).
  // So use short, single-attempt timeouts: a dead peer is dropped in ~2s and live peers are merged.
  private val internalSocketTimeout = 5000
  private val internalConnectTimeout = 2000
  private val internalRequestMaxAttempts = 1
  private val internalRequestAttemptWait = 1000
  private lazy val internalSecurityEnabled =
    fe.getConf.get(ENGINE_SECURITY_ENABLED)
  // Upper bound on a cluster-wide fan-out read; dead/slow peers are dropped, never block forever.
  private val peerFanOutTimeout = 10.seconds

  private def getInternalRestClient(kyuubiInstance: String): InternalRestClient = {
    internalRestClients.computeIfAbsent(
      kyuubiInstance,
      (inst: String) =>
        new InternalRestClient(
          inst,
          fe.getConf.get(FRONTEND_PROXY_HTTP_CLIENT_IP_HEADER),
          internalSocketTimeout,
          internalConnectTimeout,
          internalSecurityEnabled,
          internalRequestMaxAttempts,
          internalRequestAttemptWait))
  }

  /**
   * REST endpoints (host:port) of all peer Kyuubi servers except self.
   *
   * Discovery registers the THRIFT_BINARY frontend's connectionUrl (host:thriftPort), but our
   * fan-out talks to peers over REST. In a homogeneous deployment (k8s StatefulSet / identical
   * config) every node uses the same REST bind port, so we remap each discovered host to this
   * node's REST port. Self is excluded by comparing against fe.connectionUrl (the REST url).
   *
   * Requires a registering frontend to be enabled (THRIFT_BINARY) - otherwise discovery is empty.
   */
  private def peerInstances(): Seq[String] = {
    val myRestUrl = fe.connectionUrl
    val restPort = myRestUrl.substring(myRestUrl.lastIndexOf(":") + 1)
    val serverSpec = DiscoveryPaths.makePath(null, fe.getConf.get(HA_NAMESPACE))
    withDiscoveryClient(fe.getConf) { discoveryClient =>
      discoveryClient.getServiceNodesInfo(serverSpec)
        .map(node => s"${node.host}:$restPort")
        .filter(_ != myRestUrl)
        .distinct
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(mediaType = MediaType.APPLICATION_JSON)),
    description = "refresh the Kyuubi server hadoop conf, note that, " +
      "it only takes affect for frontend services now")
  @POST
  @Path("refresh/hadoop_conf")
  def refreshFrontendHadoopConf(): Response = {
    val userName = fe.getSessionUser(Map.empty[String, String])
    val ipAddress = fe.getIpAddress
    info(s"Receive refresh Kyuubi server hadoop conf request from $userName/$ipAddress")
    if (!fe.isAdministrator(userName)) {
      throw new ForbiddenException(
        s"$userName is not allowed to refresh the Kyuubi server hadoop conf")
    }
    info(s"Reloading the Kyuubi server hadoop conf")
    KyuubiServer.reloadHadoopConf()
    Response.ok(s"Refresh the hadoop conf for ${fe.connectionUrl} successfully.").build()
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(mediaType = MediaType.APPLICATION_JSON)),
    description = "refresh the user defaults configs")
  @POST
  @Path("refresh/user_defaults_conf")
  def refreshUserDefaultsConf(): Response = {
    val userName = fe.getSessionUser(Map.empty[String, String])
    val ipAddress = fe.getIpAddress
    info(s"Receive refresh user defaults conf request from $userName/$ipAddress")
    if (!fe.isAdministrator(userName)) {
      throw new ForbiddenException(
        s"$userName is not allowed to refresh the user defaults conf")
    }
    info(s"Reloading user defaults conf")
    KyuubiServer.refreshUserDefaultsConf()
    Response.ok(s"Refresh the user defaults conf successfully.").build()
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(mediaType = MediaType.APPLICATION_JSON)),
    description = "refresh the kubernetes configs")
  @POST
  @Path("refresh/kubernetes_conf")
  def refreshKubernetesConf(): Response = {
    val userName = fe.getSessionUser(Map.empty[String, String])
    val ipAddress = fe.getIpAddress
    info(s"Receive refresh kubernetes conf request from $userName/$ipAddress")
    if (!fe.isAdministrator(userName)) {
      throw new ForbiddenException(
        s"$userName is not allowed to refresh the kubernetes conf")
    }
    info(s"Reloading kubernetes conf")
    KyuubiServer.refreshKubernetesConf()
    Response.ok(s"Refresh the kubernetes conf successfully.").build()
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(mediaType = MediaType.APPLICATION_JSON)),
    description = "refresh the unlimited users")
  @POST
  @Path("refresh/unlimited_users")
  def refreshUnlimitedUser(): Response = {
    val userName = fe.getSessionUser(Map.empty[String, String])
    val ipAddress = fe.getIpAddress
    info(s"Receive refresh unlimited users request from $userName/$ipAddress")
    if (!fe.isAdministrator(userName)) {
      throw new ForbiddenException(
        s"$userName is not allowed to refresh the unlimited users")
    }
    info(s"Reloading unlimited users")
    KyuubiServer.refreshUnlimitedUsers()
    Response.ok(s"Refresh the unlimited users successfully.").build()
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(mediaType = MediaType.APPLICATION_JSON)),
    description = "refresh the deny users")
  @POST
  @Path("refresh/deny_users")
  def refreshDenyUser(): Response = {
    val userName = fe.getSessionUser(Map.empty[String, String])
    val ipAddress = fe.getIpAddress
    info(s"Receive refresh deny users request from $userName/$ipAddress")
    if (!fe.isAdministrator(userName)) {
      throw new ForbiddenException(
        s"$userName is not allowed to refresh the deny users")
    }
    info(s"Reloading deny users")
    KyuubiServer.refreshDenyUsers()
    Response.ok(s"Refresh the deny users successfully.").build()
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(mediaType = MediaType.APPLICATION_JSON)),
    description = "refresh the deny ips")
  @POST
  @Path("refresh/deny_ips")
  def refreshDenyIp(): Response = {
    val userName = fe.getSessionUser(Map.empty[String, String])
    val ipAddress = fe.getIpAddress
    info(s"Receive refresh deny ips request from $userName/$ipAddress")
    if (!fe.isAdministrator(userName)) {
      throw new ForbiddenException(
        s"$userName is not allowed to refresh the deny ips")
    }
    info(s"Reloading deny ips")
    KyuubiServer.refreshDenyIps()
    Response.ok(s"Refresh the deny ips successfully.").build()
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      array = new ArraySchema(schema = new Schema(implementation = classOf[SessionData])))),
    description = "get the list of all live sessions")
  @GET
  @Path("sessions")
  def sessions(
      @QueryParam("users") users: String,
      @QueryParam("sessionType") sessionType: String,
      @QueryParam("clusterWide") @DefaultValue("false") clusterWide: Boolean): Seq[SessionData] = {
    val userName = fe.getSessionUser(Map.empty[String, String])
    val ipAddress = fe.getIpAddress
    info(s"Received listing all live sessions request from $userName/$ipAddress" +
      (if (clusterWide) " (cluster-wide)" else ""))
    if (!fe.isAdministrator(userName)) {
      throw new ForbiddenException(
        s"$userName is not allowed to list all live sessions")
    }
    var sessions = fe.be.sessionManager.allSessions()
    if (StringUtils.isNoneBlank(sessionType)) {
      sessions = sessions.filter(session =>
        sessionType.equals(session.asInstanceOf[KyuubiSession].sessionType.toString))
    }
    if (StringUtils.isNotBlank(users)) {
      val usersSet = users.split(",").toSet
      sessions = sessions.filter(session => usersSet.contains(session.user))
    }
    val localData =
      sessions.map(session => ApiUtils.sessionData(session.asInstanceOf[KyuubiSession])).toSeq

    if (!clusterWide) return localData

    // Fan-out: query each peer's PLAIN (non-aggregating) list in parallel, tolerate dead peers.
    val peerFutures = peerInstances().map { inst =>
      Future {
        Try(getInternalRestClient(inst).listLocalSessions(userName)) match {
          case Success(rows) => rows
          case Failure(e) =>
            error(s"Failed to list sessions from peer $inst (showing partial results)", e)
            Seq.empty[SessionData]
        }
      }
    }
    val peerData = Try(Await.result(Future.sequence(peerFutures), peerFanOutTimeout))
      .getOrElse(Seq.empty).flatten
      // peers ran the no-arg list, so re-apply the requested filters here
      .filter(s => StringUtils.isBlank(sessionType) || sessionType.equals(s.getSessionType))
      .filter { s =>
        StringUtils.isBlank(users) || users.split(",").toSet.contains(s.getUser)
      }

    // dedupe by session handle (a session lives on exactly one instance, but be defensive)
    val liveData = (localData ++ peerData).groupBy(_.getIdentifier).values.map(_.head).toSeq

    // Merge in active batches from the metadata store whose owning instance is currently
    // unreachable (a dead / restarting pod). Their Spark drivers keep running independently, so
    // rather than vanishing from the view they stay listed, flagged owner-down, and are replaced
    // by the live entry as soon as the owner returns or a peer takes the batch over.
    if (StringUtils.isNotBlank(sessionType) && !SessionType.BATCH.toString.equals(sessionType)) {
      return liveData
    }
    liveData ++ orphanBatchSessions(liveData.map(_.getIdentifier).toSet, users)
  }

  // conf marker on a merged-in batch whose owning instance is currently unreachable.
  private val OWNER_REACHABLE_CONF = "kyuubi.session.owner.reachable"

  /**
   * Active (PENDING/RUNNING) batches from the metadata store that are not present in the live
   * fan-out results - i.e. their owning instance did not report them because it is down or
   * unreachable. Returned as SessionData marked owner-down.
   */
  private def orphanBatchSessions(excludeIds: Set[String], users: String): Seq[SessionData] = {
    val km = fe.be.sessionManager.asInstanceOf[KyuubiSessionManager]
    val userFilter =
      if (StringUtils.isNotBlank(users)) users.split(",").toSet else Set.empty[String]
    val batches =
      try {
        Seq(OperationState.PENDING, OperationState.RUNNING).flatMap { state =>
          km.getBatchesFromMetadataStore(
            MetadataFilter(sessionType = SessionType.BATCH, state = state.toString),
            0,
            Int.MaxValue)
        }
      } catch {
        case e: Throwable =>
          error("Failed to read batches from metadata store for session listing", e)
          Seq.empty[Batch]
      }
    batches
      .filterNot(b => excludeIds.contains(b.getId))
      .filter(b => userFilter.isEmpty || userFilter.contains(b.getUser))
      .groupBy(_.getId).values.map(_.head).toSeq
      .map(batchToSessionData)
  }

  private def batchToSessionData(b: Batch): SessionData = {
    val conf = new java.util.HashMap[String, String]()
    conf.put(OWNER_REACHABLE_CONF, "false")
    new SessionData(
      b.getId,
      null,
      b.getUser,
      null,
      conf,
      java.lang.Long.valueOf(b.getCreateTime),
      null,
      null,
      null,
      SessionType.BATCH.toString,
      b.getKyuubiInstance,
      b.getAppId,
      b.getName,
      b.getAppUrl,
      b.getName,
      java.lang.Integer.valueOf(0))
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(mediaType = MediaType.APPLICATION_JSON)),
    description = "Close a session")
  @DELETE
  @Path("sessions/{sessionHandle}")
  def closeSession(
      @PathParam("sessionHandle") sessionHandleStr: String,
      @QueryParam("kyuubiInstance") kyuubiInstance: String): Response = {
    val userName = fe.getSessionUser(Map.empty[String, String])
    val ipAddress = fe.getIpAddress
    info(s"Received closing a session request from $userName/$ipAddress")
    if (!fe.isAdministrator(userName)) {
      throw new ForbiddenException(
        s"$userName is not allowed to close the session $sessionHandleStr")
    }

    // Forward to the owning instance when the hint points elsewhere. The forwarded close
    // carries no kyuubiInstance hint, so the peer acts locally and never ping-pongs back.
    if (StringUtils.isNotBlank(kyuubiInstance) && kyuubiInstance != fe.connectionUrl) {
      info(s"Redirecting close session[$sessionHandleStr] to $kyuubiInstance")
      getInternalRestClient(kyuubiInstance).closeSession(userName, sessionHandleStr)
      return Response.ok(s"Session $sessionHandleStr closed on $kyuubiInstance.").build()
    }

    val handle = SessionHandle.fromUUID(sessionHandleStr)
    fe.be.sessionManager.getSessionOption(handle) match {
      case Some(_) =>
        fe.be.closeSession(handle)
        Response.ok(s"Session $sessionHandleStr is closed successfully.").build()
      case None =>
        throw new NotFoundException(s"Invalid session handle: $sessionHandleStr")
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      array = new ArraySchema(schema = new Schema(implementation =
        classOf[OperationData])))),
    description =
      "get the list of all active operations")
  @GET
  @Path("operations")
  def listOperations(
      @QueryParam("users") users: String,
      @QueryParam("sessionHandle") sessionHandle: String,
      @QueryParam("sessionType") sessionType: String,
      @QueryParam("clusterWide") @DefaultValue("false")
      clusterWide: Boolean): Seq[OperationData] = {
    val userName = fe.getSessionUser(Map.empty[String, String])
    val ipAddress = fe.getIpAddress
    info(s"Received listing all of the active operations request from $userName/$ipAddress" +
      (if (clusterWide) " (cluster-wide)" else ""))
    if (!fe.isAdministrator(userName)) {
      throw new ForbiddenException(
        s"$userName is not allowed to list all the operations")
    }
    var operations = fe.be.sessionManager.operationManager.allOperations()
    if (StringUtils.isNotBlank(users)) {
      val usersSet = users.split(",").toSet
      operations = operations.filter(operation => usersSet.contains(operation.getSession.user))
    }
    if (StringUtils.isNotBlank(sessionHandle)) {
      operations = operations.filter(operation =>
        operation.getSession.handle.equals(SessionHandle.fromUUID(sessionHandle)))
    }
    if (StringUtils.isNotBlank(sessionType)) {
      operations = operations.filter(operation =>
        sessionType.equalsIgnoreCase(
          operation.getSession.asInstanceOf[KyuubiSession].sessionType.toString))
    }
    val localData = operations
      .map(operation => ApiUtils.operationData(operation.asInstanceOf[KyuubiOperation])).toSeq

    if (!clusterWide) return localData

    // Fan-out: query each peer's PLAIN (non-aggregating) list in parallel, tolerate dead peers.
    val peerFutures = peerInstances().map { inst =>
      Future {
        Try(getInternalRestClient(inst).listLocalOperations(userName)) match {
          case Success(rows) => rows
          case Failure(e) =>
            error(s"Failed to list operations from peer $inst (showing partial results)", e)
            Seq.empty[OperationData]
        }
      }
    }
    val peerData = Try(Await.result(Future.sequence(peerFutures), peerFanOutTimeout))
      .getOrElse(Seq.empty).flatten
      // peers ran the no-arg list, so re-apply the requested filters here.
      .filter(o =>
        StringUtils.isBlank(sessionType) || sessionType.equalsIgnoreCase(o.getSessionType))
      .filter(o => StringUtils.isBlank(sessionHandle) || sessionHandle.equals(o.getSessionId))
      .filter { o =>
        StringUtils.isBlank(users) || users.split(",").toSet.contains(o.getSessionUser)
      }

    (localData ++ peerData).groupBy(_.getIdentifier).values.map(_.head).toSeq
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(mediaType = MediaType.APPLICATION_JSON)),
    description = "close an operation")
  @DELETE
  @Path("operations/{operationHandle}")
  def closeOperation(
      @PathParam("operationHandle") operationHandleStr: String,
      @QueryParam("kyuubiInstance") kyuubiInstance: String): Response = {
    val userName = fe.getSessionUser(Map.empty[String, String])
    val ipAddress = fe.getIpAddress
    info(s"Received close an operation request from $userName/$ipAddress")
    if (!fe.isAdministrator(userName)) {
      throw new ForbiddenException(
        s"$userName is not allowed to close the operation $operationHandleStr")
    }

    // Forward to the owning instance when the hint points elsewhere. No hint on the forwarded
    // call, so the peer acts locally and never re-forwards.
    if (StringUtils.isNotBlank(kyuubiInstance) && kyuubiInstance != fe.connectionUrl) {
      info(s"Redirecting close operation[$operationHandleStr] to $kyuubiInstance")
      getInternalRestClient(kyuubiInstance).closeOperation(userName, operationHandleStr)
      return Response.ok(s"Operation $operationHandleStr closed on $kyuubiInstance.").build()
    }

    val operationHandle = OperationHandle(operationHandleStr)
    fe.be.closeOperation(operationHandle)
    Response.ok(s"Operation $operationHandleStr is closed successfully.").build()
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(mediaType = MediaType.APPLICATION_JSON)),
    description = "delete kyuubi engine")
  @DELETE
  @Path("engine")
  def deleteEngine(
      @QueryParam("type") engineType: String,
      @QueryParam("sharelevel") shareLevel: String,
      @QueryParam("subdomain") subdomain: String,
      @QueryParam("proxyUser") kyuubiProxyUser: String,
      @QueryParam("hive.server2.proxy.user") hs2ProxyUser: String,
      @QueryParam("kill") @DefaultValue("false") kill: Boolean): Response = {
    val activeProxyUser = Option(kyuubiProxyUser).getOrElse(hs2ProxyUser)
    val userName = if (fe.isAdministrator(fe.getRealUser())) {
      Option(activeProxyUser).getOrElse(fe.getRealUser())
    } else {
      fe.getSessionUser(activeProxyUser)
    }
    val engine = normalizeEngineInfo(userName, engineType, shareLevel, subdomain, "default")
    val engineSpace = calculateEngineSpace(engine)
    val responseMsgBuilder = new StringBuilder()

    withDiscoveryClient(fe.getConf) { discoveryClient =>
      val engineNodes = discoveryClient.getServiceNodesInfo(engineSpace, silent = true)
      engineNodes.foreach { engineNode =>
        val nodePath = s"$engineSpace/${engineNode.nodeName}"
        val engineRefId = engineNode.engineRefId.orNull
        info(s"Deleting engine node:$nodePath")
        try {
          discoveryClient.delete(nodePath)
          responseMsgBuilder
            .append(s"Engine $engineSpace refId=$engineRefId is deleted successfully.")
        } catch {
          case e: Exception =>
            error(s"Failed to delete engine node:$nodePath", e)
            throw new NotFoundException(s"Failed to delete engine node:$nodePath," +
              s"${e.getMessage}")
        }

        if (kill && engineRefId != null) {
          val appMgrInfo =
            engineNode.attributes.get(KyuubiReservedKeys.KYUUBI_ENGINE_APP_MGR_INFO_KEY)
              .map(ApplicationManagerInfo.deserialize).getOrElse(ApplicationManagerInfo(None))
          val killResponse = fe.be.sessionManager.asInstanceOf[KyuubiSessionManager]
            .applicationManager.killApplication(appMgrInfo, engineRefId)
          responseMsgBuilder
            .append(s"\nKilled engine with $appMgrInfo/$engineRefId: $killResponse")
        }
      }
    }

    Response.ok(responseMsgBuilder.toString()).build()
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(mediaType = MediaType.APPLICATION_JSON)),
    description = "list kyuubi engines")
  @GET
  @Path("engine")
  def listEngines(
      @QueryParam("type") engineType: String,
      @QueryParam("sharelevel") shareLevel: String,
      @QueryParam("subdomain") subdomain: String,
      @QueryParam("proxyUser") kyuubiProxyUser: String,
      @QueryParam("hive.server2.proxy.user") hs2ProxyUser: String): Seq[Engine] = {
    val activeProxyUser = Option(kyuubiProxyUser).getOrElse(hs2ProxyUser)
    val userName = if (fe.isAdministrator(fe.getRealUser())) {
      Option(activeProxyUser).getOrElse(fe.getRealUser())
    } else {
      fe.getSessionUser(activeProxyUser)
    }
    val engine = normalizeEngineInfo(userName, engineType, shareLevel, subdomain, "")
    val engineSpace = calculateEngineSpace(engine)

    val engineNodes = ListBuffer[ServiceNodeInfo]()
    withDiscoveryClient(fe.getConf) { discoveryClient =>
      Option(subdomain).filter(_.nonEmpty) match {
        case Some(_) =>
          info(s"Listing engine nodes under $engineSpace")
          engineNodes ++= discoveryClient.getServiceNodesInfo(engineSpace)
        case None if discoveryClient.pathNonExists(engineSpace) =>
          warn(s"Path $engineSpace does not exist. user: $userName, engine type: $engineType, " +
            s"share level: $shareLevel, subdomain: $subdomain")
        case None =>
          discoveryClient.getChildren(engineSpace).map { child =>
            info(s"Listing engine nodes under $engineSpace/$child")
            engineNodes ++= discoveryClient.getServiceNodesInfo(s"$engineSpace/$child")
          }
      }
    }
    engineNodes.map(node =>
      new Engine(
        engine.getVersion,
        engine.getUser,
        engine.getEngineType,
        engine.getSharelevel,
        node.namespace.split("/").last,
        node.instance,
        node.namespace,
        node.attributes.asJava))
      .toSeq
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(
      new Content(
        mediaType = MediaType.APPLICATION_JSON,
        array = new ArraySchema(schema = new Schema(implementation =
          classOf[OperationData])))),
    description = "list all live kyuubi servers")
  @GET
  @Path("server")
  def listServers(): Seq[ServerData] = {
    val userName = fe.getSessionUser(Map.empty[String, String])
    val ipAddress = fe.getIpAddress
    info(s"Received list all live kyuubi servers request from $userName/$ipAddress")
    if (!fe.isAdministrator(userName)) {
      throw new ForbiddenException(
        s"$userName is not allowed to list all live kyuubi servers")
    }
    val kyuubiConf = fe.getConf
    val servers = ListBuffer[ServerData]()
    val serverSpec = DiscoveryPaths.makePath(null, kyuubiConf.get(HA_NAMESPACE))
    withDiscoveryClient(kyuubiConf) { discoveryClient =>
      discoveryClient.getServiceNodesInfo(serverSpec).map(nodeInfo => {
        servers += ApiUtils.serverData(nodeInfo)
      })
    }
    servers.toSeq
  }

  private def normalizeEngineInfo(
      userName: String,
      engineType: String,
      shareLevel: String,
      subdomain: String,
      subdomainDefault: String): Engine = {
    // use default value from kyuubi conf when param is not provided
    val clonedConf: KyuubiConf = fe.getConf.clone
    Option(engineType).foreach(clonedConf.set(ENGINE_TYPE, _))
    Option(subdomain).filter(_.nonEmpty)
      .foreach(_ => clonedConf.set(ENGINE_SHARE_LEVEL_SUBDOMAIN, Option(subdomain)))
    Option(shareLevel).filter(_.nonEmpty).foreach(clonedConf.set(ENGINE_SHARE_LEVEL, _))

    val serverSpace = clonedConf.get(HA_NAMESPACE)
    val normalizedEngineType = clonedConf.get(ENGINE_TYPE)
    val engineSubdomain = clonedConf.get(ENGINE_SHARE_LEVEL_SUBDOMAIN).getOrElse(subdomainDefault)
    val engineShareLevel = clonedConf.get(ENGINE_SHARE_LEVEL)

    new Engine(
      KYUUBI_VERSION,
      userName,
      normalizedEngineType,
      engineShareLevel,
      engineSubdomain,
      null,
      serverSpace,
      Collections.emptyMap())
  }

  private def calculateEngineSpace(engine: Engine): String = {
    val userOrGroup = engine.getSharelevel match {
      case "GROUP" =>
        fe.sessionManager.groupProvider.primaryGroup(engine.getUser, fe.getConf.getAll.asJava)
      case _ => engine.getUser
    }

    val engineSpace =
      s"${engine.getNamespace}_${engine.getVersion}_${engine.getSharelevel}_${engine.getEngineType}"
    DiscoveryPaths.makePath(engineSpace, userOrGroup, engine.getSubdomain)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[Count]))),
    description = "get the batch count")
  @GET
  @Path("batch/count")
  def countBatch(
      @QueryParam("batchType") @DefaultValue("SPARK") batchType: String,
      @QueryParam("batchUser") batchUser: String,
      @QueryParam("batchState") batchState: String): Count = {
    val userName = fe.getSessionUser(Map.empty[String, String])
    val ipAddress = fe.getIpAddress
    info(s"Received counting batches request from $userName/$ipAddress")
    if (!fe.isAdministrator(userName)) {
      throw new ForbiddenException(
        s"$userName is not allowed to count the batches")
    }
    val batchCount = fe.batchService
      .map(_.countBatch(batchType, Option(batchUser), Option(batchState)))
      .getOrElse(0)
    new Count(batchCount)
  }
}
