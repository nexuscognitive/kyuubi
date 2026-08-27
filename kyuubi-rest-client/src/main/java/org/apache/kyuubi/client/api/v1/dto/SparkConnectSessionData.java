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

package org.apache.kyuubi.client.api.v1.dto;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * One live Spark Connect session, as listed back to the user who owns it.
 *
 * <p>This deliberately has no {@code token} field, and never will. There is no per-session token to
 * list: a caller reaches the gRPC port with the platform credential they already hold, and Kyuubi's
 * own credential for the engine never leaves the gateway. A bearer token that a list endpoint hands
 * out is a credential that leaks into every browser cache, proxy log and screenshot of the page
 * that renders it.
 */
public class SparkConnectSessionData {
  private String sessionId;
  private String user;
  private Long createTime;
  private String state;
  private String engineId;
  private String engineUrl;
  private String connectUrl;
  private int generation;
  private int restartCount;
  private Long lastRestartTime;
  private String recoveryMessage;
  private String stateLossWarning;
  private List<SparkConnectDriverPostMortem> driverPostMortems;

  public SparkConnectSessionData() {}

  public SparkConnectSessionData(
      String sessionId,
      String user,
      Long createTime,
      String state,
      String engineId,
      String engineUrl,
      String connectUrl) {
    this(
        sessionId,
        user,
        createTime,
        state,
        engineId,
        engineUrl,
        connectUrl,
        0,
        0,
        0L,
        null,
        null,
        Collections.emptyList());
  }

  public SparkConnectSessionData(
      String sessionId,
      String user,
      Long createTime,
      String state,
      String engineId,
      String engineUrl,
      String connectUrl,
      int generation,
      int restartCount,
      Long lastRestartTime,
      String recoveryMessage,
      String stateLossWarning,
      List<SparkConnectDriverPostMortem> driverPostMortems) {
    this.sessionId = sessionId;
    this.user = user;
    this.createTime = createTime;
    this.state = state;
    this.engineId = engineId;
    this.engineUrl = engineUrl;
    this.connectUrl = connectUrl;
    this.generation = generation;
    this.restartCount = restartCount;
    this.lastRestartTime = lastRestartTime;
    this.recoveryMessage = recoveryMessage;
    this.stateLossWarning = stateLossWarning;
    this.driverPostMortems = driverPostMortems;
  }

  public String getSessionId() {
    return sessionId;
  }

  public void setSessionId(String sessionId) {
    this.sessionId = sessionId;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public Long getCreateTime() {
    return createTime;
  }

  public void setCreateTime(Long createTime) {
    this.createTime = createTime;
  }

  /**
   * One of {@code PENDING}, {@code RUNNING}, {@code RECOVERING}, {@code DEAD}, {@code CLOSED} or
   * {@code FAILED}, reconciled against the driver rather than read off Kyuubi's session record.
   *
   * <p>{@code PENDING} and {@code DEAD} are deliberately distinct: a driver that has not appeared
   * yet is waited out, and one that appeared and died is acted on.
   */
  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  /** Empty until the engine has reported in, which is the whole point of {@code PENDING}. */
  public String getEngineId() {
    return engineId;
  }

  public void setEngineId(String engineId) {
    this.engineId = engineId;
  }

  public String getEngineUrl() {
    return engineUrl;
  }

  public void setEngineUrl(String engineUrl) {
    this.engineUrl = engineUrl;
  }

  /**
   * The {@code sc://} URL to hand to {@code SparkSession.builder.remote(...)}.
   *
   * <p>Listed rather than returned only on create, because the client needs it every time it
   * connects and there is nothing secret about it -- it is the address this gateway advertises.
   */
  public String getConnectUrl() {
    return connectUrl;
  }

  public void setConnectUrl(String connectUrl) {
    this.connectUrl = connectUrl;
  }

  /**
   * How many engines this session has had.
   *
   * <p>0 for a session on its original driver. Every increment is a driver replaced, and therefore
   * a <b>new Spark session</b> -- see {@link #getStateLossWarning()}. A client that keeps this
   * value between polls can tell that its session was replaced without waiting to be surprised by a
   * missing temporary view.
   */
  public int getGeneration() {
    return generation;
  }

  public void setGeneration(int generation) {
    this.generation = generation;
  }

  /** How many times Kyuubi has relaunched a driver for this session. */
  public int getRestartCount() {
    return restartCount;
  }

  public void setRestartCount(int restartCount) {
    this.restartCount = restartCount;
  }

  /** When the most recent relaunch was started, or 0 if there has been none. */
  public Long getLastRestartTime() {
    return lastRestartTime;
  }

  public void setLastRestartTime(Long lastRestartTime) {
    this.lastRestartTime = lastRestartTime;
  }

  /**
   * Why recovery is where it is -- above all, why it gave up.
   *
   * <p>The single most useful field on a session in state {@code FAILED}: it says how many drivers
   * died, what the last one died of, and that no more will be launched.
   */
  public String getRecoveryMessage() {
    return recoveryMessage;
  }

  public void setRecoveryMessage(String recoveryMessage) {
    this.recoveryMessage = recoveryMessage;
  }

  /**
   * Set only on a session whose driver was replaced, to say that its Spark state did not survive.
   *
   * <p>Null on a session that has never been restarted, so that it means something on one that has.
   */
  public String getStateLossWarning() {
    return stateLossWarning;
  }

  public void setStateLossWarning(String stateLossWarning) {
    this.stateLossWarning = stateLossWarning;
  }

  /**
   * What killed this session's drivers, newest first.
   *
   * <p>Captured while each pod still existed, so it survives both the pod and the Kubernetes events
   * that would otherwise have been the only explanation.
   */
  public List<SparkConnectDriverPostMortem> getDriverPostMortems() {
    return driverPostMortems == null ? Collections.emptyList() : driverPostMortems;
  }

  public void setDriverPostMortems(List<SparkConnectDriverPostMortem> driverPostMortems) {
    this.driverPostMortems = driverPostMortems;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SparkConnectSessionData that = (SparkConnectSessionData) o;
    return Objects.equals(getSessionId(), that.getSessionId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getSessionId());
  }

  @Override
  public String toString() {
    return new ReflectionToStringBuilder(this, ToStringStyle.JSON_STYLE).toString();
  }
}
