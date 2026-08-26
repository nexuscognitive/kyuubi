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
 * The driver behind one Spark Connect session.
 *
 * <p>{@code available} is load-bearing: a Spark Connect session spends its first minute or two with
 * no driver pod at all, and a deployment that does not run engines on Kubernetes never has one.
 * Both cases return {@code available = false} with a {@code message} that says which, rather than
 * an empty record that reads like a healthy driver with no containers.
 */
public class SparkConnectDriverInfo {
  private String sessionId;
  private Boolean available;
  private String message;
  private String engineId;
  private String engineUrl;
  private String podName;
  private String namespace;
  private String nodeName;
  private String phase;
  private String reason;
  private String startTime;
  private String podIp;
  private List<SparkConnectDriverContainer> containers;

  public SparkConnectDriverInfo() {}

  public SparkConnectDriverInfo(
      String sessionId,
      Boolean available,
      String message,
      String engineId,
      String engineUrl,
      String podName,
      String namespace,
      String nodeName,
      String phase,
      String reason,
      String startTime,
      String podIp,
      List<SparkConnectDriverContainer> containers) {
    this.sessionId = sessionId;
    this.available = available;
    this.message = message;
    this.engineId = engineId;
    this.engineUrl = engineUrl;
    this.podName = podName;
    this.namespace = namespace;
    this.nodeName = nodeName;
    this.phase = phase;
    this.reason = reason;
    this.startTime = startTime;
    this.podIp = podIp;
    this.containers = containers;
  }

  public String getSessionId() {
    return sessionId;
  }

  public void setSessionId(String sessionId) {
    this.sessionId = sessionId;
  }

  /** Whether a driver pod was found. Every pod field below is null when this is false. */
  public Boolean getAvailable() {
    return available;
  }

  public void setAvailable(Boolean available) {
    this.available = available;
  }

  /** Why no driver pod is being reported, when {@code available} is false. */
  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  /** The Spark application id, once the engine has reported in. Tracked by Kyuubi, not the pod. */
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

  public String getPodName() {
    return podName;
  }

  public void setPodName(String podName) {
    this.podName = podName;
  }

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public String getNodeName() {
    return nodeName;
  }

  public void setNodeName(String nodeName) {
    this.nodeName = nodeName;
  }

  /** Pod phase: {@code Pending}, {@code Running}, {@code Succeeded}, {@code Failed}. */
  public String getPhase() {
    return phase;
  }

  public void setPhase(String phase) {
    this.phase = phase;
  }

  public String getReason() {
    return reason;
  }

  public void setReason(String reason) {
    this.reason = reason;
  }

  public String getStartTime() {
    return startTime;
  }

  public void setStartTime(String startTime) {
    this.startTime = startTime;
  }

  public String getPodIp() {
    return podIp;
  }

  public void setPodIp(String podIp) {
    this.podIp = podIp;
  }

  public List<SparkConnectDriverContainer> getContainers() {
    return containers == null ? Collections.emptyList() : containers;
  }

  public void setContainers(List<SparkConnectDriverContainer> containers) {
    this.containers = containers;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SparkConnectDriverInfo that = (SparkConnectDriverInfo) o;
    return Objects.equals(getSessionId(), that.getSessionId())
        && Objects.equals(getPodName(), that.getPodName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getSessionId(), getPodName());
  }

  @Override
  public String toString() {
    return new ReflectionToStringBuilder(this, ToStringStyle.JSON_STYLE).toString();
  }
}
