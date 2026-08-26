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
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/** One container of a Spark Connect engine's driver pod. */
public class SparkConnectDriverContainer {
  private String name;
  private String state;
  private String stateReason;
  private Boolean ready;
  private Integer restartCount;
  private Integer exitCode;
  private String lastTerminationReason;
  private Integer lastTerminationExitCode;
  private Map<String, String> requests;
  private Map<String, String> limits;

  public SparkConnectDriverContainer() {}

  public SparkConnectDriverContainer(
      String name,
      String state,
      String stateReason,
      Boolean ready,
      Integer restartCount,
      Integer exitCode,
      String lastTerminationReason,
      Integer lastTerminationExitCode,
      Map<String, String> requests,
      Map<String, String> limits) {
    this.name = name;
    this.state = state;
    this.stateReason = stateReason;
    this.ready = ready;
    this.restartCount = restartCount;
    this.exitCode = exitCode;
    this.lastTerminationReason = lastTerminationReason;
    this.lastTerminationExitCode = lastTerminationExitCode;
    this.requests = requests;
    this.limits = limits;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  /** One of {@code Running}, {@code Waiting}, {@code Terminated} or {@code Unknown}. */
  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  /**
   * Why the container is waiting or was terminated, e.g. {@code ImagePullBackOff}, {@code
   * OOMKilled}. Null while a container is simply running.
   */
  public String getStateReason() {
    return stateReason;
  }

  public void setStateReason(String stateReason) {
    this.stateReason = stateReason;
  }

  public Boolean getReady() {
    return ready;
  }

  public void setReady(Boolean ready) {
    this.ready = ready;
  }

  /**
   * Restarts so far. Any value above zero means the engine lost its Spark Connect state, even if
   * the container is running again now.
   */
  public Integer getRestartCount() {
    return restartCount;
  }

  public void setRestartCount(Integer restartCount) {
    this.restartCount = restartCount;
  }

  public Integer getExitCode() {
    return exitCode;
  }

  public void setExitCode(Integer exitCode) {
    this.exitCode = exitCode;
  }

  public String getLastTerminationReason() {
    return lastTerminationReason;
  }

  public void setLastTerminationReason(String lastTerminationReason) {
    this.lastTerminationReason = lastTerminationReason;
  }

  public Integer getLastTerminationExitCode() {
    return lastTerminationExitCode;
  }

  public void setLastTerminationExitCode(Integer lastTerminationExitCode) {
    this.lastTerminationExitCode = lastTerminationExitCode;
  }

  public Map<String, String> getRequests() {
    return requests == null ? Collections.emptyMap() : requests;
  }

  public void setRequests(Map<String, String> requests) {
    this.requests = requests;
  }

  public Map<String, String> getLimits() {
    return limits == null ? Collections.emptyMap() : limits;
  }

  public void setLimits(Map<String, String> limits) {
    this.limits = limits;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SparkConnectDriverContainer that = (SparkConnectDriverContainer) o;
    return Objects.equals(getName(), that.getName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getName());
  }

  @Override
  public String toString() {
    return new ReflectionToStringBuilder(this, ToStringStyle.JSON_STYLE).toString();
  }
}
