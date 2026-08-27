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
 * What killed one of a session's drivers, recorded while its pod still existed.
 *
 * <p>Kubernetes events are namespaced objects with a short TTL, collected once the object they
 * involve is gone, so by the time anyone looks at a session that died overnight there is nothing
 * left on the cluster to read. This is the copy Kyuubi took at the moment of death, which is why it
 * can still answer "why did it die" hours later.
 *
 * <p>A session carries several of these, newest first: the same failure on every attempt is a crash
 * loop, and three different failures are three problems.
 */
public class SparkConnectDriverPostMortem {
  private Long capturedTime;
  private String driverName;
  private String location;
  private String finalState;
  private String applicationState;
  private String summary;
  private boolean oomKilled;
  private String reason;
  private String message;
  private List<SparkConnectDriverContainerExit> containers;
  private List<SparkConnectDriverEvent> events;

  public SparkConnectDriverPostMortem() {}

  public SparkConnectDriverPostMortem(
      Long capturedTime,
      String driverName,
      String location,
      String finalState,
      String applicationState,
      String summary,
      boolean oomKilled,
      String reason,
      String message,
      List<SparkConnectDriverContainerExit> containers,
      List<SparkConnectDriverEvent> events) {
    this.capturedTime = capturedTime;
    this.driverName = driverName;
    this.location = location;
    this.finalState = finalState;
    this.applicationState = applicationState;
    this.summary = summary;
    this.oomKilled = oomKilled;
    this.reason = reason;
    this.message = message;
    this.containers = containers;
    this.events = events;
  }

  /**
   * When Kyuubi took this snapshot.
   *
   * <p>The closest honest answer to a time of death: a driver's own timestamps are often absent,
   * and this is the moment Kyuubi observed the pod terminate.
   */
  public Long getCapturedTime() {
    return capturedTime;
  }

  public void setCapturedTime(Long capturedTime) {
    this.capturedTime = capturedTime;
  }

  /** What the driver was called on the cluster -- the pod name, on Kubernetes. */
  public String getDriverName() {
    return driverName;
  }

  public void setDriverName(String driverName) {
    this.driverName = driverName;
  }

  /** Where it ran -- the namespace, on Kubernetes. */
  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  /** The terminal state the cluster manager reported, verbatim. */
  public String getFinalState() {
    return finalState;
  }

  public void setFinalState(String finalState) {
    this.finalState = finalState;
  }

  /** The state Kyuubi derived from it, which is what drove Kyuubi's own decisions. */
  public String getApplicationState() {
    return applicationState;
  }

  public void setApplicationState(String applicationState) {
    this.applicationState = applicationState;
  }

  /** A one-line cause, for a badge or a log line. */
  public String getSummary() {
    return summary;
  }

  public void setSummary(String summary) {
    this.summary = summary;
  }

  public boolean isOomKilled() {
    return oomKilled;
  }

  public void setOomKilled(boolean oomKilled) {
    this.oomKilled = oomKilled;
  }

  public String getReason() {
    return reason;
  }

  public void setReason(String reason) {
    this.reason = reason;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public List<SparkConnectDriverContainerExit> getContainers() {
    return containers == null ? Collections.emptyList() : containers;
  }

  public void setContainers(List<SparkConnectDriverContainerExit> containers) {
    this.containers = containers;
  }

  /** The pod's Kubernetes events as of the moment of death, newest first and bounded. */
  public List<SparkConnectDriverEvent> getEvents() {
    return events == null ? Collections.emptyList() : events;
  }

  public void setEvents(List<SparkConnectDriverEvent> events) {
    this.events = events;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SparkConnectDriverPostMortem that = (SparkConnectDriverPostMortem) o;
    return Objects.equals(getDriverName(), that.getDriverName())
        && Objects.equals(getCapturedTime(), that.getCapturedTime());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getDriverName(), getCapturedTime());
  }

  @Override
  public String toString() {
    return new ReflectionToStringBuilder(this, ToStringStyle.JSON_STYLE).toString();
  }
}
