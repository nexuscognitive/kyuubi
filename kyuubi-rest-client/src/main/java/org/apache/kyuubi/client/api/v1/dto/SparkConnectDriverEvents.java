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
 * Kubernetes events for a Spark Connect session's driver pod, newest first.
 *
 * <p>Wrapped rather than returned as a bare list so that "there is no driver pod to have events"
 * and "the driver pod has had no events" stay distinguishable: a bare empty array would read as the
 * second in both cases.
 */
public class SparkConnectDriverEvents {
  private String sessionId;
  private Boolean available;
  private String message;
  private List<SparkConnectDriverEvent> events;

  public SparkConnectDriverEvents() {}

  public SparkConnectDriverEvents(
      String sessionId, Boolean available, String message, List<SparkConnectDriverEvent> events) {
    this.sessionId = sessionId;
    this.available = available;
    this.message = message;
    this.events = events;
  }

  public String getSessionId() {
    return sessionId;
  }

  public void setSessionId(String sessionId) {
    this.sessionId = sessionId;
  }

  /** Whether a driver pod was found to read events from. */
  public Boolean getAvailable() {
    return available;
  }

  public void setAvailable(Boolean available) {
    this.available = available;
  }

  /** Why there are no events: no driver pod yet, not a Kubernetes deployment, or none recorded. */
  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

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
    SparkConnectDriverEvents that = (SparkConnectDriverEvents) o;
    return Objects.equals(getSessionId(), that.getSessionId())
        && Objects.equals(getEvents(), that.getEvents());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getSessionId(), getEvents());
  }

  @Override
  public String toString() {
    return new ReflectionToStringBuilder(this, ToStringStyle.JSON_STYLE).toString();
  }
}
