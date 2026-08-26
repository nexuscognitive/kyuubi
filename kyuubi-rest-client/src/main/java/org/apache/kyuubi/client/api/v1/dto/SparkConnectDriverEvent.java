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

import java.util.Objects;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/** One Kubernetes event recorded against a Spark Connect session's driver pod. */
public class SparkConnectDriverEvent {
  private String type;
  private String reason;
  private String message;
  private Integer count;
  private String firstTimestamp;
  private String lastTimestamp;

  public SparkConnectDriverEvent() {}

  public SparkConnectDriverEvent(
      String type,
      String reason,
      String message,
      Integer count,
      String firstTimestamp,
      String lastTimestamp) {
    this.type = type;
    this.reason = reason;
    this.message = message;
    this.count = count;
    this.firstTimestamp = firstTimestamp;
    this.lastTimestamp = lastTimestamp;
  }

  /** {@code Normal} or {@code Warning}. The warnings are the ones worth reading first. */
  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  /** e.g. {@code FailedScheduling}, {@code ErrImagePull}, {@code Killing}. */
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

  /** How many times Kubernetes coalesced this same event. */
  public Integer getCount() {
    return count;
  }

  public void setCount(Integer count) {
    this.count = count;
  }

  public String getFirstTimestamp() {
    return firstTimestamp;
  }

  public void setFirstTimestamp(String firstTimestamp) {
    this.firstTimestamp = firstTimestamp;
  }

  public String getLastTimestamp() {
    return lastTimestamp;
  }

  public void setLastTimestamp(String lastTimestamp) {
    this.lastTimestamp = lastTimestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SparkConnectDriverEvent that = (SparkConnectDriverEvent) o;
    return Objects.equals(getType(), that.getType())
        && Objects.equals(getReason(), that.getReason())
        && Objects.equals(getMessage(), that.getMessage())
        && Objects.equals(getLastTimestamp(), that.getLastTimestamp());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getType(), getReason(), getMessage(), getLastTimestamp());
  }

  @Override
  public String toString() {
    return new ReflectionToStringBuilder(this, ToStringStyle.JSON_STYLE).toString();
  }
}
