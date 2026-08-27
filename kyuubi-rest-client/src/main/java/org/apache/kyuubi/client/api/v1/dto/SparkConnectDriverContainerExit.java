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

/**
 * Why one container of a dead driver stopped, as recorded when it died.
 *
 * <p>{@code oomKilled} is broken out rather than left for a reader to spot in {@code reason}
 * because it is both the commonest way a Spark Connect driver dies and the one with a different
 * answer: more driver memory, rather than a look at the query.
 */
public class SparkConnectDriverContainerExit {
  private String name;
  private String reason;
  private String message;
  private Integer exitCode;
  private Integer signal;
  private boolean oomKilled;
  private int restartCount;
  private String finishedAt;

  public SparkConnectDriverContainerExit() {}

  public SparkConnectDriverContainerExit(
      String name,
      String reason,
      String message,
      Integer exitCode,
      Integer signal,
      boolean oomKilled,
      int restartCount,
      String finishedAt) {
    this.name = name;
    this.reason = reason;
    this.message = message;
    this.exitCode = exitCode;
    this.signal = signal;
    this.oomKilled = oomKilled;
    this.restartCount = restartCount;
    this.finishedAt = finishedAt;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
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

  public Integer getExitCode() {
    return exitCode;
  }

  public void setExitCode(Integer exitCode) {
    this.exitCode = exitCode;
  }

  public Integer getSignal() {
    return signal;
  }

  public void setSignal(Integer signal) {
    this.signal = signal;
  }

  public boolean isOomKilled() {
    return oomKilled;
  }

  public void setOomKilled(boolean oomKilled) {
    this.oomKilled = oomKilled;
  }

  public int getRestartCount() {
    return restartCount;
  }

  public void setRestartCount(int restartCount) {
    this.restartCount = restartCount;
  }

  public String getFinishedAt() {
    return finishedAt;
  }

  public void setFinishedAt(String finishedAt) {
    this.finishedAt = finishedAt;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SparkConnectDriverContainerExit that = (SparkConnectDriverContainerExit) o;
    return Objects.equals(getName(), that.getName())
        && Objects.equals(getFinishedAt(), that.getFinishedAt());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getName(), getFinishedAt());
  }

  @Override
  public String toString() {
    return new ReflectionToStringBuilder(this, ToStringStyle.JSON_STYLE).toString();
  }
}
