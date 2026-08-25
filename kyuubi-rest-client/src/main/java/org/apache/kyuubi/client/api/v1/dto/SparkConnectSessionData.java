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
 * One live Spark Connect session, as listed back to the user who owns it.
 *
 * <p>This deliberately has no {@code token} field, and never will. Kyuubi keeps only the token's
 * digest, so it could not reissue one even if listing it were desirable; more to the point, a
 * bearer token that a list endpoint hands out is a credential that leaks into every browser cache,
 * proxy log and screenshot of the page that renders it. The token is shown once, in the response to
 * the create call, and the UI tells the user so.
 */
public class SparkConnectSessionData {
  private String sessionId;
  private String user;
  private Long createTime;
  private String state;
  private String engineId;
  private String engineUrl;

  public SparkConnectSessionData() {}

  public SparkConnectSessionData(
      String sessionId,
      String user,
      Long createTime,
      String state,
      String engineId,
      String engineUrl) {
    this.sessionId = sessionId;
    this.user = user;
    this.createTime = createTime;
    this.state = state;
    this.engineId = engineId;
    this.engineUrl = engineUrl;
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

  /** One of {@code PENDING}, {@code RUNNING}, {@code CLOSED} or {@code FAILED}. */
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
