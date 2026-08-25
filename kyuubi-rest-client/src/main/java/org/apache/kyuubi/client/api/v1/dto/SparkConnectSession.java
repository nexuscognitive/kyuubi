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
 * A newly created Spark Connect session.
 *
 * <p>Spark Connect has no open-session RPC, so a client creates its session over REST and only then
 * points a {@code SparkSession} at the gRPC port. The token both routes the connection to this
 * session's engine and authenticates it, and is returned exactly once -- Kyuubi keeps only its
 * digest and cannot reissue it.
 */
public class SparkConnectSession {
  private String sessionId;
  private String token;
  private String connectUrl;

  public SparkConnectSession() {}

  public SparkConnectSession(String sessionId, String token, String connectUrl) {
    this.sessionId = sessionId;
    this.token = token;
    this.connectUrl = connectUrl;
  }

  public String getSessionId() {
    return sessionId;
  }

  public void setSessionId(String sessionId) {
    this.sessionId = sessionId;
  }

  public String getToken() {
    return token;
  }

  public void setToken(String token) {
    this.token = token;
  }

  /** The {@code sc://} URL to hand to {@code SparkSession.builder.remote(...)}. */
  public String getConnectUrl() {
    return connectUrl;
  }

  public void setConnectUrl(String connectUrl) {
    this.connectUrl = connectUrl;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SparkConnectSession that = (SparkConnectSession) o;
    return Objects.equals(getSessionId(), that.getSessionId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getSessionId());
  }

  /** Deliberately omits the token so it cannot reach a log through a stray {@code toString}. */
  @Override
  public String toString() {
    return new ReflectionToStringBuilder(this, ToStringStyle.JSON_STYLE)
        .setExcludeFieldNames("token")
        .toString();
  }
}
