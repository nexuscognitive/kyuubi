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
 * The caller's Spark Connect session.
 *
 * <p>Spark Connect has no open-session RPC, so a client creates its session over REST and only then
 * points a {@code SparkSession} at the gRPC port. There is no token here: the gRPC port
 * authenticates the same bearer credential this REST call was made with, and routes on the user it
 * resolves to. A caller has one session, so creating one twice returns the same session rather than
 * a second.
 */
public class SparkConnectSession {
  private String sessionId;
  private String connectUrl;

  public SparkConnectSession() {}

  public SparkConnectSession(String sessionId, String connectUrl) {
    this.sessionId = sessionId;
    this.connectUrl = connectUrl;
  }

  public String getSessionId() {
    return sessionId;
  }

  public void setSessionId(String sessionId) {
    this.sessionId = sessionId;
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

  @Override
  public String toString() {
    return new ReflectionToStringBuilder(this, ToStringStyle.JSON_STYLE).toString();
  }
}
