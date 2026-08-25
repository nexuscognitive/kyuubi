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

import request from '@/utils/request'

const SPARK_CONNECT_BASE_URL = 'api/v1/spark-connect'

/**
 * The caller's session, as the create call returns it.
 *
 * There is no token here, and there is no token anywhere else either: a client authenticates the
 * gRPC port with the same credential it made this call with, and the server routes on the user it
 * resolves to. Creating a session twice returns the same session rather than a second one.
 */
export interface SparkConnectSession {
  sessionId: string
  connectUrl: string
}

/** A session as listed back. Deliberately has no `token` -- see the server-side DTO. */
export interface SparkConnectSessionData {
  sessionId: string
  user: string
  createTime: number
  state: string
  engineId: string
  engineUrl: string
  connectUrl: string
}

export function openSparkConnectSession(
  configs: Record<string, string> = {}
): Promise<SparkConnectSession> {
  return request({
    url: `${SPARK_CONNECT_BASE_URL}/sessions`,
    method: 'post',
    data: { configs }
  }) as Promise<SparkConnectSession>
}

export function listSparkConnectSessions(): Promise<SparkConnectSessionData[]> {
  return request({
    url: `${SPARK_CONNECT_BASE_URL}/sessions`,
    method: 'get'
  }) as Promise<SparkConnectSessionData[]>
}

export function closeSparkConnectSession(sessionId: string): Promise<unknown> {
  return request({
    url: `${SPARK_CONNECT_BASE_URL}/sessions/${encodeURIComponent(sessionId)}`,
    method: 'delete'
  })
}
