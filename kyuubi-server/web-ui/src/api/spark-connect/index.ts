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

/** A log the server hands back a page of, shaped like every other Kyuubi operation log. */
export interface SparkConnectLog {
  logRowSet: string[]
  rowCount: number
}

/** One container of the driver pod. */
export interface SparkConnectDriverContainer {
  name: string
  state: string
  stateReason: string | null
  ready: boolean
  restartCount: number
  exitCode: number | null
  lastTerminationReason: string | null
  lastTerminationExitCode: number | null
  requests: Record<string, string>
  limits: Record<string, string>
}

/**
 * The driver behind a session.
 *
 * `available` is what the page has to branch on: a session spends its first minute or two with no
 * driver pod, and a deployment that does not run engines on Kubernetes never has one. The server
 * says which in `message` rather than returning an empty record.
 */
export interface SparkConnectDriverInfo {
  sessionId: string
  available: boolean
  message: string | null
  engineId: string | null
  engineUrl: string | null
  podName: string | null
  namespace: string | null
  nodeName: string | null
  phase: string | null
  reason: string | null
  startTime: string | null
  podIp: string | null
  containers: SparkConnectDriverContainer[]
}

export interface SparkConnectDriverEvent {
  type: string
  reason: string
  message: string
  count: number
  firstTimestamp: string | null
  lastTimestamp: string | null
}

export interface SparkConnectDriverEvents {
  sessionId: string
  available: boolean
  message: string | null
  events: SparkConnectDriverEvent[]
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

/** The engine submit log -- what `spark-submit` printed, from Kyuubi's own work directory. */
export function getSparkConnectSubmitLog(
  sessionId: string,
  size = 500
): Promise<SparkConnectLog> {
  return request({
    url: `${SPARK_CONNECT_BASE_URL}/sessions/${encodeURIComponent(
      sessionId
    )}/log`,
    method: 'get',
    // `from: 0` rather than the server's `-1` default, so a page that polls re-reads the log from
    // the top instead of consuming it a window at a time.
    params: { from: 0, size }
  }) as Promise<SparkConnectLog>
}

export function getSparkConnectDriverInfo(
  sessionId: string
): Promise<SparkConnectDriverInfo> {
  return request({
    url: `${SPARK_CONNECT_BASE_URL}/sessions/${encodeURIComponent(
      sessionId
    )}/driver`,
    method: 'get'
  }) as Promise<SparkConnectDriverInfo>
}

export function getSparkConnectDriverLog(
  sessionId: string,
  lines = 500
): Promise<SparkConnectLog> {
  return request({
    url: `${SPARK_CONNECT_BASE_URL}/sessions/${encodeURIComponent(
      sessionId
    )}/driver/log`,
    method: 'get',
    params: { lines }
  }) as Promise<SparkConnectLog>
}

export function getSparkConnectDriverEvents(
  sessionId: string,
  size = 100
): Promise<SparkConnectDriverEvents> {
  return request({
    url: `${SPARK_CONNECT_BASE_URL}/sessions/${encodeURIComponent(
      sessionId
    )}/driver/events`,
    method: 'get',
    params: { size }
  }) as Promise<SparkConnectDriverEvents>
}
