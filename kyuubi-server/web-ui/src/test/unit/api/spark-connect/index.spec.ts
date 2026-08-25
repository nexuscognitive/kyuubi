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

import { describe, it, expect, vi, beforeEach } from 'vitest'

const request = vi.fn()
vi.mock('@/utils/request', () => ({
  default: (config: unknown) => request(config)
}))

import {
  openSparkConnectSession,
  listSparkConnectSessions,
  closeSparkConnectSession
} from '@/api/spark-connect'

describe('spark-connect api', () => {
  beforeEach(() => {
    request.mockReset()
    request.mockResolvedValue(undefined)
  })

  it('posts the session confs under a configs object', async () => {
    await openSparkConnectSession({ 'spark.sql.shuffle.partitions': '42' })

    expect(request).toHaveBeenCalledWith({
      url: 'api/v1/spark-connect/sessions',
      method: 'post',
      data: { configs: { 'spark.sql.shuffle.partitions': '42' } }
    })
  })

  it('posts an empty conf map when the caller supplies none', async () => {
    await openSparkConnectSession()

    expect(request).toHaveBeenCalledWith(
      expect.objectContaining({ data: { configs: {} } })
    )
  })

  it('returns the created session as the server sent it', async () => {
    const created = {
      sessionId: 'a-session-id',
      token: 'a-token',
      connectUrl: 'sc://host:15002'
    }
    request.mockResolvedValue(created)

    await expect(openSparkConnectSession()).resolves.toEqual(created)
  })

  it('gets the session list from the same path', async () => {
    request.mockResolvedValue([])
    await listSparkConnectSessions()

    expect(request).toHaveBeenCalledWith({
      url: 'api/v1/spark-connect/sessions',
      method: 'get'
    })
  })

  it('does not model a token on listed sessions', async () => {
    // The server omits it deliberately; if one ever appeared, nothing here would carry it onward.
    request.mockResolvedValue([
      {
        sessionId: 'a-session-id',
        user: 'alice',
        createTime: 1,
        state: 'RUNNING',
        engineId: 'app-1',
        engineUrl: 'http://engine:4040'
      }
    ])

    const sessions = await listSparkConnectSessions()

    expect(Object.keys(sessions[0])).not.toContain('token')
  })

  it('deletes by session id', async () => {
    await closeSparkConnectSession('a-session-id')

    expect(request).toHaveBeenCalledWith({
      url: 'api/v1/spark-connect/sessions/a-session-id',
      method: 'delete'
    })
  })

  it('escapes a session id so it cannot break out of the path', async () => {
    await closeSparkConnectSession('../../admin')

    expect(request).toHaveBeenCalledWith(
      expect.objectContaining({
        url: 'api/v1/spark-connect/sessions/..%2F..%2Fadmin'
      })
    )
  })

  it('propagates a failure rather than swallowing it', async () => {
    request.mockRejectedValue(new Error('boom'))

    await expect(listSparkConnectSessions()).rejects.toThrow('boom')
  })
})
