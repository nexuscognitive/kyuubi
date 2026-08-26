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
import { mount, flushPromises } from '@vue/test-utils'
import ElementPlus from 'element-plus'
import { createI18n } from '@/test/unit/utils'
import en_US from '@/locales/en_US'

const getDriverInfo = vi.fn()
const getDriverEvents = vi.fn()
const getSubmitLog = vi.fn()
const getDriverLog = vi.fn()

vi.mock('@/api/spark-connect', () => ({
  getSparkConnectDriverInfo: (id: string) => getDriverInfo(id),
  getSparkConnectDriverEvents: (id: string) => getDriverEvents(id),
  getSparkConnectSubmitLog: (id: string) => getSubmitLog(id),
  getSparkConnectDriverLog: (id: string) => getDriverLog(id)
}))

import DiagnosticsCard from '@/views/spark-connect/components/DiagnosticsCard.vue'

const RUNNING_DRIVER = {
  sessionId: 'a-session-id',
  available: true,
  message: null,
  engineId: 'spark-application-1',
  engineUrl: 'http://engine:4040',
  podName: 'spark-connect-driver',
  namespace: 'analytics',
  nodeName: 'node-7',
  phase: 'Running',
  reason: null,
  startTime: '2026-07-23T18:20:01Z',
  podIp: '10.1.2.3',
  containers: [
    {
      name: 'spark-kubernetes-driver',
      state: 'Running',
      stateReason: null,
      ready: true,
      restartCount: 0,
      exitCode: null,
      lastTerminationReason: null,
      lastTerminationExitCode: null,
      requests: { cpu: '1', memory: '4Gi' },
      limits: { memory: '4Gi' }
    }
  ]
}

async function mountCard() {
  const wrapper = mount(DiagnosticsCard, {
    props: { sessionId: 'a-session-id' },
    global: { plugins: [createI18n(), ElementPlus] }
  })
  await flushPromises()
  return wrapper
}

describe('DiagnosticsCard', () => {
  beforeEach(() => {
    getDriverInfo.mockReset().mockResolvedValue(RUNNING_DRIVER)
    getDriverEvents.mockReset().mockResolvedValue({
      sessionId: 'a-session-id',
      available: true,
      message: null,
      events: []
    })
    getSubmitLog
      .mockReset()
      .mockResolvedValue({ logRowSet: ['submitting engine'], rowCount: 1 })
    getDriverLog
      .mockReset()
      .mockResolvedValue({ logRowSet: ['driver up'], rowCount: 1 })
  })

  it('shows the driver pod, where it runs and what it asked for', async () => {
    const text = (await mountCard()).text()

    expect(text).toContain('spark-connect-driver')
    expect(text).toContain('analytics')
    expect(text).toContain('node-7')
    expect(text).toContain('Running')
    expect(text).toContain('cpu=1, memory=4Gi')
  })

  it('says plainly that there is no driver instead of rendering empty panels', async () => {
    getDriverInfo.mockResolvedValue({
      ...RUNNING_DRIVER,
      available: false,
      message: 'No driver pod for this session yet.',
      podName: null,
      namespace: null,
      nodeName: null,
      phase: null,
      startTime: null,
      podIp: null,
      containers: []
    })

    const text = (await mountCard()).text()

    expect(text).toContain(en_US.spark_connect.no_driver_yet)
    // The server's own explanation, rather than a reason the page made up.
    expect(text).toContain('No driver pod for this session yet.')
    expect(text).not.toContain('spark-connect-driver')
  })

  it('passes on why there are no events rather than showing an empty table', async () => {
    getDriverEvents.mockResolvedValue({
      sessionId: 'a-session-id',
      available: false,
      message: 'Driver diagnostics are unavailable.',
      events: []
    })

    const wrapper = await mountCard()
    await wrapper.findAll('.el-tabs__item')[1].trigger('click')
    await flushPromises()

    expect(wrapper.text()).toContain('Driver diagnostics are unavailable.')
  })

  it('surfaces the events that explain a stuck session, newest first', async () => {
    getDriverEvents.mockResolvedValue({
      sessionId: 'a-session-id',
      available: true,
      message: null,
      events: [
        {
          type: 'Warning',
          reason: 'FailedScheduling',
          message: '0/5 nodes are available',
          count: 3,
          firstTimestamp: '2026-07-23T18:20:01Z',
          lastTimestamp: '2026-07-23T18:25:01Z'
        }
      ]
    })

    const wrapper = await mountCard()
    await wrapper.findAll('.el-tabs__item')[1].trigger('click')
    await flushPromises()

    expect(wrapper.text()).toContain('FailedScheduling')
    expect(wrapper.text()).toContain('0/5 nodes are available')
  })

  it('warns that a restarted driver has lost the session state', async () => {
    getDriverInfo.mockResolvedValue({
      ...RUNNING_DRIVER,
      containers: [
        {
          ...RUNNING_DRIVER.containers[0],
          restartCount: 2,
          lastTerminationReason: 'OOMKilled',
          lastTerminationExitCode: 137
        }
      ]
    })

    const text = (await mountCard()).text()

    expect(text).toContain(en_US.spark_connect.restarted_note)
    expect(text).toContain('OOMKilled (137)')
  })

  it('reads the driver log and the submit log as separate things', async () => {
    const wrapper = await mountCard()

    await wrapper.findAll('.el-tabs__item')[2].trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('driver up')

    await wrapper.findAll('.el-tabs__item')[3].trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('submitting engine')
  })

  it('asks for exactly the session it was given', async () => {
    await mountCard()

    expect(getDriverInfo).toHaveBeenCalledWith('a-session-id')
    expect(getDriverEvents).toHaveBeenCalledWith('a-session-id')
    expect(getSubmitLog).toHaveBeenCalledWith('a-session-id')
    expect(getDriverLog).toHaveBeenCalledWith('a-session-id')
  })

  it('says a log is empty rather than rendering a blank panel', async () => {
    getDriverLog.mockResolvedValue({ logRowSet: [], rowCount: 0 })

    const wrapper = await mountCard()
    await wrapper.findAll('.el-tabs__item')[2].trigger('click')
    await flushPromises()

    expect(wrapper.text()).toContain(en_US.spark_connect.empty_log)
  })
})
