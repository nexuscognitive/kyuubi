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

import { describe, it, expect } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'
import ElementPlus from 'element-plus'
import RecoveryCard from '@/views/spark-connect/components/RecoveryCard.vue'
import SessionCard from '@/views/spark-connect/components/SessionCard.vue'
import { createI18n } from '@/test/unit/utils'
import en_US from '@/locales/en_US'
import zh_CN from '@/locales/zh_CN'
import type {
  SparkConnectDriverPostMortem,
  SparkConnectSessionData
} from '@/api/spark-connect'

const SESSION: SparkConnectSessionData = {
  sessionId: 'a-session-id',
  user: 'alice',
  createTime: 1700000000000,
  state: 'RUNNING',
  engineId: 'spark-application-1',
  engineUrl: 'http://engine:4040',
  connectUrl: 'sc://kyuubi.example.com:15002',
  generation: 0,
  restartCount: 0,
  lastRestartTime: 0,
  recoveryMessage: null,
  stateLossWarning: null,
  driverPostMortems: []
}

const OOM_POST_MORTEM: SparkConnectDriverPostMortem = {
  capturedTime: 1700000600000,
  driverName: 'spark-connect-driver-1',
  location: 'analytics',
  finalState: 'Failed',
  applicationState: 'FAILED',
  summary: 'OOMKilled (exit 137)',
  oomKilled: true,
  reason: null,
  message: null,
  containers: [
    {
      name: 'spark-kubernetes-driver',
      reason: 'OOMKilled',
      message: null,
      exitCode: 137,
      signal: 9,
      oomKilled: true,
      restartCount: 0,
      finishedAt: '2026-08-27T02:11:04Z'
    }
  ],
  events: [
    {
      type: 'Warning',
      reason: 'Evicted',
      message: 'The node was low on resource: memory',
      count: 1,
      firstTimestamp: '2026-08-27T02:11:03Z',
      lastTimestamp: '2026-08-27T02:11:03Z'
    }
  ]
}

function mountRecovery(overrides: Partial<SparkConnectSessionData> = {}) {
  return mount(RecoveryCard, {
    props: { session: { ...SESSION, ...overrides } },
    global: { plugins: [createI18n(), ElementPlus] }
  })
}

function mountSession(overrides: Partial<SparkConnectSessionData> = {}) {
  return mount(SessionCard, {
    props: { session: { ...SESSION, ...overrides } },
    global: { plugins: [createI18n(), ElementPlus] }
  })
}

describe('SessionCard driver-derived states', () => {
  it('says a dead session is dead rather than leaving DEAD bare', () => {
    const text = mountSession({ state: 'DEAD' }).text()

    expect(text).toContain('DEAD')
    // An operator seeing a bare state has to know which of the two responses it calls for.
    expect(text).toContain(en_US.spark_connect.state_dead_note)
  })

  it('distinguishes a session still starting from one whose driver died', () => {
    expect(mountSession({ state: 'PENDING' }).text()).toContain(
      en_US.spark_connect.pending_note
    )
    expect(mountSession({ state: 'PENDING' }).text()).not.toContain(
      en_US.spark_connect.state_dead_note
    )
  })

  it('says plainly when a session is finished and will not be recovered', () => {
    expect(mountSession({ state: 'FAILED' }).text()).toContain(
      en_US.spark_connect.state_failed_note
    )
  })

  it('says a replacement driver is on its way while one is being launched', () => {
    expect(mountSession({ state: 'RECOVERING' }).text()).toContain(
      en_US.spark_connect.state_recovering_note
    )
  })
})

describe('RecoveryCard', () => {
  it('shows how many drivers were replaced and when', () => {
    const text = mountRecovery({
      restartCount: 2,
      generation: 2,
      lastRestartTime: 1700000600000
    }).text()

    expect(text).toContain('2')
    expect(text).toContain('2023-11-14')
    expect(text).not.toContain(en_US.spark_connect.never_restarted)
  })

  it('says a recovered session is a new Spark session', () => {
    const text = mountRecovery({
      restartCount: 1,
      generation: 1,
      stateLossWarning: 'Your temporary views are gone.'
    }).text()

    expect(text).toContain(en_US.spark_connect.state_loss_warning)
    expect(text).toContain('Your temporary views are gone.')
    // The general statement is on the card whether or not the server sent a warning, because it
    // is what makes the restart count mean something.
    expect(text).toContain(en_US.spark_connect.recovery_note)
  })

  it('leads with the reason when recovery has been abandoned', () => {
    const text = mountRecovery({
      restartCount: 3,
      recoveryMessage: 'The driver died 4 times; the limit is 3.'
    }).text()

    // The state an operator most needs to understand, so it is a sentence rather than a number
    // to infer from.
    expect(text).toContain(en_US.spark_connect.recovery_abandoned)
    expect(text).toContain('The driver died 4 times; the limit is 3.')
  })

  it('shows what killed each driver, including the events the pod took with it', async () => {
    // The tables inside the collapse render on the next tick, like every other el-table here.
    const wrapper = mountRecovery({
      restartCount: 1,
      generation: 1,
      driverPostMortems: [OOM_POST_MORTEM]
    })
    await flushPromises()
    const text = wrapper.text()

    expect(text).toContain('OOMKilled (exit 137)')
    expect(text).toContain('spark-connect-driver-1')
    expect(text).toContain(en_US.spark_connect.post_mortem_oom)
    // The whole point of capturing at the moment of death: this event no longer exists on the
    // cluster, because Kubernetes deleted it with the pod.
    expect(text).toContain('The node was low on resource: memory')
  })

  it('says so rather than showing an empty panel when no driver has died', () => {
    expect(mountRecovery().text()).toContain(
      en_US.spark_connect.no_post_mortems
    )
  })

  it('is translated in both locales', () => {
    const keys = [
      'recovery',
      'recovery_note',
      'restart_count',
      'generation',
      'last_restart',
      'never_restarted',
      'recovery_abandoned',
      'state_loss_warning',
      'post_mortems',
      'post_mortems_note',
      'no_post_mortems',
      'post_mortem_when',
      'post_mortem_driver',
      'post_mortem_cause',
      'post_mortem_state',
      'post_mortem_oom',
      'post_mortem_events',
      'post_mortem_containers',
      'container_exit_code',
      'container_signal',
      'container_finished_at',
      'state_dead_note',
      'state_recovering_note',
      'state_failed_note'
    ] as const

    keys.forEach((key) => {
      expect(en_US.spark_connect[key], `en_US is missing ${key}`).toBeTruthy()
      expect(zh_CN.spark_connect[key], `zh_CN is missing ${key}`).toBeTruthy()
    })
  })
})
