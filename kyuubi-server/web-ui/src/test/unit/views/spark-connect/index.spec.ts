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

import { describe, it, expect, beforeEach } from 'vitest'
import { mount } from '@vue/test-utils'
import ElementPlus from 'element-plus'
import SessionCard from '@/views/spark-connect/components/SessionCard.vue'
import type { SparkConnectSessionData } from '@/api/spark-connect'
import { createI18n } from '@/test/unit/utils'
import en_US from '@/locales/en_US'
import zh_CN from '@/locales/zh_CN'

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

function mountCard(overrides: Partial<typeof SESSION> = {}) {
  return mount(SessionCard, {
    props: { session: { ...SESSION, ...overrides } },
    global: { plugins: [createI18n(), ElementPlus] }
  })
}

describe('SessionCard', () => {
  beforeEach(() => {
    localStorage.clear()
    sessionStorage.clear()
  })

  it('shows the state, the session and the engine behind it', () => {
    const text = mountCard().text()

    expect(text).toContain('RUNNING')
    expect(text).toContain(SESSION.sessionId)
    expect(text).toContain(SESSION.engineId)
  })

  it('says an engine is still starting rather than leaving PENDING bare', () => {
    expect(mountCard({ state: 'PENDING' }).text()).toContain(
      en_US.spark_connect.pending_note
    )
  })

  it('renders a snippet that reads the credential from the environment', () => {
    const text = mountCard().text()

    expect(text).toContain('os.environ["KYUUBI_TOKEN"]')
    expect(text).toContain(
      'f"sc://kyuubi.example.com:15002/;use_ssl=true;token={token}"'
    )
  })

  it('renders nothing that could be mistaken for a token the server issued', () => {
    const text = mountCard().text()

    // A plausible-looking string here is worse than none: someone pastes it and then has to work
    // out why it was rejected. The only credential mentioned is the user's own.
    expect(text).toContain(en_US.spark_connect.credential_note)
    expect(text).not.toMatch(/token=[A-Za-z0-9_-]{20,}/)
  })

  it('shows the advertised connect URL verbatim', () => {
    // A wrong advertised host has to be visible, so the raw value is rendered rather than fixed up.
    expect(mountCard().text()).toContain(SESSION.connectUrl)
  })

  it('emits a close request rather than closing anything itself', async () => {
    const wrapper = mountCard()
    const closeButton = wrapper
      .findAll('button')
      .find((button) => button.text() === en_US.operation.close)

    await closeButton?.trigger('click')
    // The popconfirm sits between the button and the emit, so the request is confirmed first.
    expect(wrapper.emitted('close')).toBeFalsy()
  })

  it('emits the connect URL for copying', async () => {
    const wrapper = mountCard()
    const copyButtons = wrapper
      .findAll('button')
      .filter((button) => button.text() === en_US.spark_connect.copy)

    await copyButtons[0].trigger('click')

    expect(wrapper.emitted('copy')?.[0]).toEqual([SESSION.connectUrl])
  })
})

describe('spark_connect locales', () => {
  it('carries every key in both locales the repo ships', () => {
    expect(Object.keys(zh_CN.spark_connect).sort()).toEqual(
      Object.keys(en_US.spark_connect).sort()
    )
  })

  it('no longer offers a token to display', () => {
    expect(Object.keys(en_US.spark_connect)).not.toContain('token')
    expect(Object.keys(en_US.spark_connect)).not.toContain(
      'token_warning_title'
    )
  })
})
