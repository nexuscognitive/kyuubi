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
import SessionCredentials from '@/views/spark-connect/components/SessionCredentials.vue'
import { createI18n } from '@/test/unit/utils'
import en_US from '@/locales/en_US'
import zh_CN from '@/locales/zh_CN'

const SESSION = {
  sessionId: 'a-session-id',
  token: 'a-secret-token',
  connectUrl: 'sc://kyuubi.example.com:15002'
}

function mountCredentials() {
  return mount(SessionCredentials, {
    props: { session: SESSION },
    global: { plugins: [createI18n(), ElementPlus] }
  })
}

describe('SessionCredentials', () => {
  beforeEach(() => {
    localStorage.clear()
    sessionStorage.clear()
  })

  it('shows the token together with the warning that it is shown once', () => {
    const text = mountCredentials().text()

    expect(text).toContain(SESSION.token)
    expect(text).toContain(en_US.spark_connect.token_warning_title)
  })

  it('renders the ready-to-paste snippet', () => {
    expect(mountCredentials().text()).toContain(
      `sc://kyuubi.example.com:15002/;use_ssl=true;token=${SESSION.token}`
    )
  })

  it('shows the advertised connect URL verbatim', () => {
    // A wrong advertised host has to be visible, so the raw value is rendered rather than fixed up.
    expect(mountCredentials().text()).toContain(SESSION.connectUrl)
  })

  it('never writes the token to web storage', () => {
    mountCredentials()

    expect(JSON.stringify(localStorage)).not.toContain(SESSION.token)
    expect(JSON.stringify(sessionStorage)).not.toContain(SESSION.token)
  })

  it('emits the token for copying rather than storing it anywhere', async () => {
    const wrapper = mountCredentials()
    const copyButtons = wrapper.findAll('button')
    await copyButtons[copyButtons.length - 1].trigger('click')

    expect(wrapper.emitted('copy')).toBeTruthy()
  })
})

describe('spark_connect locales', () => {
  it('carries every key in both locales the repo ships', () => {
    expect(Object.keys(zh_CN.spark_connect).sort()).toEqual(
      Object.keys(en_US.spark_connect).sort()
    )
  })
})
