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

import { createPinia, setActivePinia } from 'pinia'
import { flushPromises, mount } from '@vue/test-utils'
import { beforeEach, describe, expect, test, vi } from 'vitest'
import ElementPlus from 'element-plus'
import { createI18n } from 'vue-i18n'
import Login from '@/components/login/index.vue'
import en_US from '@/locales/en_US'

const getWebUIConfig = vi.fn()
vi.mock('@/api/server', () => ({
  getWebUIConfig: (...args: unknown[]) => getWebUIConfig(...args)
}))

const i18n = createI18n({
  legacy: false,
  locale: 'en_US',
  messages: { en_US }
})

/**
 * Mount and open the dialog. It stays hidden until an `auth-required` event
 * fires, which is what a 401 from the API layer dispatches.
 */
async function mountLogin() {
  const wrapper = mount(Login, {
    global: { plugins: [ElementPlus, i18n], stubs: { teleport: true } }
  })
  await flushPromises()
  window.dispatchEvent(new CustomEvent('auth-required'))
  await flushPromises()
  return wrapper
}

beforeEach(() => {
  setActivePinia(createPinia())
  getWebUIConfig.mockReset()
})

describe('login dialog', () => {
  test('offers SSO and no password fields when the server advertises OIDC', async () => {
    getWebUIConfig.mockResolvedValue({
      engineUIProxyEnabled: true,
      oidcEnabled: true,
      oidcIssuer: 'https://sso.example.com/realms/main',
      oidcClientId: 'kyuubi-web-ui',
      oidcScopes: 'openid profile email'
    })

    const wrapper = await mountLogin()

    expect(wrapper.text()).toContain('Continue with SSO')
    // Showing a password box under SSO invites users to type their IdP
    // credentials into a form that cannot verify them.
    expect(wrapper.find('input[type="password"]').exists()).toBe(false)
  })

  test('falls back to the password form when OIDC is off', async () => {
    getWebUIConfig.mockResolvedValue({
      engineUIProxyEnabled: true,
      oidcEnabled: false
    })

    const wrapper = await mountLogin()

    expect(wrapper.text()).not.toContain('Continue with SSO')
    expect(wrapper.find('input[type="password"]').exists()).toBe(true)
  })

  test('falls back to the password form when the config call fails', async () => {
    // An unreachable config endpoint must not strand the user with no way in.
    getWebUIConfig.mockRejectedValue(new Error('network down'))

    const wrapper = await mountLogin()

    expect(wrapper.text()).not.toContain('Continue with SSO')
    expect(wrapper.find('input[type="password"]').exists()).toBe(true)
  })

  test('does not offer SSO when the server flags it but omits the client id', async () => {
    // A half-configured server would otherwise send the browser to a broken login.
    getWebUIConfig.mockResolvedValue({
      engineUIProxyEnabled: true,
      oidcEnabled: true,
      oidcIssuer: 'https://sso.example.com/realms/main'
    })

    const wrapper = await mountLogin()

    expect(wrapper.text()).not.toContain('Continue with SSO')
    expect(wrapper.find('input[type="password"]').exists()).toBe(true)
  })
})
