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

const beginLogin = vi.fn()
vi.mock('@/utils/oidc', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/utils/oidc')>()),
  beginLogin: (...args: unknown[]) => beginLogin(...args)
}))

const oidcConfig = {
  engineUIProxyEnabled: true,
  oidcEnabled: true,
  oidcIssuer: 'https://sso.example.com/realms/main',
  oidcClientId: 'kyuubi-web-ui',
  oidcScopes: 'openid profile email'
}

/** Mark silent auth as already exhausted, so the interactive prompt is shown. */
function silentAlreadyFailed() {
  sessionStorage.setItem('kyuubi.oidc.silentFailed', '1')
}

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
  sessionStorage.clear()
  getWebUIConfig.mockReset()
  beginLogin.mockReset()
  beginLogin.mockResolvedValue(undefined)
})

describe('login dialog', () => {
  /*
   * The access token is memory-only, so a reload always begins signed out. The
   * app must first retry with prompt=none rather than showing a login screen --
   * otherwise every refresh looks like a logout, which is the bug this covers.
   */
  test('retries silently instead of prompting on a fresh load', async () => {
    getWebUIConfig.mockResolvedValue(oidcConfig)

    const wrapper = await mountLogin()

    expect(beginLogin).toHaveBeenCalled()
    // second arg is `silent`
    expect(beginLogin.mock.calls[0][1]).toBe(true)
    expect(wrapper.text()).not.toContain('Continue with SSO')
  })

  /*
   * Regression: the modal renders at the app root, so it also mounts on the OIDC
   * callback page. There it sees an unauthenticated session and used to fire a
   * silent authorization request, which overwrote the single-use state/verifier
   * that the callback was about to exchange -- the sign-in then failed with
   * "Authorization state mismatch". It must stay out of the way entirely.
   */
  test('does not start a new authorization request on the callback page', async () => {
    getWebUIConfig.mockResolvedValue(oidcConfig)
    const original = window.location.pathname
    window.history.replaceState({}, '', '/ui/auth/callback?code=abc&state=xyz')
    try {
      await mountLogin()
      expect(beginLogin).not.toHaveBeenCalled()
    } finally {
      window.history.replaceState({}, '', original)
    }
  })

  test('offers SSO once a silent attempt has come back needing interaction', async () => {
    getWebUIConfig.mockResolvedValue(oidcConfig)
    silentAlreadyFailed()

    const wrapper = await mountLogin()

    // No second silent attempt: that is what would loop.
    expect(beginLogin).not.toHaveBeenCalled()
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
