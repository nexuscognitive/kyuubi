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

import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'
import {
  completeLogin,
  discover,
  refresh,
  silentAuthExhausted
} from '@/utils/oidc'

const settings = {
  issuer: 'https://sso.example.com/realms/main',
  clientId: 'kyuubi-web-ui',
  scopes: 'openid profile email'
}

const metadata = {
  authorization_endpoint: 'https://sso.example.com/auth',
  token_endpoint: 'https://sso.example.com/token',
  end_session_endpoint: 'https://sso.example.com/logout'
}

function jsonResponse(body: unknown, ok = true, status = 200) {
  return {
    ok,
    status,
    json: async () => body
  } as unknown as Response
}

beforeEach(() => {
  sessionStorage.clear()
  vi.restoreAllMocks()
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('discovery', () => {
  test('reads endpoints from the well-known document', async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(metadata))
    vi.stubGlobal('fetch', fetchMock)

    // A distinct issuer per test avoids the module-level metadata cache.
    const result = await discover('https://sso.example.com/realms/discovery')
    expect(result.token_endpoint).toBe(metadata.token_endpoint)
    expect(fetchMock.mock.calls[0][0]).toBe(
      'https://sso.example.com/realms/discovery/.well-known/openid-configuration'
    )
  })
})

describe('completeLogin', () => {
  test('rejects a callback whose state does not match', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(metadata)))
    sessionStorage.setItem('kyuubi.oidc.state', 'expected-state')
    sessionStorage.setItem('kyuubi.oidc.verifier', 'a-verifier')

    await expect(
      completeLogin(settings, '?code=abc&state=attacker-state')
    ).rejects.toThrow(/state mismatch/i)
  })

  test('rejects a callback with no stored state at all', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(metadata)))

    await expect(
      completeLogin(settings, '?code=abc&state=whatever')
    ).rejects.toThrow(/state mismatch/i)
  })

  test('surfaces a provider error response', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(metadata)))

    await expect(
      completeLogin(settings, '?error=access_denied&error_description=Nope')
    ).rejects.toThrow('Nope')
  })

  test('clears single-use state even when the exchange is rejected', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(metadata)))
    sessionStorage.setItem('kyuubi.oidc.state', 'expected-state')
    sessionStorage.setItem('kyuubi.oidc.verifier', 'a-verifier')

    await expect(
      completeLogin(settings, '?code=abc&state=wrong')
    ).rejects.toThrow()

    // Left behind, a replayed callback could be processed a second time.
    expect(sessionStorage.getItem('kyuubi.oidc.state')).toBeNull()
    expect(sessionStorage.getItem('kyuubi.oidc.verifier')).toBeNull()
  })

  test('exchanges the code with the PKCE verifier and returns the token set', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse(metadata))
      .mockResolvedValueOnce(
        jsonResponse({
          access_token: 'at',
          refresh_token: 'rt',
          expires_in: 300
        })
      )
    vi.stubGlobal('fetch', fetchMock)
    sessionStorage.setItem('kyuubi.oidc.state', 'st')
    sessionStorage.setItem('kyuubi.oidc.verifier', 'ver')
    sessionStorage.setItem('kyuubi.oidc.returnTo', '/ui/management/session')

    const { tokens, returnTo } = await completeLogin(
      { ...settings, issuer: 'https://sso.example.com/realms/exchange' },
      '?code=the-code&state=st'
    )

    expect(tokens.accessToken).toBe('at')
    expect(tokens.refreshToken).toBe('rt')
    expect(tokens.expiresAt).toBeGreaterThan(Date.now())
    expect(returnTo).toBe('/ui/management/session')

    const body = String(fetchMock.mock.calls[1][1].body)
    expect(body).toContain('code_verifier=ver')
    expect(body).toContain('grant_type=authorization_code')
    // A public client must not be sending a secret.
    expect(body).not.toContain('client_secret')
  })
})

describe('silent re-authentication', () => {
  /*
   * A reload always starts unauthenticated because the token is memory-only, so
   * the app retries with prompt=none. When there is no live provider session that
   * comes back as login_required -- which must be treated as "now prompt", not as
   * an error, and must not be retried silently or the page ping-pongs between app
   * and provider forever.
   */
  test('a silent attempt that needs interaction is typed, not a hard error', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(metadata)))
    sessionStorage.setItem('kyuubi.oidc.silent', '1')

    await expect(
      completeLogin(settings, '?error=login_required')
    ).rejects.toThrow(
      expect.objectContaining({ name: 'InteractionRequiredError' })
    )
    // The flag is what stops the loop.
    expect(silentAuthExhausted()).toBe(true)
  })

  test('the same error from an interactive attempt stays a hard error', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(metadata)))
    sessionStorage.setItem('kyuubi.oidc.silent', '0')

    await expect(
      completeLogin(settings, '?error=login_required')
    ).rejects.toThrow(
      expect.not.objectContaining({ name: 'InteractionRequiredError' })
    )
    expect(silentAuthExhausted()).toBe(false)
  })

  test('a completed sign-in makes silent auth viable again', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse(metadata))
      .mockResolvedValueOnce(
        jsonResponse({ access_token: 'at', expires_in: 300 })
      )
    vi.stubGlobal('fetch', fetchMock)
    sessionStorage.setItem('kyuubi.oidc.silentFailed', '1')
    sessionStorage.setItem('kyuubi.oidc.state', 'st')
    sessionStorage.setItem('kyuubi.oidc.verifier', 'ver')

    await completeLogin(
      { ...settings, issuer: 'https://sso.example.com/realms/again' },
      '?code=c&state=st'
    )
    expect(silentAuthExhausted()).toBe(false)
  })
})

describe('refresh', () => {
  test('keeps the existing refresh token when the provider omits it', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse(metadata))
      .mockResolvedValueOnce(
        jsonResponse({ access_token: 'at2', expires_in: 300 })
      )
    vi.stubGlobal('fetch', fetchMock)

    const tokens = await refresh(
      { ...settings, issuer: 'https://sso.example.com/realms/refresh' },
      'original-rt'
    )
    expect(tokens.accessToken).toBe('at2')
    expect(tokens.refreshToken).toBe('original-rt')
  })
})
