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
import { beforeEach, describe, expect, test } from 'vitest'
import {
  PERSISTED_PATHS,
  serializeAuthState,
  useAuthStore
} from '@/pinia/auth/auth'

beforeEach(() => {
  setActivePinia(createPinia())
  localStorage.clear()
})

describe('persisted auth state', () => {
  /**
   * Emulates the persistence plugin: it narrows state to `paths`, then hands the
   * result to the serializer.
   */
  function persist(state: Record<string, unknown>): string {
    const picked = Object.fromEntries(
      PERSISTED_PATHS.map((key) => [key, state[key]])
    )
    return serializeAuthState(picked)
  }

  test('tokens are outside the persisted paths entirely', () => {
    expect(PERSISTED_PATHS).not.toContain('refreshToken')
    expect(PERSISTED_PATHS).not.toContain('idToken')
    expect(PERSISTED_PATHS).not.toContain('expiresAt')
  })

  /*
   * The one place an OIDC access token could escape memory into storage readable
   * by any script on the origin. Losing this guard would be silent, so assert it.
   */
  test('the bearer token is stripped in oidc mode', () => {
    const written = persist({
      user: 'alice',
      authToken: 'Bearer super-secret-access-token',
      isAuthenticated: true,
      mode: 'oidc',
      refreshToken: 'super-secret-refresh-token',
      idToken: 'an-id-token'
    })

    expect(written).not.toContain('super-secret-access-token')
    expect(written).not.toContain('super-secret-refresh-token')
    expect(written).not.toContain('an-id-token')

    const parsed = JSON.parse(written)
    expect(parsed.authToken).toBeNull()
    // Forces a fresh redirect on reload rather than trusting a token we dropped.
    expect(parsed.isAuthenticated).toBe(false)
    // ...but remember it was SSO, so the user is not shown a password prompt.
    expect(parsed.mode).toBe('oidc')
    expect(parsed.user).toBe('alice')
  })

  test('basic mode keeps its header, which is all it has', () => {
    const written = persist({
      user: 'bob',
      authToken: `Basic ${btoa('bob:pw')}`,
      isAuthenticated: true,
      mode: 'basic'
    })
    const parsed = JSON.parse(written)
    expect(parsed.authToken).toContain('Basic ')
    expect(parsed.isAuthenticated).toBe(true)
  })
})

describe('token refresh', () => {
  test('is a no-op for basic auth', async () => {
    const store = useAuthStore()
    store.mode = 'basic'
    store.isAuthenticated = true
    store.authToken = 'Basic abc'
    await store.ensureFreshToken()
    expect(store.authToken).toBe('Basic abc')
  })

  test('leaves a token alone while it still has time', async () => {
    const store = useAuthStore()
    store.configureOidc({
      issuer: 'https://sso.example.com/realms/main',
      clientId: 'c',
      scopes: 'openid'
    })
    store.applyTokens({
      accessToken: 'still-valid',
      refreshToken: 'rt',
      expiresAt: Date.now() + 300_000
    })
    await store.ensureFreshToken()
    expect(store.authToken).toBe('Bearer still-valid')
  })

  test('signs the user out when an expired token has nothing to renew with', async () => {
    const store = useAuthStore()
    store.configureOidc({
      issuer: 'https://sso.example.com/realms/main',
      clientId: 'c',
      scopes: 'openid'
    })
    store.applyTokens({
      accessToken: 'expired',
      expiresAt: Date.now() - 1000
    })
    // applyTokens left refreshToken null, so renewal is impossible.
    await store.ensureFreshToken()
    expect(store.isAuthenticated).toBe(false)
    expect(store.authToken).toBeNull()
  })
})
