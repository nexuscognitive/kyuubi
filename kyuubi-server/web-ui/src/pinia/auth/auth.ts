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

import { defineStore } from 'pinia'
import request from '@/utils/request'
import {
  beginLogin,
  completeLogin,
  endSession,
  refresh,
  type OidcSettings,
  type TokenSet
} from '@/utils/oidc'

/** Refresh this long before expiry so an in-flight request never races it. */
const REFRESH_SKEW_MS = 30_000

/** State keys the persistence plugin is allowed to write. */
export const PERSISTED_PATHS = ['user', 'authToken', 'isAuthenticated', 'mode']

/**
 * Strip the bearer token on the way to storage.
 *
 * In OIDC mode `authToken` holds an access token; persisting it would make it
 * readable by any script on the origin and outlive the tab, which is exactly what
 * the redirect flow exists to avoid. Refresh and id tokens are not in
 * [[PERSISTED_PATHS]] at all, so they never reach this point.
 *
 * `mode` and `user` survive, so a reload knows to re-run the authorization
 * redirect -- silent while the provider session is live -- rather than dropping
 * the user at a password prompt.
 *
 * Exported so this is directly testable: a regression here would leak tokens
 * silently.
 */
export function serializeAuthState(state: Record<string, unknown>): string {
  const persisted =
    state.mode === 'oidc'
      ? { ...state, authToken: null, isAuthenticated: false }
      : state
  return JSON.stringify(persisted)
}

export const useAuthStore = defineStore('auth', {
  state: () => ({
    user: null as string | null,
    authToken: null as string | null,
    isAuthenticated: false,
    mode: 'basic' as 'basic' | 'oidc',
    oidc: null as OidcSettings | null,
    // OIDC tokens are deliberately NOT persisted -- see `persist` below.
    refreshToken: null as string | null,
    idToken: null as string | null,
    expiresAt: 0
  }),
  actions: {
    configureOidc(settings: OidcSettings) {
      this.mode = 'oidc'
      this.oidc = settings
    },

    /** Basic auth: verify the credentials, then keep the encoded header. */
    async setUser(user: string, password: string) {
      const response = await request({
        url: 'api/v1/ping',
        method: 'get',
        auth: {
          username: user,
          password: password
        }
      })

      if (response) {
        this.user = user
        this.authToken = `Basic ${btoa(user + ':' + password)}`
        this.mode = 'basic'
        this.isAuthenticated = true
      } else {
        throw new Error('Authentication failed')
      }
    },

    async loginWithOidc(silent = false) {
      if (!this.oidc) throw new Error('OIDC is not configured')
      await beginLogin(this.oidc, silent)
    },

    async completeOidcLogin(search: string): Promise<string> {
      if (!this.oidc) throw new Error('OIDC is not configured')
      const { tokens, returnTo } = await completeLogin(this.oidc, search)
      this.applyTokens(tokens)
      return returnTo
    },

    applyTokens(tokens: TokenSet) {
      this.authToken = `Bearer ${tokens.accessToken}`
      this.refreshToken = tokens.refreshToken ?? null
      this.idToken = tokens.idToken ?? null
      this.expiresAt = tokens.expiresAt
      this.user = tokens.username ?? this.user
      this.mode = 'oidc'
      this.isAuthenticated = true
    },

    /**
     * Renew the access token when it is close to expiring. Called before each API
     * request; a no-op for Basic auth and for tokens with time left.
     */
    async ensureFreshToken(): Promise<void> {
      if (this.mode !== 'oidc' || !this.isAuthenticated) return
      if (Date.now() < this.expiresAt - REFRESH_SKEW_MS) return
      if (!this.oidc || !this.refreshToken) {
        // Nothing to renew with: force a fresh sign-in rather than firing a
        // request that is certain to 401.
        this.clearUser()
        window.dispatchEvent(new CustomEvent('auth-required'))
        return
      }
      try {
        this.applyTokens(await refresh(this.oidc, this.refreshToken))
      } catch {
        this.clearUser()
        window.dispatchEvent(new CustomEvent('auth-required'))
      }
    },

    async logout() {
      const settings = this.oidc
      const idToken = this.idToken ?? undefined
      const wasOidc = this.mode === 'oidc'
      this.clearUser()
      if (wasOidc && settings) {
        // Also end the session at the provider, otherwise signing back in is
        // silent and "log out" only appears to have worked.
        await endSession(settings, idToken)
      }
    },

    clearUser() {
      this.user = null
      this.authToken = null
      this.isAuthenticated = false
      this.refreshToken = null
      this.idToken = null
      this.expiresAt = 0
    }
  },
  persist: {
    key: 'auth',
    paths: PERSISTED_PATHS,
    serializer: {
      serialize: serializeAuthState,
      deserialize: (value: string) => JSON.parse(value)
    }
  }
})
