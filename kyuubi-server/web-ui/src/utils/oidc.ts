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

/*
 * OIDC authorization code flow with PKCE (RFC 7636).
 *
 * The Web UI is a *public* client: it ships to the browser and therefore holds no
 * client secret, which is why PKCE rather than a client credential is what binds
 * the authorization code to this browser session.
 *
 * Deliberately implemented against the Web Crypto API rather than pulling in an
 * OIDC library. The browser's job here is only to obtain a token and present it;
 * it never validates a token signature -- the Kyuubi server does that against the
 * provider's JWKS. That keeps this to standard protocol plumbing.
 *
 * Token storage: the access token is held in memory only. `sessionStorage` holds
 * just the short-lived PKCE verifier and state between the redirect and the
 * callback. Nothing durable is written, so a token cannot be lifted out of
 * localStorage the way the Basic-auth credentials previously could.
 */

export interface OidcSettings {
  issuer: string
  clientId: string
  scopes: string
}

interface ProviderMetadata {
  authorization_endpoint: string
  token_endpoint: string
  end_session_endpoint?: string
}

export interface TokenSet {
  accessToken: string
  refreshToken?: string
  idToken?: string
  /** Epoch milliseconds at which the access token expires. */
  expiresAt: number
  username?: string
}

const VERIFIER_KEY = 'kyuubi.oidc.verifier'
const STATE_KEY = 'kyuubi.oidc.state'
const RETURN_TO_KEY = 'kyuubi.oidc.returnTo'
/** Whether the in-flight attempt used prompt=none. */
const SILENT_KEY = 'kyuubi.oidc.silent'
/** Set once a silent attempt has come back needing real interaction. */
const SILENT_FAILED_KEY = 'kyuubi.oidc.silentFailed'

/**
 * Thrown when a `prompt=none` attempt needs the user to interact after all --
 * their provider session has expired, or consent is outstanding.
 */
export class InteractionRequiredError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'InteractionRequiredError'
  }
}

/** True once a silent attempt has failed, so we stop retrying it. */
export function silentAuthExhausted(): boolean {
  return sessionStorage.getItem(SILENT_FAILED_KEY) === '1'
}

export function resetSilentAuth(): void {
  sessionStorage.removeItem(SILENT_FAILED_KEY)
}

/** Where the provider sends the browser back to. Must be registered on the client. */
export function redirectUri(): string {
  return `${window.location.origin}/ui/auth/callback`
}

function base64UrlEncode(bytes: Uint8Array): string {
  let binary = ''
  bytes.forEach((b) => {
    binary += String.fromCharCode(b)
  })
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
}

function randomUrlSafe(byteLength: number): string {
  const bytes = new Uint8Array(byteLength)
  crypto.getRandomValues(bytes)
  return base64UrlEncode(bytes)
}

async function s256Challenge(verifier: string): Promise<string> {
  const digest = await crypto.subtle.digest(
    'SHA-256',
    new TextEncoder().encode(verifier)
  )
  return base64UrlEncode(new Uint8Array(digest))
}

let metadataCache: { issuer: string; metadata: ProviderMetadata } | null = null

export async function discover(issuer: string): Promise<ProviderMetadata> {
  if (metadataCache && metadataCache.issuer === issuer) {
    return metadataCache.metadata
  }
  const url = `${issuer.replace(/\/$/, '')}/.well-known/openid-configuration`
  const response = await fetch(url)
  if (!response.ok) {
    throw new Error(
      `OIDC discovery failed for ${issuer} (HTTP ${response.status})`
    )
  }
  const metadata = (await response.json()) as ProviderMetadata
  if (!metadata.authorization_endpoint || !metadata.token_endpoint) {
    throw new Error(`OIDC discovery for ${issuer} returned no endpoints`)
  }
  metadataCache = { issuer, metadata }
  return metadata
}

/**
 * Begin sign-in by navigating to the provider. Never resolves in practice -- the
 * document is replaced by the redirect.
 *
 * `silent` sends `prompt=none`, which the provider answers immediately from an
 * existing SSO session without showing a login screen. That is what makes a page
 * reload transparent: the access token only lives in memory, so every reload has
 * to re-acquire one, and asking the user to click through a login screen each
 * time would be indistinguishable from being logged out.
 */
export async function beginLogin(
  settings: OidcSettings,
  silent = false
): Promise<void> {
  const metadata = await discover(settings.issuer)
  const verifier = randomUrlSafe(32)
  const state = randomUrlSafe(16)
  const challenge = await s256Challenge(verifier)

  sessionStorage.setItem(VERIFIER_KEY, verifier)
  sessionStorage.setItem(STATE_KEY, state)
  sessionStorage.setItem(SILENT_KEY, silent ? '1' : '0')
  // Send the user back to whatever they were looking at, not always the home page.
  sessionStorage.setItem(
    RETURN_TO_KEY,
    window.location.pathname + window.location.search
  )

  const params = new URLSearchParams({
    response_type: 'code',
    client_id: settings.clientId,
    redirect_uri: redirectUri(),
    scope: settings.scopes,
    state,
    code_challenge: challenge,
    code_challenge_method: 'S256'
  })
  if (silent) params.set('prompt', 'none')
  window.location.assign(`${metadata.authorization_endpoint}?${params}`)
}

function decodeUsername(idToken?: string): string | undefined {
  // Read the display name only. This is NOT verification -- the server validates
  // the token against the provider's JWKS. Never trust these claims for access
  // decisions.
  if (!idToken) return undefined
  const parts = idToken.split('.')
  if (parts.length < 2) return undefined
  try {
    const payload = JSON.parse(
      atob(parts[1].replace(/-/g, '+').replace(/_/g, '/'))
    )
    return payload.preferred_username || payload.email || payload.sub
  } catch {
    return undefined
  }
}

function toTokenSet(payload: any): TokenSet {
  const expiresInSeconds = Number(payload.expires_in) || 300
  return {
    accessToken: payload.access_token,
    refreshToken: payload.refresh_token,
    idToken: payload.id_token,
    expiresAt: Date.now() + expiresInSeconds * 1000,
    username: decodeUsername(payload.id_token)
  }
}

/**
 * Complete sign-in from the provider's redirect. Returns the token set and the
 * path the user started from.
 */
export async function completeLogin(
  settings: OidcSettings,
  search: string
): Promise<{ tokens: TokenSet; returnTo: string }> {
  const params = new URLSearchParams(search)
  const wasSilent = sessionStorage.getItem(SILENT_KEY) === '1'
  const error = params.get('error')
  if (error) {
    // A silent attempt legitimately fails when there is no live provider session.
    // Record that so the app stops retrying silently and prompts instead --
    // without this the reload would bounce between app and provider forever.
    if (
      wasSilent &&
      ['login_required', 'interaction_required', 'consent_required'].includes(
        error
      )
    ) {
      sessionStorage.setItem(SILENT_FAILED_KEY, '1')
      sessionStorage.removeItem(SILENT_KEY)
      throw new InteractionRequiredError(error)
    }
    throw new Error(params.get('error_description') || error)
  }

  const code = params.get('code')
  const state = params.get('state')
  const expectedState = sessionStorage.getItem(STATE_KEY)
  const verifier = sessionStorage.getItem(VERIFIER_KEY)
  const returnTo = sessionStorage.getItem(RETURN_TO_KEY) || '/'

  // Clear first: these are single-use, and leaving them behind would let a
  // replayed callback be processed twice.
  sessionStorage.removeItem(STATE_KEY)
  sessionStorage.removeItem(VERIFIER_KEY)
  sessionStorage.removeItem(RETURN_TO_KEY)
  sessionStorage.removeItem(SILENT_KEY)

  if (!code) throw new Error('Authorization response contained no code')
  // The state check is what rejects a callback this browser did not initiate.
  if (!state || !expectedState || state !== expectedState) {
    throw new Error('Authorization state mismatch; sign-in was not completed')
  }
  if (!verifier) throw new Error('Missing PKCE verifier; sign-in was restarted')

  const metadata = await discover(settings.issuer)
  const body = new URLSearchParams({
    grant_type: 'authorization_code',
    code,
    redirect_uri: redirectUri(),
    client_id: settings.clientId,
    code_verifier: verifier
  })
  const response = await fetch(metadata.token_endpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body
  })
  if (!response.ok) {
    throw new Error(`Token exchange failed (HTTP ${response.status})`)
  }
  // A completed sign-in means silent auth is viable again next reload.
  sessionStorage.removeItem(SILENT_FAILED_KEY)
  return { tokens: toTokenSet(await response.json()), returnTo }
}

export async function refresh(
  settings: OidcSettings,
  refreshToken: string
): Promise<TokenSet> {
  const metadata = await discover(settings.issuer)
  const body = new URLSearchParams({
    grant_type: 'refresh_token',
    refresh_token: refreshToken,
    client_id: settings.clientId
  })
  const response = await fetch(metadata.token_endpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body
  })
  if (!response.ok) {
    throw new Error(`Token refresh failed (HTTP ${response.status})`)
  }
  const tokens = toTokenSet(await response.json())
  // Keycloak omits refresh_token on refresh when rotation is off; keep the old one.
  if (!tokens.refreshToken) tokens.refreshToken = refreshToken
  return tokens
}

/** Redirect to the provider's end-session endpoint, if it advertises one. */
export async function endSession(
  settings: OidcSettings,
  idToken?: string
): Promise<boolean> {
  const metadata = await discover(settings.issuer)
  if (!metadata.end_session_endpoint) return false
  const params = new URLSearchParams({
    post_logout_redirect_uri: `${window.location.origin}/ui`,
    client_id: settings.clientId
  })
  if (idToken) params.set('id_token_hint', idToken)
  window.location.assign(`${metadata.end_session_endpoint}?${params}`)
  return true
}
