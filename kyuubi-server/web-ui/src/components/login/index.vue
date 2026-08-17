<!--
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
-->

<template>
  <el-dialog
    v-model="dialogVisible"
    :close-on-click-modal="false"
    width="400px">
    <div class="dialog-header">
      <img class="logo" src="@/assets/images/nx1-mark.svg" alt="Nexus One" />
      <p class="product">Spark<b>Engine</b></p>
    </div>
    <!--
      When the server advertises OIDC there is no local password to collect --
      showing the fields anyway would invite users to type their SSO credentials
      into a form that cannot verify them.
    -->
    <div v-if="oidcEnabled" class="sso">
      <p class="sso-hint">{{ $t('login.sso_hint') }}</p>
      <p v-if="loginError" class="login-error">{{ loginError }}</p>
    </div>
    <el-form v-else class="login-form">
      <el-form-item>
        <el-input v-model="username" placeholder="Username" />
      </el-form-item>
      <el-form-item>
        <el-input v-model="password" type="password" placeholder="Password" />
      </el-form-item>
      <el-form-item>
        <p v-if="loginError" class="login-error">{{ loginError }}</p>
      </el-form-item>
    </el-form>
    <template #footer>
      <div class="dialog-footer">
        <el-button
          v-if="oidcEnabled"
          type="primary"
          :loading="redirecting"
          @click="handleSsoLogin"
          >{{ $t('login.sso_button') }}</el-button
        >
        <el-button
          v-else
          type="primary"
          :disabled="isLoginDisabled"
          @click="handleLogin"
          >Log in</el-button
        >
      </div>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
  import { ref, computed, onMounted } from 'vue'
  import { useI18n } from 'vue-i18n'
  import { useAuthStore } from '@/pinia/auth/auth'
  import { getWebUIConfig } from '@/api/server'
  import { isAuthCallback, silentAuthExhausted } from '@/utils/oidc'

  const { t } = useI18n()
  const authStore = useAuthStore()
  const dialogVisible = ref(false)
  const username = ref('')
  const password = ref('')
  const loginError = ref('')
  const oidcEnabled = ref(false)
  const redirecting = ref(false)

  const isLoginDisabled = computed(() => {
    return (
      username.value.trim().length === 0 || password.value.trim().length === 0
    )
  })

  const handleLogin = async () => {
    try {
      await authStore.setUser(username.value, password.value)
      dialogVisible.value = false
      loginError.value = ''
    } catch (error) {
      const status = (error as any)?.response?.status
      if (status === 401 || status === 403) {
        loginError.value = t('login.invalid_credentials')
      } else if (typeof status === 'number' && status >= 500) {
        loginError.value = t('login.server_error')
      } else {
        loginError.value = (error as Error)?.message || t('login.failed')
      }
    }
  }

  const handleSsoLogin = async () => {
    redirecting.value = true
    try {
      await authStore.loginWithOidc()
    } catch (error) {
      // Only reached if discovery or the redirect fails; on success the document
      // is replaced by the provider.
      redirecting.value = false
      loginError.value = (error as Error)?.message || t('login.failed')
    }
  }

  /*
   * The access token is deliberately not persisted, so every reload starts
   * unauthenticated. Making the user click through a login screen each time would
   * be indistinguishable from being logged out, so first try `prompt=none`: the
   * provider answers it straight from its SSO cookie and the user sees only a
   * redirect. Only when that comes back needing interaction do we prompt, and
   * `silentAuthExhausted` makes sure that decision sticks for the tab rather than
   * bouncing between app and provider.
   */
  const handleAuthRequired = async () => {
    // Never start a new authorization request while the callback is being
    // exchanged: beginLogin would overwrite the single-use state/verifier that
    // completeLogin is about to read, and the sign-in would fail with a state
    // mismatch.
    if (isAuthCallback()) return
    loginError.value = ''
    if (oidcEnabled.value && !silentAuthExhausted()) {
      try {
        await authStore.loginWithOidc(true)
        return
      } catch {
        // Discovery failed; fall through to the interactive prompt.
      }
    }
    dialogVisible.value = true
  }

  onMounted(async () => {
    window.addEventListener('auth-required', handleAuthRequired)
    try {
      const config = await getWebUIConfig()
      if (config.oidcEnabled && config.oidcIssuer && config.oidcClientId) {
        oidcEnabled.value = true
        authStore.configureOidc({
          issuer: config.oidcIssuer,
          clientId: config.oidcClientId,
          scopes: config.oidcScopes || 'openid profile email'
        })
        // A reload lands here already signed out. If the provider session is
        // still live this restores it without the user noticing.
        if (!authStore.isAuthenticated && !silentAuthExhausted()) {
          await handleAuthRequired()
        }
      }
    } catch {
      // Leave the Basic form in place: an unreachable config endpoint should not
      // strand the user with no way to sign in at all.
      oidcEnabled.value = false
    }
  })
</script>

<style scoped>
  .dialog-header {
    display: flex;
    flex-direction: column;
    align-items: center;
    margin-bottom: 20px;
  }

  .logo {
    width: 44px;
    height: 44px;
    margin-bottom: 10px;
  }

  .product {
    margin: 0;
    font-size: 21px;
    font-weight: 300;
    letter-spacing: -0.01em;
    color: var(--nx1-text);
  }

  .product b {
    font-weight: 600;
  }

  .login-form {
    margin-bottom: 20px;
  }

  .login-error {
    color: var(--nx1-danger);
    font-size: 13px;
    margin-top: 10px;
    text-align: left;
  }

  .sso {
    text-align: center;
    padding: 4px 0 8px;
  }

  .sso-hint {
    margin: 0;
    color: var(--nx1-text-muted);
    font-size: 14px;
  }

  .dialog-footer {
    text-align: center;
    padding: 15px 20px;
  }
</style>
