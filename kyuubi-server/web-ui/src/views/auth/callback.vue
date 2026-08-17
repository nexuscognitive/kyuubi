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
  <main class="callback">
    <p v-if="error" class="callback-error">{{ error }}</p>
    <p v-else class="callback-status">{{ $t('login.signing_in') }}</p>
  </main>
</template>

<script setup lang="ts">
  import { onMounted, ref } from 'vue'
  import { useRouter } from 'vue-router'
  import { useAuthStore } from '@/pinia/auth/auth'
  import { getWebUIConfig } from '@/api/server'

  const router = useRouter()
  const authStore = useAuthStore()
  const error = ref('')

  onMounted(async () => {
    try {
      // The store is not persisted across the provider redirect, so the OIDC
      // settings have to be fetched again before the code can be exchanged.
      const config = await getWebUIConfig()
      if (!config.oidcEnabled || !config.oidcIssuer || !config.oidcClientId) {
        throw new Error('OIDC is not configured on this server')
      }
      authStore.configureOidc({
        issuer: config.oidcIssuer,
        clientId: config.oidcClientId,
        scopes: config.oidcScopes || 'openid profile email'
      })
      const returnTo = await authStore.completeOidcLogin(window.location.search)
      // replace() so the callback (which carries the authorization code in the
      // URL) does not stay in history and cannot be re-entered with Back.
      await router.replace(
        returnTo.startsWith('/ui') ? returnTo.slice(3) : returnTo
      )
    } catch (e) {
      if ((e as Error)?.name === 'InteractionRequiredError') {
        // Expected: the silent attempt found no live provider session. Not an
        // error worth showing -- go back to the app, which now prompts.
        await router.replace('/')
        return
      }
      error.value = (e as Error)?.message || 'Sign-in failed'
    }
  })
</script>

<style scoped lang="scss">
  .callback {
    display: flex;
    align-items: center;
    justify-content: center;
    min-height: 60vh;
    font-family: var(--nx1-font-body);
  }

  .callback-status {
    color: var(--nx1-text-muted);
  }

  .callback-error {
    color: var(--nx1-danger);
    max-width: 50ch;
  }
</style>
