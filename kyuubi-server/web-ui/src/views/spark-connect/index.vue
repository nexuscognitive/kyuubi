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
  <main class="spark-connect">
    <header class="page-head">
      <p class="nx1-eyebrow">{{ $t('spark_connect.eyebrow') }}</p>
      <h2>{{ $t('spark_connect.title') }}</h2>
      <p class="page-sub">{{ $t('spark_connect.subtitle') }}</p>
    </header>

    <SessionCredentials
      v-if="createdSession"
      :session="createdSession"
      @copy="copy"
      @dismiss="forgetCreatedSession" />

    <CreateSessionCard :creating="creating" @create="createSession" />

    <SessionsTable
      :sessions="sessions"
      :loading="loadingSessions"
      @refresh="loadSessions"
      @close="closeSession" />
  </main>
</template>

<script lang="ts">
  export default { name: 'SparkConnect' }
</script>

<script lang="ts" setup>
  import { onMounted, onUnmounted, ref, shallowRef } from 'vue'
  import { useI18n } from 'vue-i18n'
  import { ElMessage } from 'element-plus'
  import {
    closeSparkConnectSession,
    listSparkConnectSessions,
    openSparkConnectSession,
    type SparkConnectSession,
    type SparkConnectSessionData
  } from '@/api/spark-connect'
  import CreateSessionCard from './components/CreateSessionCard.vue'
  import SessionCredentials from './components/SessionCredentials.vue'
  import SessionsTable from './components/SessionsTable.vue'
  import { copyToClipboard } from './utils/clipboard'

  const { t } = useI18n()

  const sessions = ref<SparkConnectSessionData[]>([])
  const loadingSessions = ref(false)
  const creating = ref(false)

  // The freshly minted session, token and all. It lives in a component ref and nowhere else: not
  // in a store, not in localStorage or sessionStorage, and never logged. Cleared when the user
  // dismisses the panel and when the page unmounts, so navigating away drops the credential.
  const createdSession = shallowRef<SparkConnectSession | null>(null)

  function forgetCreatedSession() {
    createdSession.value = null
  }

  function errorMessage(error: unknown): string {
    return error instanceof Error && error.message
      ? error.message
      : t('spark_connect.unknown_error')
  }

  async function loadSessions() {
    loadingSessions.value = true
    try {
      const result = await listSparkConnectSessions()
      sessions.value = Array.isArray(result) ? result : []
    } catch (error) {
      ElMessage.error(
        t('spark_connect.list_failed', { message: errorMessage(error) })
      )
    } finally {
      loadingSessions.value = false
    }
  }

  async function createSession(configs: Record<string, string>) {
    creating.value = true
    try {
      createdSession.value = await openSparkConnectSession(configs)
      ElMessage.success(t('spark_connect.create_succeeded'))
      await loadSessions()
    } catch (error) {
      ElMessage.error(
        t('spark_connect.create_failed', { message: errorMessage(error) })
      )
    } finally {
      creating.value = false
    }
  }

  async function closeSession(sessionId: string) {
    try {
      await closeSparkConnectSession(sessionId)
      // Closing the session the panel is showing makes its token dead; stop displaying it.
      if (createdSession.value?.sessionId === sessionId) forgetCreatedSession()
      ElMessage.success(t('spark_connect.close_succeeded'))
    } catch (error) {
      ElMessage.error(
        t('spark_connect.close_failed', { message: errorMessage(error) })
      )
    } finally {
      await loadSessions()
    }
  }

  async function copy(text: string) {
    if (await copyToClipboard(text)) {
      ElMessage.success({ message: t('spark_connect.copied'), duration: 1500 })
    } else {
      ElMessage.warning(t('spark_connect.copy_failed'))
    }
  }

  onMounted(loadSessions)
  onUnmounted(forgetCreatedSession)
</script>

<style lang="scss" scoped>
  .spark-connect {
    display: flex;
    flex-direction: column;
    gap: 16px;
  }
  .page-head {
    h2 {
      margin: 4px 0 0;
    }
  }
  .page-sub {
    margin: 6px 0 0;
    color: var(--el-text-color-secondary);
  }
</style>
