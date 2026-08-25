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

    <SessionCard
      v-if="session"
      :session="session"
      @copy="copy"
      @refresh="loadSession"
      @close="closeSession" />

    <CreateSessionCard
      v-else
      :creating="creating"
      :loading="loadingSession"
      @create="createSession" />
  </main>
</template>

<script lang="ts">
  export default { name: 'SparkConnect' }
</script>

<script lang="ts" setup>
  import { onMounted, ref, shallowRef } from 'vue'
  import { useI18n } from 'vue-i18n'
  import { ElMessage } from 'element-plus'
  import {
    closeSparkConnectSession,
    listSparkConnectSessions,
    openSparkConnectSession,
    type SparkConnectSessionData
  } from '@/api/spark-connect'
  import CreateSessionCard from './components/CreateSessionCard.vue'
  import SessionCard from './components/SessionCard.vue'
  import { copyToClipboard } from './utils/clipboard'

  const { t } = useI18n()

  // A user has one Spark Connect session, so this is a session rather than a list. The server
  // enforces that; the page only has to render whichever one comes back.
  const session = shallowRef<SparkConnectSessionData | null>(null)
  const loadingSession = ref(false)
  const creating = ref(false)

  function errorMessage(error: unknown): string {
    return error instanceof Error && error.message
      ? error.message
      : t('spark_connect.unknown_error')
  }

  async function loadSession() {
    loadingSession.value = true
    try {
      const result = await listSparkConnectSessions()
      session.value =
        Array.isArray(result) && result.length > 0 ? result[0] : null
    } catch (error) {
      ElMessage.error(
        t('spark_connect.list_failed', { message: errorMessage(error) })
      )
    } finally {
      loadingSession.value = false
    }
  }

  async function createSession(configs: Record<string, string>) {
    creating.value = true
    try {
      await openSparkConnectSession(configs)
      ElMessage.success(t('spark_connect.create_succeeded'))
    } catch (error) {
      ElMessage.error(
        t('spark_connect.create_failed', { message: errorMessage(error) })
      )
    } finally {
      creating.value = false
      // Reloaded rather than filled in from the create response, because that response carries no
      // state or engine: those arrive once the engine reports in.
      await loadSession()
    }
  }

  async function closeSession(sessionId: string) {
    try {
      await closeSparkConnectSession(sessionId)
      ElMessage.success(t('spark_connect.close_succeeded'))
    } catch (error) {
      ElMessage.error(
        t('spark_connect.close_failed', { message: errorMessage(error) })
      )
    } finally {
      await loadSession()
    }
  }

  async function copy(text: string) {
    if (await copyToClipboard(text)) {
      ElMessage.success({ message: t('spark_connect.copied'), duration: 1500 })
    } else {
      ElMessage.warning(t('spark_connect.copy_failed'))
    }
  }

  onMounted(loadSession)
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
