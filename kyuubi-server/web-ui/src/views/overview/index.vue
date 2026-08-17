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
  <main v-loading="loading" class="overview">
    <header class="overview-head">
      <p class="nx1-eyebrow">{{ $t('overview.eyebrow') }}</p>
      <h2>{{ $t('overview.title') }}</h2>
      <p class="overview-sub">{{ $t('overview.subtitle') }}</p>
    </header>

    <p v-if="loadError" class="overview-error">{{ loadError }}</p>

    <template v-else>
      <div class="nx1-section-header">
        <span class="nx1-section-label">{{ $t('overview.activity') }}</span>
      </div>
      <summary-bar :items="activity" />

      <div class="nx1-section-header">
        <span class="nx1-section-label">{{ $t('overview.cluster') }}</span>
      </div>
      <el-row :gutter="16">
        <el-col
          v-for="tile in tiles"
          :key="tile.label"
          :xs="24"
          :md="12"
          :xl="8">
          <div class="tile" :class="`is-${tile.accent}`">
            <p class="tile-label">{{ tile.label }}</p>
            <p class="tile-value" :title="String(tile.value)">{{
              tile.value
            }}</p>
          </div>
        </el-col>
      </el-row>
    </template>
  </main>
</template>

<script lang="ts" setup>
  import { computed, onMounted, ref } from 'vue'
  import { useI18n } from 'vue-i18n'
  import { getAllServer } from '@/api/server'
  import { getAllSessions } from '@/api/session'
  import { getAllOperations } from '@/api/operation'
  import { getAllEngines } from '@/api/engine'
  import SummaryBar, { SummaryItem } from '@/components/summary-bar/index.vue'

  const { t } = useI18n()

  const loading = ref(true)
  const loadError = ref('')
  const servers = ref<any[]>([])
  const sessions = ref<any[]>([])
  const operations = ref<any[]>([])
  const engines = ref<any[]>([])

  const asArray = (value: any): any[] => (Array.isArray(value) ? value : [])

  const TERMINAL_OPERATION_STATES = new Set([
    'FINISHED_STATE',
    'CLOSED_STATE',
    'CANCELED_STATE',
    'TIMEOUT_STATE',
    'ERROR_STATE'
  ])

  // Each endpoint is settled independently: a single unreachable admin endpoint
  // should degrade one tile, not blank the whole page.
  const getList = async () => {
    loading.value = true
    const [serverRes, sessionRes, operationRes, engineRes] =
      await Promise.allSettled([
        getAllServer(),
        getAllSessions(),
        getAllOperations(),
        getAllEngines({} as any)
      ])

    servers.value =
      serverRes.status === 'fulfilled' ? asArray(serverRes.value) : []
    sessions.value =
      sessionRes.status === 'fulfilled' ? asArray(sessionRes.value) : []
    operations.value =
      operationRes.status === 'fulfilled' ? asArray(operationRes.value) : []
    engines.value =
      engineRes.status === 'fulfilled' ? asArray(engineRes.value) : []

    const allFailed = [serverRes, sessionRes, operationRes, engineRes].every(
      (r) => r.status === 'rejected'
    )
    loadError.value = allFailed ? t('overview.unavailable') : ''
    loading.value = false
  }

  const activity = computed<SummaryItem[]>(() => {
    const running = operations.value.filter(
      (o) => !TERMINAL_OPERATION_STATES.has(o?.state)
    ).length
    const failed = operations.value.filter(
      (o) => o?.state === 'ERROR_STATE'
    ).length
    return [
      {
        label: t('overview.servers'),
        value: servers.value.length,
        type: 'primary'
      },
      {
        label: t('overview.engines'),
        value: engines.value.length,
        type: 'info'
      },
      {
        label: t('overview.sessions'),
        value: sessions.value.length,
        type: 'success'
      },
      { label: t('overview.running'), value: running, type: 'primary' },
      { label: t('overview.failed'), value: failed, type: 'danger' }
    ]
  })

  const tiles = computed(() => {
    const uniq = (values: any[]) =>
      Array.from(new Set(values.filter((v) => v != null && v !== '')))

    const engineTypes = uniq(engines.value.map((e) => e?.engineType))
    const users = uniq(sessions.value.map((s) => s?.user))
    const instances = uniq(sessions.value.map((s) => s?.kyuubiInstance))
    const dash = '—'

    return [
      {
        label: t('overview.version'),
        value: version || dash,
        accent: 'sky'
      },
      {
        label: t('overview.engine_types'),
        value: engineTypes.length ? engineTypes.join(', ') : dash,
        accent: 'violet'
      },
      {
        label: t('overview.active_users'),
        value: users.length || dash,
        accent: 'moss'
      },
      {
        label: t('overview.instances'),
        value: instances.length || servers.value.length || dash,
        accent: 'teal'
      }
    ]
  })

  const version = import.meta.env.VITE_APP_VERSION

  onMounted(getList)
</script>

<style scoped lang="scss">
  .overview-head {
    margin-bottom: 28px;

    h2 {
      margin: 6px 0 4px;
    }
  }

  .overview-sub {
    margin: 0;
    max-width: 46ch;
    color: var(--nx1-text-muted);
  }

  .overview-error {
    color: var(--nx1-text-muted);
    font-size: 14px;
  }

  /*
   * Accent-stripe tiles: a 6px left edge in the section hue, growing on hover.
   * The value stays in ink -- the stripe is what carries the category.
   */
  .tile {
    position: relative;
    overflow: hidden;
    margin-bottom: 16px;
    padding: 16px 20px 16px 22px;
    border: 1px solid var(--nx1-border);
    border-radius: var(--nx1-radius);
    background: var(--nx1-card);
    box-shadow: var(--nx1-shadow-1);

    &::before {
      content: '';
      position: absolute;
      top: 0;
      left: 0;
      bottom: 0;
      width: 6px;
      background: var(--nx1-stripe, var(--nx1-border));
      transition: width 0.15s ease;
    }

    &:hover::before {
      width: 10px;
    }
  }

  .tile-label {
    margin: 0 0 4px;
    font-family: var(--nx1-font-mono);
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: var(--nx1-text-muted);
  }

  .tile-value {
    margin: 0;
    font-family: var(--nx1-font-display);
    font-size: 24px;
    line-height: 1.25;
    color: var(--nx1-text);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .is-sky {
    --nx1-stripe: var(--nx1-sky);
  }
  .is-moss {
    --nx1-stripe: var(--nx1-moss);
  }
  .is-violet {
    --nx1-stripe: var(--nx1-violet);
  }
  .is-teal {
    --nx1-stripe: var(--nx1-teal);
  }
</style>
