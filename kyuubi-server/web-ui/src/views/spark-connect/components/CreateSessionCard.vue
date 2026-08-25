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
  <el-card class="create-card">
    <template #header>
      <span class="nx1-section-label">{{
        $t('spark_connect.new_session')
      }}</span>
    </template>

    <p class="conf-hint">{{ $t('spark_connect.conf_hint') }}</p>

    <div v-for="(entry, index) in entries" :key="index" class="conf-row">
      <el-input
        v-model="entry.key"
        class="conf-key"
        :placeholder="$t('spark_connect.conf_key_placeholder')" />
      <el-input
        v-model="entry.value"
        class="conf-value"
        :placeholder="$t('spark_connect.conf_value_placeholder')" />
      <el-button
        icon="Delete"
        circle
        :aria-label="$t('spark_connect.remove_conf')"
        @click="removeEntry(index)" />
    </div>

    <el-button icon="Plus" text @click="addEntry">
      {{ $t('spark_connect.add_conf') }}
    </el-button>

    <div class="create-actions">
      <el-button type="primary" :loading="creating" @click="emitCreate">
        {{ $t('spark_connect.create_session') }}
      </el-button>
      <span class="create-note">{{ $t('spark_connect.create_note') }}</span>
    </div>
  </el-card>
</template>

<script lang="ts" setup>
  import { ref } from 'vue'

  interface ConfEntry {
    key: string
    value: string
  }

  defineProps<{ creating: boolean }>()
  const emit = defineEmits<{
    create: [configs: Record<string, string>]
  }>()

  const entries = ref<ConfEntry[]>([])

  function addEntry() {
    entries.value.push({ key: '', value: '' })
  }

  function removeEntry(index: number) {
    entries.value.splice(index, 1)
  }

  function emitCreate() {
    const configs: Record<string, string> = {}
    for (const entry of entries.value) {
      const key = entry.key.trim()
      // A blank row is a half-finished edit, not a request to set an empty key.
      if (key) configs[key] = entry.value
    }
    emit('create', configs)
  }
</script>

<style lang="scss" scoped>
  .conf-hint {
    margin: 0 0 12px;
    font-size: 13px;
    color: var(--el-text-color-secondary);
  }
  .conf-row {
    display: flex;
    gap: 8px;
    margin-bottom: 8px;
  }
  .conf-key {
    flex: 1 1 45%;
  }
  .conf-value {
    flex: 1 1 55%;
  }
  .create-actions {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-top: 16px;
    flex-wrap: wrap;
  }
  .create-note {
    font-size: 12px;
    color: var(--el-text-color-secondary);
  }
</style>
