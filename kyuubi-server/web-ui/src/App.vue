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

<script lang="ts" setup>
  import { computed } from 'vue'
  import { useRoute } from 'vue-router'

  const route = useRoute()

  /*
   * The login modal must not exist while the OIDC callback is being exchanged.
   * It starts a silent authorization request when it sees an unauthenticated
   * session -- which is exactly the state the callback page is in -- and that
   * request overwrites the single-use state/verifier the exchange is about to
   * read, failing the sign-in with a state mismatch.
   */
  const isAuthCallbackRoute = computed(() => route.name === 'auth-callback')
</script>

<template>
  <login-modal v-if="!isAuthCallbackRoute" />
  <router-view />
</template>

<style scoped></style>
