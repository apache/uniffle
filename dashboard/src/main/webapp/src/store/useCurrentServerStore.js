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
import { ref, watch } from 'vue'

/**
 * Create a global shared repository that allows you to share state across components/pages.
 * @type {StoreDefinition<"overall", _ExtractStateFromSetupStore<{currentServer: Ref<UnwrapRef<string>>}>, _ExtractGettersFromSetupStore<{currentServer: Ref<UnwrapRef<string>>}>, _ExtractActionsFromSetupStore<{currentServer: Ref<UnwrapRef<string>>}>>}
 */
export const useCurrentServerStore = defineStore('overall', () => {
    const currentServer = ref(sessionStorage.getItem('currentServer'))

    watch(currentServer, (newVal) => {
        sessionStorage.setItem('currentServer', newVal)
    })

    return { currentServer }
})
