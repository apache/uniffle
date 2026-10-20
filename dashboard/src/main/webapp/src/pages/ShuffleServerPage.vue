<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one or more
  ~ contributor license agreements.  See the NOTICE file distributed with
  ~ this work for additional information regarding copyright ownership.
  ~ The ASF licenses this file to You under the Apache License, Version 2.0
  ~ (the "License"); you may not use this file except in compliance with
  ~ the License.  You may obtain a copy of the License at
  ~
  ~    http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->

<template>
  <div class="page-wrap">
    <div class="stats-row">
      <router-link
        v-for="item in statCards"
        :key="item.path"
        class="stat-link"
        :to="{ path: item.path, query: { isExcludedPage: item.isExcluded } }"
        @click.native="routerHandler"
      >
        <div class="stat-card" :class="{ 'stat-card--active': activeRoute === item.path }">
          <div class="stat-label">{{ item.label }}</div>
          <div class="stat-value" :class="item.colorClass">{{ dataList.allshuffleServerSize[item.key] ?? 0 }}</div>
        </div>
      </router-link>
    </div>
    <div class="table-section flex-fill">
      <router-view></router-view>
    </div>
  </div>
</template>

<script>
import { onMounted, reactive, provide, computed } from 'vue'
import { useRoute } from 'vue-router'
import { getShufflegetStatusTotal } from '@/api/api'
import { useCurrentServerStore } from '@/store/useCurrentServerStore'

export default {
  setup() {
    const route = useRoute()
    const dataList = reactive({ allshuffleServerSize: { ACTIVE: 0, DECOMMISSIONED: 0, DECOMMISSIONING: 0, EXCLUDED: 0, LOST: 0, UNHEALTHY: 0 } })
    const currentServerStore = useCurrentServerStore()

    const statCards = [
      { label: 'Active', key: 'ACTIVE', path: '/shuffleserverpage/activeNodeList', name: 'activeNodeList', isExcluded: false, colorClass: 'sv-success' },
      { label: 'Decommissioning', key: 'DECOMMISSIONING', path: '/shuffleserverpage/decommissioningNodeList', name: 'decommissioningNodeList', isExcluded: false, colorClass: 'sv-primary' },
      { label: 'Decommissioned', key: 'DECOMMISSIONED', path: '/shuffleserverpage/decommissionedNodeList', name: 'decommissionedNodeList', isExcluded: false, colorClass: 'sv-purple' },
      { label: 'Lost', key: 'LOST', path: '/shuffleserverpage/lostNodeList', name: 'lostNodeList', isExcluded: false, colorClass: 'sv-danger' },
      { label: 'Unhealthy', key: 'UNHEALTHY', path: '/shuffleserverpage/unhealthyNodeList', name: 'unhealthyNodeList', isExcluded: false, colorClass: 'sv-warning' },
      { label: 'Excludes', key: 'EXCLUDED', path: '/shuffleserverpage/excludeNodeList', name: 'excludeNodeList', isExcluded: true, colorClass: 'sv-default' }
    ]

    const activeRoute = computed(() => route.path)

    async function getShuffleStatusTotalPage() {
      try { const res = await getShufflegetStatusTotal(); dataList.allshuffleServerSize = res?.data?.data || dataList.allshuffleServerSize } catch (e) { console.warn('Failed to fetch shuffle status total:', e) }
    }

    currentServerStore.$subscribe((mutable, state) => { if (state.currentServer) getShuffleStatusTotalPage() })
    onMounted(() => { if (currentServerStore.currentServer) getShuffleStatusTotalPage() })

    const routerHandler = () => { getShuffleStatusTotalPage() }
    provide('updateTotalPage', () => { getShuffleStatusTotalPage() })

    return { dataList, routerHandler, statCards, activeRoute }
  }
}
</script>

<style scoped>
.stat-link {
  text-decoration: none;
  flex: 1;
  display: block;
  color: inherit;
}

.stat-card--active {
  border-color: var(--brand-default) !important;
  background: var(--bg-surface-200) !important;
  box-shadow: 0 0 0 1px var(--brand-default);
}
</style>
