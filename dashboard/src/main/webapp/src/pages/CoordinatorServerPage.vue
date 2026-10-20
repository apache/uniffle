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
    <div class="page-section">
      <div class="page-section-header">
        <el-icon :size="16" class="page-section-icon"><Platform /></el-icon>
        <span class="page-section-title">Coordinator Server</span>
      </div>
      <div class="page-section-body">
        <el-descriptions :column="3" :size="size" border>
          <el-descriptions-item>
            <template #label><div class="cell-item"><el-icon :style="iconStyle"><Platform /></el-icon> Coordinator ID</div></template>
            {{ pageData.serverInfo.coordinatorId }}
          </el-descriptions-item>
          <el-descriptions-item>
            <template #label><div class="cell-item"><el-icon :style="iconStyle"><Link /></el-icon> IP Address</div></template>
            {{ pageData.serverInfo.serverIp }}
          </el-descriptions-item>
          <el-descriptions-item>
            <template #label><div class="cell-item"><el-icon :style="iconStyle"><Wallet /></el-icon> Server Port</div></template>
            {{ pageData.serverInfo.serverPort }}
          </el-descriptions-item>
          <el-descriptions-item>
            <template #label><div class="cell-item"><el-icon :style="iconStyle"><Wallet /></el-icon> Web Port</div></template>
            {{ pageData.serverInfo.serverWebPort }}
          </el-descriptions-item>
          <el-descriptions-item>
            <template #label><div class="cell-item"><el-icon :style="iconStyle"><Wallet /></el-icon> Monitoring</div></template>
            <div class="monitoring-btns">
              <el-button type="primary" size="small" @click="handlerMetrics">Metrics</el-button>
              <el-button type="success" size="small" @click="handlerPromMetrics">Prometheus</el-button>
              <el-button type="warning" size="small" @click="handlerStacks">Stacks</el-button>
            </div>
          </el-descriptions-item>
          <el-descriptions-item>
            <template #label><div class="cell-item"><el-icon :style="iconStyle"><Wallet /></el-icon> Version</div></template>
            {{ pageData.serverInfo.version }}_{{ pageData.serverInfo.gitCommitId }}
          </el-descriptions-item>
          <el-descriptions-item>
            <template #label><div class="cell-item"><el-icon :style="iconStyle"><Wallet /></el-icon> Start Time</div></template>
            <template #default>{{ dateFormatter(null, null, pageData.serverInfo.startTime) }}</template>
          </el-descriptions-item>
        </el-descriptions>
      </div>
    </div>

    <div class="table-section flex-fill">
      <div class="table-section-bar">
        <el-icon :size="14" class="page-section-icon"><Setting /></el-icon>
        <span class="table-section-title">Coordinator Properties</span>
      </div>
      <el-table :data="filteredTableData" stripe height="100%">
        <el-table-column prop="argumentKey" label="Name" min-width="380" />
        <el-table-column prop="argumentValue" label="Value" min-width="380" :show-overflow-tooltip="true" />
        <el-table-column align="right">
          <template #header>
            <el-input v-model="searchKeyword" size="small" placeholder="Type to search" class="table-search-input" />
          </template>
        </el-table-column>
      </el-table>
    </div>
  </div>
</template>

<script>
import { ref, reactive, computed, onMounted, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { getCoordinatorConf, getCoordinatorMetrics, getCoordinatorPrometheusMetrics, getCoordinatorServerInfo, getCoordinatorStacks } from '@/api/api'
import { useCurrentServerStore } from '@/store/useCurrentServerStore'
import { dateFormatter } from '@/utils/common'

export default {
  setup() {
    const pageData = reactive({ tableData: [], serverInfo: {} })
    const currentServerStore = useCurrentServerStore()

    async function getCoordinatorServerConfPage() {
      try { const res = await getCoordinatorConf(); pageData.tableData = res?.data?.data || [] } catch (e) { console.warn('Failed to fetch coordinator conf:', e) }
    }
    async function getCoorServerInfo() {
      try { const res = await getCoordinatorServerInfo(); pageData.serverInfo = res?.data?.data || {} } catch (e) { console.warn('Failed to fetch coordinator server info:', e) }
    }

    async function handlerMetrics() {
      try {
        const response = await getCoordinatorMetrics()
        if (response.status >= 200 && response.status < 300) { const w = window.open('', '_blank'); w.document.write('<pre>' + JSON.stringify(response.data, null, 2) + '</pre>') } else { ElMessage.error('Request failed.') }
      } catch (err) { ElMessage.error('Internal error.') }
    }
    async function handlerPromMetrics() {
      try {
        const response = await getCoordinatorPrometheusMetrics()
        if (response.status >= 200 && response.status < 300) { const w = window.open('', '_blank'); w.document.write('<pre>' + response.data + '</pre>') } else { ElMessage.error('Request failed.') }
      } catch (err) { ElMessage.error('Internal error.') }
    }
    async function handlerStacks() {
      try {
        const response = await getCoordinatorStacks()
        if (response.status >= 200 && response.status < 300) { const w = window.open('', '_blank'); w.document.write('<pre>' + response.data + '</pre>') } else { ElMessage.error('Request failed.') }
      } catch (err) { ElMessage.error('Internal error.') }
    }

    watch(() => currentServerStore.currentServer, () => { getCoordinatorServerConfPage(); getCoorServerInfo() })
    onMounted(() => { if (currentServerStore.currentServer) { getCoordinatorServerConfPage(); getCoorServerInfo() } })

    const size = ref('')
    const iconStyle = computed(() => ({ marginRight: '6px' }))
    const searchKeyword = ref('')
    const filteredTableData = computed(() => {
      const keyword = searchKeyword.value.trim()
      if (!keyword) return pageData.tableData
      return pageData.tableData.filter((row) => row.argumentValue.includes(keyword) || row.argumentKey.includes(keyword))
    })

    return { pageData, iconStyle, size, handlerMetrics, handlerPromMetrics, handlerStacks, filteredTableData, searchKeyword, dateFormatter }
  }
}
</script>

<style scoped>
.monitoring-btns {
  display: flex;
  gap: 8px;
}
</style>
