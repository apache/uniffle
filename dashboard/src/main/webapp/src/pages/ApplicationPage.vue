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
      <div class="stat-card">
        <div class="stat-label">Apps Total</div>
        <div class="stat-value sv-primary">{{ pageData.apptotal.appTotality ?? '-' }}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">Apps Current</div>
        <div class="stat-value sv-success">{{ pageData.apptotal.appCurrent ?? '-' }}</div>
      </div>
    </div>

    <div class="table-section user-rank-section">
      <div class="table-section-bar">
        <el-icon :size="14" class="page-section-icon"><User /></el-icon>
        <span class="table-section-title">User App Ranking</span>
      </div>
      <el-table :data="pageData.userAppCount" height="100%" :default-sort="sortAppCollect" @sort-change="sortAppCollectChangeEvent">
        <el-table-column prop="userName" label="UserName" min-width="180" sortable fixed />
        <el-table-column prop="appNum" label="Totality" min-width="180" sortable />
      </el-table>
    </div>

    <div class="table-section flex-fill">
      <div class="table-section-bar">
        <el-icon :size="14" class="page-section-icon"><Coin /></el-icon>
        <span class="table-section-title">Apps</span>
      </div>
      <el-table :data="pageData.appInfoData" height="100%" :default-sort="sortApp" @sort-change="sortAppChangeEvent">
        <el-table-column prop="appId" label="AppId" min-width="180" sortable fixed />
        <el-table-column prop="userName" label="UserName" min-width="180" sortable />
        <el-table-column prop="registrationTime" label="Registration Time" min-width="180" :formatter="dateFormatter" sortable />
        <el-table-column prop="updateTime" label="Update Time" min-width="180" :formatter="dateFormatter" sortable />
        <el-table-column label="Version" min-width="180">
          <template v-slot="{ row }">{{ row.version }}_{{ row.gitCommitId }}</template>
        </el-table-column>
        <el-table-column prop="partitionNum" label="PartitionNum" min-width="180" sortable />
        <el-table-column prop="memorySize" label="MemorySize" min-width="180" :formatter="memFormatter" />
        <el-table-column label="LocalFile" min-width="180">
          <template v-slot="{ row }">{{ row.localFileNum }}({{ memFormatter(null, null, row.localTotalSize) }})</template>
        </el-table-column>
        <el-table-column label="HadoopFile" min-width="180">
          <template v-slot="{ row }">{{ row.hadoopFileNum }}({{ memFormatter(null, null, row.hadoopTotalSize) }})</template>
        </el-table-column>
        <el-table-column prop="totalSize" label="TotalSize" min-width="180" :formatter="memFormatter" sortable />
      </el-table>
    </div>
  </div>
</template>

<script>
import { getApplicationInfoList, getAppTotal, getTotalForUser } from '@/api/api'
import { onMounted, reactive } from 'vue'
import { dateFormatter, memFormatter } from '@/utils/common'
import { useCurrentServerStore } from '@/store/useCurrentServerStore'

export default {
  setup() {
    const pageData = reactive({ apptotal: {}, userAppCount: [], appInfoData: [] })
    const currentServerStore = useCurrentServerStore()

    async function getApplicationInfoListPage() {
      try { const res = await getApplicationInfoList(); pageData.appInfoData = res?.data?.data || [] } catch (e) { console.warn('Failed to fetch application info:', e) }
    }
    async function getTotalForUserPage() {
      try { const res = await getTotalForUser(); pageData.userAppCount = res?.data?.data || [] } catch (e) { console.warn('Failed to fetch user app count:', e) }
    }
    async function getAppTotalPage() {
      try { const res = await getAppTotal(); pageData.apptotal = res?.data?.data || {} } catch (e) { console.warn('Failed to fetch app total:', e) }
    }

    currentServerStore.$subscribe((mutable, state) => { if (state.currentServer) { getApplicationInfoListPage(); getTotalForUserPage(); getAppTotalPage() } })
    onMounted(() => { if (currentServerStore.currentServer) { getApplicationInfoListPage(); getTotalForUserPage(); getAppTotalPage() } })

    const sortAppCollect = reactive({})
    const sortAppCollectChangeEvent = (sortInfo) => { for (const k in sortAppCollect) delete sortAppCollect[k]; sortAppCollect[sortInfo.prop] = sortInfo.order }
    const sortApp = reactive({})
    const sortAppChangeEvent = (sortInfo) => { for (const k in sortApp) delete sortApp[k]; sortApp[sortInfo.prop] = sortInfo.order }

    return { pageData, sortAppCollect, sortAppCollectChangeEvent, sortApp, sortAppChangeEvent, dateFormatter, memFormatter }
  }
}
</script>
