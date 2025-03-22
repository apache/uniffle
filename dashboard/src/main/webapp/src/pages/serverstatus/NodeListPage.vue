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
  <div>
    <el-button type="success" @click="dialogFormVisible = true" v-show="isExcludePage()"> Add Node </el-button>
    <el-button type="danger" @click="handleDeleteNode" v-show="isExcludePage()">Delete({{ selectItemNum }})</el-button>
    <el-table
      :data="listPageData.tableData"
      height="800"
      style="width: 100%"
      :row-key="rowKey"
      :default-sort="sortColumn"
      @sort-change="sortChangeEvent"
      @selection-change="handlerSelectionChange"
    >
      <el-table-column type="selection" width="55" v-show="isExcludePage()"/>
      <el-table-column prop="id" label="Id" min-width="140" sortable fixed />
      <el-table-column label="NodeInfo(ip:jetty/grpc/netty)" min-width="140" >
        <template v-slot="{ row }">
          <div class="mb-4">
            {{ row.ip }}:{{ row.jettyPort }}/{{ row.grpcPort }}/{{ row.nettyPort }}
          </div>
        </template>
      </el-table-column>
      <el-table-column
        prop="usedMemory"
        label="UsedMem"
        min-width="80"
        :formatter="memFormatter"
        sortable
      />
      <el-table-column
        prop="preAllocatedMemory"
        label="PreAllocatedMem"
        min-width="100"
        :formatter="memFormatter"
        sortable
      />
      <el-table-column
        prop="availableMemory"
        label="AvailableMem"
        min-width="100"
        :formatter="memFormatter"
        sortable
      />
      <el-table-column prop="eventNumInFlush" label="FlushNum" min-width="80" sortable />
      <el-table-column prop="status" label="Status" min-width="80" sortable />
      <el-table-column
        prop="startTime"
        label="StartTime"
        min-width="120"
        :formatter="dateFormatter"
        sortable
      />
      <el-table-column
        prop="registrationTime"
        label="RegistrationTime"
        min-width="120"
        :formatter="dateFormatter"
        sortable
      />
      <el-table-column
        prop="timestamp"
        label="HeartbeatTime"
        min-width="120"
        :formatter="dateFormatter"
        sortable
      />
      <el-table-column label="Operations" min-width="240">
        <template v-slot="{ row }">
          <div class="mb-4">
            <el-button type="warning" @click="handlerServerConf(row)">conf</el-button>
            <el-button type="primary" @click="handlerServerMetrics(row)">metrics</el-button>
            <el-button type="success" @click="handlerServerPrometheusMetrics(row)">metrics(prom)</el-button>
            <el-button type="info" @click="handlerServerStacks(row)">stacks</el-button>
          </div>
        </template>
      </el-table-column>
      <el-table-column prop="tags" label="Tags" min-width="140" />
      <el-table-column label="Version" min-width="140">
        <template v-slot="{ row }">
          <div class="mb-4">
            {{ row.version }}_{{ row.gitCommitId }}
          </div>
        </template>
      </el-table-column>
      <el-table-column v-if="isShowRemove" label="Operations(admin)">
        <template v-slot:default="scope">
          <el-button size="small" type="danger" @click="showDeleteConfirm(scope.row)">
            Remove
          </el-button>
        </template>
      </el-table-column>
      <el-table-column prop="displayMetrics" label="DisplayMetrics" min-width="120">
        <template v-slot="{ row }">
          <div v-for="(value, key) in row.displayMetrics" :key="key">
            <span>{{ key }}:{{ value }}</span>
          </div>
        </template>
      </el-table-column>
    </el-table>
    <el-dialog
        v-model="dialogFormVisible"
        title="Please enter the server id list to be excluded:"
        class="dialog-wrapper"
        v-show="isExcludePage()"
    >
      <el-form>
        <el-form-item :label-width="formLabelWidth">
          <el-input
              v-model="textarea"
              class="textarea-wrapper"
              :rows="10"
              type="textarea"
              placeholder="Please input"
          />
        </el-form-item>
      </el-form>
      <template #footer>
        <div class="dialog-footer">
          <el-button @click="dialogFormVisible = false">Cancel</el-button>
          <el-button type="primary" @click="handleConfirmAddHandler"> Confirm </el-button>
        </div>
      </template>
    </el-dialog>
  </div>
</template>
<script>
import { onMounted, reactive, watch, ref, inject, watchEffect } from 'vue'
import { memFormatter, dateFormatter } from '@/utils/common'
import { useRouter } from 'vue-router'
import { useCurrentServerStore } from '@/store/useCurrentServerStore'
import { ElMessageBox, ElMessage } from 'element-plus'
import {
  getShuffleActiveNodes,
  getShuffleDecommissionedList,
  getShuffleDecommissioningList,
  getShuffleLostList,
  getShuffleUnhealthyList,
  deleteConfirmedLostServer,
  getShuffleServerConf,
  getShuffleServerMetrics,
  getShuffleServerPrometheusMetrics,
  getShuffleServerStacks,
  addShuffleExcludeNodes,
  getShuffleExcludeNodes,
  removeShuffleExcludeNodes
} from '@/api/api'

export default {
  setup() {
    const router = useRouter()
    const currentServerStore = useCurrentServerStore()
    const sortColumn = reactive({})
    const listPageData = reactive({ tableData: [] })
    const isShowRemove = ref(false)
    const dialogFormVisible = ref(false)
    const formLabelWidth = '10%'
    const textarea = ref('')
    const selectItemNum = ref(0)
    const rowKey = 'id'
    const selectedRows = ref([])
    function isExcludePage() {
      return router.currentRoute.value.query.isExcludedPage === 'true';
    }

    async function getShuffleExcludeNodesPage() {
      const res = await getShuffleExcludeNodes()
      listPageData.tableData = res.data.data
    }

    // The system obtains data from global variables and requests the interface to obtain new data after data changes.
    currentServerStore.$subscribe((mutable, state) => {
      if (state.currentServer) {
        getShuffleExcludeNodesPage()
      }
    })

    onMounted(() => {
      // If the coordinator address to request is not found in the global variable, the request is not initiated.
      if (currentServerStore.currentServer) {
        getShuffleExcludeNodesPage()
      }
    })

    /**
     * The following describes how to handle add events.
     */
    function handleConfirmAddHandler() {
      dialogFormVisible.value = false
      addShuffleExcludeNodesPage()
      // Refreshing the number of blacklists.
      updateTotalPage()
      // Refreshing the Blacklist list.
      getShuffleExcludeNodesPage()
    }
    async function addShuffleExcludeNodesPage() {
      try {
        const excludeNodes = textarea.value.split('\n').map((item) => item.trim())
        const excludeNodesObj = { excludeNodes }
        const res = await addShuffleExcludeNodes(excludeNodesObj)
        if (res.status >= 200 && res.status < 300) {
          if (res.data.data === 'success') {
            ElMessage.success('Add successfully.')
          } else {
            ElMessage.error('Add failed.')
          }
        } else {
          ElMessage.error('Failed to add due to server bad.')
        }
      } catch (err) {
        ElMessage.error('Failed to add due to network exception.')
      }
    }
    function handleDeleteNode() {
      ElMessageBox.confirm('Are you sure about removing these nodes?', 'Warning', {
        confirmButtonText: 'OK',
        cancelButtonText: 'Cancel',
        type: 'warning'
      })
          .then(() => {
            if (selectedRows.value.length === 0) {
              ElMessage({
                type: 'info',
                message: 'No node is selected, Nothing!'
              })
            } else {
              const selectedIds = selectedRows.value.map((row) => row[rowKey])
              deleteShuffleExcludedNodes(selectedIds)
              // Refreshing the number of blacklists.
              updateTotalPage()
              // Refreshing the Blacklist list.
              getShuffleExcludeNodesPage()
              ElMessage({
                type: 'success',
                message: 'Delete completed'
              })
            }
          })
          .catch(() => {
            ElMessage({
              type: 'info',
              message: 'Delete canceled'
            })
          })
    }

    async function deleteShuffleExcludedNodes(excludeNodes) {
      try {
        const excludeNodesObj = { excludeNodes }
        const res = await removeShuffleExcludeNodes(excludeNodesObj)
        if (res.status >= 200 && res.status < 300) {
          if (res.data.data === 'success') {
            ElMessage.success('Delete successfully.')
          } else {
            ElMessage.error('Delete failed.')
          }
        } else {
          ElMessage.error('Failed to delete due to server bad.')
        }
      } catch (err) {
        ElMessage.error('Failed to delete due to network exception.')
      }
    }
    async function deleteLostServer(row) {
      try {
        const params = { serverId: row.id }
        const res = await deleteConfirmedLostServer(params)
        // Invoke the interface to delete the lost server, prompting a message based on the result returned.
        if (res.data.data === 'success') {
          ElMessage.success('remove ' + row.id + ' success...')
        } else {
          ElMessage.error('remove ' + row.id + ' fail...')
        }
      } catch (err) {
        ElMessage.error('remove ' + row.id + ' request timeout...')
      }
    }

    async function getShuffleActiveNodesPage() {
      const res = await getShuffleActiveNodes()
      listPageData.tableData = res.data.data
    }

    async function getShuffleDecommissionedListPage() {
      const res = await getShuffleDecommissionedList()
      listPageData.tableData = res.data.data
    }

    async function getShuffleDecommissioningListPage() {
      const res = await getShuffleDecommissioningList()
      listPageData.tableData = res.data.data
    }

    async function getShuffleLostListPage() {
      const res = await getShuffleLostList()
      listPageData.tableData = res.data.data
    }

    async function getShuffleUnhealthyListPage() {
      const res = await getShuffleUnhealthyList()
      listPageData.tableData = res.data.data
    }

    async function getShuffleExcludeListPage() {
      const res = await getShuffleExcludeNodes()
      listPageData.tableData = res.data.data
    }

    function combinedRequestAddress(serverRow) {
      return 'http://' + serverRow.ip + ':' + serverRow.jettyPort
    }

    async function handlerServerConf(serverRow) {
      try {
        const headers = {}
        headers.targetAddress = combinedRequestAddress(serverRow)
        const response = await getShuffleServerConf({}, headers)
        if (response.status >= 200 && response.status < 300) {
          const newWindow = window.open('', '_blank')
          let tableHTML = `
                          <style>
                            table {
                              width: 100%;
                            }
                            th, td {
                              padding: 0 20px;
                              text-align: left;
                            }
                          </style>
                          <table>
                            <tr>
                              <th>Key</th>
                              <th>Value</th>
                            </tr>
                        `
          for (const item of response.data.data) {
            tableHTML += `<tr><td>${item.argumentKey}</td><td>${item.argumentValue}</td></tr>`
          }
          tableHTML += '</table>'
          newWindow.document.write(tableHTML)
        } else {
          ElMessage.error('Request failed.')
        }
      } catch (err) {
        ElMessage.error('Internal error.')
      }
    }

    async function handlerServerMetrics(serverRow) {
      try {
        const headers = {}
        headers.targetAddress = combinedRequestAddress(serverRow)
        const response = await getShuffleServerMetrics({}, headers)
        if (response.status >= 200 && response.status < 300) {
          const newWindow = window.open('', '_blank')
          newWindow.document.write('<pre>' + JSON.stringify(response.data, null, 2) + '</pre>')
        } else {
          ElMessage.error('Request failed.')
        }
      } catch (err) {
        ElMessage.error('Internal error.')
      }
    }

    async function handlerServerPrometheusMetrics(serverRow) {
      try {
        const headers = {}
        headers.targetAddress = combinedRequestAddress(serverRow)
        const response = await getShuffleServerPrometheusMetrics({}, headers)
        if (response.status >= 200 && response.status < 300) {
          const newWindow = window.open('', '_blank')
          newWindow.document.write('<pre>' + response.data + '</pre>')
        } else {
          ElMessage.error('Request failed.')
        }
      } catch (err) {
        ElMessage.error('Internal error.')
      }
    }

    async function handlerServerStacks(serverRow) {
      try {
        const headers = {}
        headers.targetAddress = combinedRequestAddress(serverRow)
        const response = await getShuffleServerStacks({}, headers)
        if (response.status >= 200 && response.status < 300) {
          const newWindow = window.open('', '_blank')
          newWindow.document.write('<pre>' + response.data + '</pre>')
        } else {
          ElMessage.error('Request failed.')
        }
      } catch (err) {
        ElMessage.error('Internal error.')
      }
    }

    const loadPageData = () => {
      isShowRemove.value = false
      listPageData.tableData = []
      if (router.currentRoute.value.name === 'activeNodeList') {
        getShuffleActiveNodesPage()
      } else if (router.currentRoute.value.name === 'decommissioningNodeList') {
        getShuffleDecommissioningListPage()
      } else if (router.currentRoute.value.name === 'decommissionedNodeList') {
        getShuffleDecommissionedListPage()
      } else if (router.currentRoute.value.name === 'unhealthyNodeList') {
        getShuffleUnhealthyListPage()
      } else if (router.currentRoute.value.name === 'lostNodeList') {
        isShowRemove.value = true
        getShuffleLostListPage()
      } else if (router.currentRoute.value.name === 'excludeNodeList') {
        getShuffleExcludeListPage()
      }
    }

    onMounted(() => {
      watchEffect(() => {
        // If the coordinator address to request is not found in the global variable, the request is not initiated.
        if (currentServerStore.currentServer) {
          loadPageData()
        }
      })
    })

    watch(router.currentRoute, () => {
      if (currentServerStore.currentServer) {
        loadPageData()
      }
    })

    const sortChangeEvent = (sortInfo) => {
      for (const sortColumnKey in sortColumn) {
        delete sortColumn[sortColumnKey]
      }
      sortColumn[sortInfo.prop] = sortInfo.order
    }
    /**
     * Get the callback method of the parent page and update the number of servers on the page.
     */
    const updateTotalPage = inject('updateTotalPage')
    const showDeleteConfirm = (row) => {
      ElMessageBox.confirm(`Are you sure to remove ${row.id}?`, 'Confirmation', {
        confirmButtonText: 'Remove',
        cancelButtonText: 'Cancel',
        type: 'warning'
      })
        .then(() => {
          // Perform deletion logic here.
          deleteLostServer(row)
          // Reload the lost server information
          getShuffleLostListPage()
          updateTotalPage()
        })
        .catch(() => {
          // Cancelled
        })
    }
    function handlerSelectionChange(selection) {
      selectedRows.value = selection
      selectItemNum.value = selectedRows.value.length
    }
    return {
      listPageData,
      sortColumn,
      isShowRemove,
      showDeleteConfirm,
      sortChangeEvent,
      handlerServerConf,
      handlerServerPrometheusMetrics,
      handlerServerMetrics,
      handlerServerStacks,
      memFormatter,
      dateFormatter,
      dialogFormVisible,
      formLabelWidth,
      textarea,
      selectItemNum,
      handleDeleteNode,
      getShuffleExcludeNodesPage,
      handleConfirmAddHandler,
      isExcludePage,
      rowKey,
      handlerSelectionChange
    }
  }
}
</script>

<style>
.textarea-wrapper {
  width: 90%;
}
.dialog-wrapper {
  width: 50%;
}
</style>