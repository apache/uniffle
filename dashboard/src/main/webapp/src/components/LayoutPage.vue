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
  <div class="common-layout">
    <el-container>
      <el-header>
        <el-row>
          <el-col :span="24">
            <el-menu
              :default-active="currentActive"
              router
              class="el-menu-demo"
              mode="horizontal"
              background-color="#20B2AA"
              box-shadow="0 -2px 8px 0 rgba(0,0,0,0.12)"
              text-color="#fff"
              active-text-color="#ffd04b"
            >
              <el-menu-item index="0">
                <div class="unffilelogo">
                  <img src="../assets/uniffle-logo.png" alt="unffile" />
                </div>
              </el-menu-item>
              <el-menu-item index="/dashboardpage">
                <el-icon><House /></el-icon>
                <span>Dashboard</span>
              </el-menu-item>
              <el-menu-item index="/coordinatorserverpage">
                <el-icon><House /></el-icon>
                <span>Coordinator</span>
              </el-menu-item>
              <el-menu-item index="/shuffleserverpage">
                <el-icon><Monitor /></el-icon>
                <span>Shuffle Server</span>
              </el-menu-item>
              <el-menu-item index="/applicationpage">
                <el-icon><Coin /></el-icon>
                <span>Application</span>
              </el-menu-item>
              <el-sub-menu index="">
                <template #title>Switching server</template>
                <el-menu-item
                  v-for="item in hostNameAndPorts"
                  :key="item.label"
                  index="/nullpage"
                  @click="handleChangeServer(item.label)"
                >
                  <span>{{ item.label }}</span>
                </el-menu-item>
              </el-sub-menu>
              <el-menu-item index="">
                <el-icon><SwitchFilled /></el-icon>
                <label class="currentserver">
                  current:{{ currentServerStore.currentServer }}
                </label>
              </el-menu-item>
            </el-menu>
          </el-col>
        </el-row>
      </el-header>
      <el-main>
        <router-view></router-view>
      </el-main>
    </el-container>
  </div>
</template>

<script>
import { ref, reactive, onMounted } from 'vue'
import { getAllCoordinatorAddrees } from '@/api/api'
import { useCurrentServerStore } from '@/store/useCurrentServerStore'

export default {
  setup() {
    const currentActive = ref('0')
    const currentServerStore = useCurrentServerStore()
    const hostNameAndPorts = reactive([
      {
        value: '',
        label: ''
      }
    ])

    /**
     * Troubleshoot the problem that the browser refresh address menu cannot be selected.
     */
    function handleSelectMenu() {
      const urlAddress = window.location.hash.toString().replace(/^#/, '')
      const shuffleServerPage = '/shuffleserverpage'
      if (urlAddress.startsWith(shuffleServerPage)) {
        currentActive.value = shuffleServerPage
      } else {
        currentActive.value = urlAddress
      }
    }

    const handleChangeServer = (key) => {
      currentServerStore.currentServer = key
    }

    async function getSelectCurrentServer() {
      const res = await getAllCoordinatorAddrees()
      const selectCurrentServer = res.data.data
      if (!currentServerStore.currentServer) {
        currentServerStore.currentServer = Object.keys(selectCurrentServer)[0]
      }
      hostNameAndPorts.length = 0
      Object.entries(selectCurrentServer).forEach(([key, value]) => {
        hostNameAndPorts.push({ value: value, label: key })
      })
      hostNameAndPorts.sort((a, b) => a.label.localeCompare(b.label))
    }

    onMounted(() => {
      getSelectCurrentServer()
      handleSelectMenu()
    })

    return {
      currentActive,
      currentServerStore,
      hostNameAndPorts,
      handleChangeServer
    }
  }
}
</script>

<style scoped>
a {
  text-decoration: none;
  color: white;
}

.unffilelogo {
  background-color: #20b2aa;
  height: 100%;
  position: relative;
}

.unffilelogo > img {
  height: 55px;
}

.currentserver {
  font-family: 'Andale Mono';
  font-size: smaller;
  color: yellow;
}
</style>
