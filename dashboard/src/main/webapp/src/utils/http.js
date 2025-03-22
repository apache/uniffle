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

import request from '@/utils/request'
import { useCurrentServerStore } from '@/store/useCurrentServerStore'

const requestServerType = { coordinator: 'coordinator', server: 'server' }
const http = {
  get(url, params, headers, fontBackFlag) {
    if (fontBackFlag === 0) {
      // The system obtains the address of the Coordinator to be accessed from global variables.
      const currentServerStore = useCurrentServerStore()
      if (headers) {
        headers.requestServerType = requestServerType.server
      } else {
        headers = {}
        headers.requestServerType = requestServerType.coordinator
        headers.targetAddress = currentServerStore.currentServer
      }
      return request.getBackEndAxiosInstance().get(url, { params, headers })
    } else {
      return request.getFrontEndAxiosInstance().get(url, { params, headers })
    }
  },
  post(url, data, headers, fontBackFlag) {
    if (fontBackFlag === 0) {
      // The system obtains the address of the Coordinator to be accessed from global variables.
      const currentServerStore = useCurrentServerStore()
      if (headers) {
        headers.requestServerType = requestServerType.server
      } else {
        headers = {}
        headers.requestServerType = requestServerType.coordinator
        headers.targetAddress = currentServerStore.currentServer
      }
      return request.getBackEndAxiosInstance().post(url, data, headers)
    } else {
      return request.getFrontEndAxiosInstance().post(url, data, headers)
    }
  },
  delete(url, params, headers, fontBackFlag) {
    if (fontBackFlag === 0) {
      // The system obtains the address of the Coordinator to be accessed from global variables.
      const currentServerStore = useCurrentServerStore()
      if (headers) {
        headers.requestServerType = requestServerType.server
      } else {
        headers = {}
        headers.requestServerType = requestServerType.coordinator
        headers.targetAddress = currentServerStore.currentServer
      }
      return request.getBackEndAxiosInstance().delete(url, { params, headers })
    } else {
      return request.getFrontEndAxiosInstance().delete(url, { params, headers })
    }
  }
}
export default http
