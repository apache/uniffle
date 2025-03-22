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

import Mock from 'mockjs'

Mock.mock(/\/server\/nodes\?status=active/, 'get', function (options) {
  return {
    code: 0,
    data: [
      {
        id: '192.168.1.1-29999-29997',
        ip: '192.168.1.1',
        grpcPort: 29999,
        usedMemory: 0,
        preAllocatedMemory: 0,
        availableMemory: 42949672960,
        eventNumInFlush: 0,
        registrationTime: 1722057265853,
        timestamp: 1722404445850,
        tags: ['GRPC_NETTY', 'ss_v5'],
        status: 'ACTIVE',
        storageInfo: {
          '/dev/nvme1n1': {
            type: 'HDD',
            status: 'NORMAL'
          },
          '/dev/nvme0n1': {
            type: 'HDD',
            status: 'NORMAL'
          }
        },
        nettyPort: 29997,
        jettyPort: 29998,
        totalMemory: 42949672960
      },
      {
        id: '192.168.1.2-29999-29997',
        ip: '192.168.1.2',
        grpcPort: 29999,
        usedMemory: 0,
        preAllocatedMemory: 0,
        availableMemory: 42949672960,
        eventNumInFlush: 0,
        registrationTime: 1722057259803,
        timestamp: 1722404449663,
        tags: ['GRPC_NETTY', 'ss_v5'],
        status: 'ACTIVE',
        storageInfo: {
          '/dev/nvme1n1': {
            type: 'HDD',
            status: 'NORMAL'
          },
          '/dev/nvme0n1': {
            type: 'HDD',
            status: 'NORMAL'
          }
        },
        nettyPort: 29997,
        jettyPort: 29998,
        totalMemory: 42949672960
      },
      {
        id: '192.168.1.3-29999-29997',
        ip: '192.168.1.3',
        grpcPort: 29999,
        usedMemory: 0,
        preAllocatedMemory: 0,
        availableMemory: 42949672960,
        eventNumInFlush: 0,
        registrationTime: 1722057274269,
        timestamp: 1722404454266,
        tags: ['GRPC_NETTY', 'ss_v5'],
        status: 'ACTIVE',
        storageInfo: {
          '/dev/nvme1n1': {
            type: 'HDD',
            status: 'NORMAL'
          },
          '/dev/nvme0n1': {
            type: 'HDD',
            status: 'NORMAL'
          }
        },
        nettyPort: 29997,
        jettyPort: 29998,
        totalMemory: 42949672960
      },
      {
        id: '192.168.1.4-29999-29997',
        ip: '192.168.1.4',
        grpcPort: 29999,
        usedMemory: 0,
        preAllocatedMemory: 0,
        availableMemory: 42949672960,
        eventNumInFlush: 0,
        registrationTime: 1722057272201,
        timestamp: 1722404452198,
        tags: ['GRPC_NETTY', 'ss_v5'],
        status: 'ACTIVE',
        storageInfo: {
          '/dev/sdd1': {
            type: 'HDD',
            status: 'NORMAL'
          },
          '/dev/sdc1': {
            type: 'HDD',
            status: 'NORMAL'
          }
        },
        nettyPort: 29997,
        jettyPort: 29998,
        totalMemory: 42949672960
      },
      {
        id: '192.168.1.5-29999-29997',
        ip: '192.168.1.5',
        grpcPort: 29999,
        usedMemory: 0,
        preAllocatedMemory: 0,
        availableMemory: 42949672960,
        eventNumInFlush: 0,
        registrationTime: 1722057271192,
        timestamp: 1722404451189,
        tags: ['GRPC_NETTY', 'ss_v5'],
        status: 'ACTIVE',
        storageInfo: {
          '/dev/sdd1': {
            type: 'HDD',
            status: 'NORMAL'
          },
          '/dev/sdc1': {
            type: 'HDD',
            status: 'NORMAL'
          }
        },
        nettyPort: 29997,
        jettyPort: 29998,
        totalMemory: 42949672960
      },
      {
        id: '192.168.1.6-29999-29997',
        ip: '192.168.1.6',
        grpcPort: 29999,
        usedMemory: 0,
        preAllocatedMemory: 0,
        availableMemory: 42949672960,
        eventNumInFlush: 0,
        registrationTime: 1722057271192,
        timestamp: 1722404451189,
        tags: ['GRPC_NETTY', 'ss_v5'],
        status: 'ACTIVE',
        storageInfo: {
          '/dev/sdd1': {
            type: 'HDD',
            status: 'NORMAL'
          },
          '/dev/sdc1': {
            type: 'HDD',
            status: 'NORMAL'
          }
        },
        nettyPort: 29997,
        jettyPort: 29998,
        totalMemory: 42949672960
      },
      {
        id: '192.168.1.7-29999-29997',
        ip: '192.168.1.7',
        grpcPort: 29999,
        usedMemory: 0,
        preAllocatedMemory: 0,
        availableMemory: 42949672960,
        eventNumInFlush: 0,
        registrationTime: 1722057271192,
        timestamp: 1722404451189,
        tags: ['GRPC_NETTY', 'ss_v5'],
        status: 'ACTIVE',
        storageInfo: {
          '/dev/sdd1': {
            type: 'HDD',
            status: 'NORMAL'
          },
          '/dev/sdc1': {
            type: 'HDD',
            status: 'NORMAL'
          }
        },
        nettyPort: 29997,
        jettyPort: 29998,
        totalMemory: 42949672960
      },
      {
        id: '192.168.1.8-29999-29997',
        ip: '192.168.1.8',
        grpcPort: 29999,
        usedMemory: 0,
        preAllocatedMemory: 0,
        availableMemory: 42949672960,
        eventNumInFlush: 0,
        registrationTime: 1722057271192,
        timestamp: 1722404451189,
        tags: ['GRPC_NETTY', 'ss_v5'],
        status: 'ACTIVE',
        storageInfo: {
          '/dev/sdd1': {
            type: 'HDD',
            status: 'NORMAL'
          },
          '/dev/sdc1': {
            type: 'HDD',
            status: 'NORMAL'
          }
        },
        nettyPort: 29997,
        jettyPort: 29998,
        totalMemory: 42949672960
      },
      {
        id: '192.168.1.9-29999-29997',
        ip: '192.168.1.9',
        grpcPort: 29999,
        usedMemory: 0,
        preAllocatedMemory: 0,
        availableMemory: 42949672960,
        eventNumInFlush: 0,
        registrationTime: 1722057271192,
        timestamp: 1722404451189,
        tags: ['GRPC_NETTY', 'ss_v5'],
        status: 'ACTIVE',
        storageInfo: {
          '/dev/sdd1': {
            type: 'HDD',
            status: 'NORMAL'
          },
          '/dev/sdc1': {
            type: 'HDD',
            status: 'NORMAL'
          }
        },
        nettyPort: 29997,
        jettyPort: 29998,
        totalMemory: 42949672960
      },
      {
        id: '192.168.1.10-29999-29997',
        ip: '192.168.1.10',
        grpcPort: 29999,
        usedMemory: 0,
        preAllocatedMemory: 0,
        availableMemory: 42949672960,
        eventNumInFlush: 0,
        registrationTime: 1722057271192,
        timestamp: 1722404451189,
        tags: ['GRPC_NETTY', 'ss_v5'],
        status: 'ACTIVE',
        storageInfo: {
          '/dev/sdd1': {
            type: 'HDD',
            status: 'NORMAL'
          },
          '/dev/sdc1': {
            type: 'HDD',
            status: 'NORMAL'
          }
        },
        nettyPort: 29997,
        jettyPort: 29998,
        totalMemory: 42949672960
      },
    ],
    errMsg: 'success'
  }
})

Mock.mock(/\/server\/nodes\?status=decommissioning/, 'get', function (options) {
  return {
    code: 0,
    data: [
      {
        id: '192.168.1.1-29999-29997',
        ip: '192.168.1.1',
        grpcPort: 29999,
        usedMemory: 0,
        preAllocatedMemory: 0,
        availableMemory: 42949672960,
        eventNumInFlush: 0,
        registrationTime: 1722057265853,
        timestamp: 1722404445850,
        tags: ['GRPC_NETTY', 'ss_v5'],
        status: 'ACTIVE',
        storageInfo: {
          '/dev/nvme1n1': {
            type: 'HDD',
            status: 'NORMAL'
          },
          '/dev/nvme0n1': {
            type: 'HDD',
            status: 'NORMAL'
          }
        },
        nettyPort: 29997,
        jettyPort: 29998,
        totalMemory: 42949672960
      }
    ],
    errMsg: 'success'
  }
})

Mock.mock(/\/server\/nodes\?status=decommissioned/, 'get', function (options) {
  return {
    code: 0,
    data: [
      {
        id: '192.168.1.1-29999-29997',
        ip: '192.168.1.1',
        grpcPort: 29999,
        usedMemory: 0,
        preAllocatedMemory: 0,
        availableMemory: 42949672960,
        eventNumInFlush: 0,
        registrationTime: 1722057265853,
        timestamp: 1722404445850,
        tags: ['GRPC_NETTY', 'ss_v5'],
        status: 'ACTIVE',
        storageInfo: {
          '/dev/nvme1n1': {
            type: 'HDD',
            status: 'NORMAL'
          },
          '/dev/nvme0n1': {
            type: 'HDD',
            status: 'NORMAL'
          }
        },
        nettyPort: 29997,
        jettyPort: 29998,
        totalMemory: 42949672960
      }
    ],
    errMsg: 'success'
  }
})

Mock.mock(/\/server\/nodes\?status=lost/, 'get', function (options) {
  return {
    code: 0,
    data: [
      {
        id: '192.168.1.1-29999-29997',
        ip: '192.168.1.1',
        grpcPort: 29999,
        usedMemory: 0,
        preAllocatedMemory: 0,
        availableMemory: 42949672960,
        eventNumInFlush: 0,
        registrationTime: 1722057265853,
        timestamp: 1722404445850,
        tags: ['GRPC_NETTY', 'ss_v5'],
        status: 'ACTIVE',
        storageInfo: {
          '/dev/nvme1n1': {
            type: 'HDD',
            status: 'NORMAL'
          },
          '/dev/nvme0n1': {
            type: 'HDD',
            status: 'NORMAL'
          }
        },
        nettyPort: 29997,
        jettyPort: 29998,
        totalMemory: 42949672960
      }
    ],
    errMsg: 'success'
  }
})

Mock.mock(/\/server\/nodes\?status=unhealthy/, 'get', function (options) {
  return {
    code: 0,
    data: [
      {
        id: '192.168.1.1-29999-29997',
        ip: '192.168.1.1',
        grpcPort: 29999,
        usedMemory: 0,
        preAllocatedMemory: 0,
        availableMemory: 42949672960,
        eventNumInFlush: 0,
        registrationTime: 1722057265853,
        timestamp: 1722404445850,
        tags: ['GRPC_NETTY', 'ss_v5'],
        status: 'ACTIVE',
        storageInfo: {
          '/dev/nvme1n1': {
            type: 'HDD',
            status: 'NORMAL'
          },
          '/dev/nvme0n1': {
            type: 'HDD',
            status: 'NORMAL'
          }
        },
        nettyPort: 29997,
        jettyPort: 29998,
        totalMemory: 42949672960
      }
    ],
    errMsg: 'success'
  }
})
export default Mock
