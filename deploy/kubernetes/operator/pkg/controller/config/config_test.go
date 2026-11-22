/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"sigs.k8s.io/controller-runtime/pkg/cache"
)

func TestCompleteManagerOptions(t *testing.T) {
	for _, tt := range []struct {
		name               string
		namespace          string
		expectedNamespaces map[string]cache.Config
	}{
		{
			name:               "all namespaces",
			namespace:          "",
			expectedNamespaces: nil,
		},
		{
			name:               "one namespace",
			namespace:          "kube-system",
			expectedNamespaces: map[string]cache.Config{"kube-system": {}},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := &Config{
				syncPeriod:  time.Hour,
				retryPeriod: 3 * time.Second,
				namespace:   tt.namespace,
			}
			c.completeManagerOptions()

			assertion := assert.New(t)
			if assertion.NotNil(c.ManagerOptions.Cache.SyncPeriod) {
				assertion.Equal(time.Hour, *c.ManagerOptions.Cache.SyncPeriod)
			}
			if assertion.NotNil(c.ManagerOptions.RetryPeriod) {
				assertion.Equal(3*time.Second, *c.ManagerOptions.RetryPeriod)
			}
			assertion.Equal(tt.expectedNamespaces, c.ManagerOptions.Cache.DefaultNamespaces)
		})
	}
}
