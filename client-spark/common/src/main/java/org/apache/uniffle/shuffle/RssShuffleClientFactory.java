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

package org.apache.uniffle.shuffle;

import java.util.function.Supplier;

import org.apache.uniffle.client.api.ShuffleManagerClient;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.client.impl.ShuffleWriteClientImpl;

public class RssShuffleClientFactory extends ShuffleClientFactory {

  private static final RssShuffleClientFactory INSTANCE = new RssShuffleClientFactory();

  public static RssShuffleClientFactory getInstance() {
    return INSTANCE;
  }

  public ShuffleWriteClient createShuffleWriteClient(ExtendWriteClientBuilder builder) {
    return builder.build();
  }

  public static ExtendWriteClientBuilder<?> newWriteBuilder() {
    return new ExtendWriteClientBuilder();
  }

  public static class ExtendWriteClientBuilder<T extends ExtendWriteClientBuilder<T>>
      extends WriteClientBuilder<T> {
    private boolean blockIdSelfManagedEnabled;
    private Supplier<ShuffleManagerClient> managerClientSupplier;

    public boolean isBlockIdSelfManagedEnabled() {
      return blockIdSelfManagedEnabled;
    }

    public Supplier<ShuffleManagerClient> getManagerClientSupplier() {
      return managerClientSupplier;
    }

    public T managerClientSupplier(Supplier<ShuffleManagerClient> managerClientSupplier) {
      this.managerClientSupplier = managerClientSupplier;
      return self();
    }

    public T blockIdSelfManagedEnabled(boolean blockIdSelfManagedEnabled) {
      this.blockIdSelfManagedEnabled = blockIdSelfManagedEnabled;
      return self();
    }

    @Override
    public ShuffleWriteClientImpl build() {
      if (blockIdSelfManagedEnabled) {
        return new BlockIdSelfManagedShuffleWriteClient(this);
      }
      return super.build();
    }
  }
}
