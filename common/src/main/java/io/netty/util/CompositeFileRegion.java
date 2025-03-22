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

package io.netty.util;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

import io.netty.channel.FileRegion;

import org.apache.uniffle.common.netty.protocol.AbstractFileRegion;

public class CompositeFileRegion extends AbstractFileRegion {
  private final FileRegion[] regions;
  private long totalSize = 0;
  private long bytesTransferred = 0;

  public CompositeFileRegion(FileRegion... regions) {
    this.regions = regions;
    for (FileRegion region : regions) {
      totalSize += region.count();
    }
  }

  @Override
  public long position() {
    return bytesTransferred;
  }

  @Override
  public long count() {
    return totalSize;
  }

  @Override
  public long transferTo(WritableByteChannel target, long position) throws IOException {
    long totalBytesTransferred = 0;

    for (FileRegion region : regions) {
      if (position >= region.count()) {
        position -= region.count();
      } else {
        long currentBytesTransferred = region.transferTo(target, position);
        totalBytesTransferred += currentBytesTransferred;
        bytesTransferred += currentBytesTransferred;

        if (currentBytesTransferred < region.count() - position) {
          break;
        }
        position = 0;
      }
    }

    return totalBytesTransferred;
  }

  @Override
  public long transferred() {
    return bytesTransferred;
  }

  @Override
  public AbstractFileRegion retain() {
    super.retain();
    for (FileRegion region : regions) {
      region.retain();
    }
    return this;
  }

  @Override
  public AbstractFileRegion retain(int increment) {
    super.retain(increment);
    for (FileRegion region : regions) {
      region.retain(increment);
    }
    return this;
  }

  @Override
  public boolean release() {
    boolean released = super.release();
    for (FileRegion region : regions) {
      if (!region.release()) {
        released = false;
      }
    }
    return released;
  }

  @Override
  public boolean release(int decrement) {
    boolean released = super.release(decrement);
    for (FileRegion region : regions) {
      if (!region.release(decrement)) {
        released = false;
      }
    }
    return released;
  }

  @Override
  protected void deallocate() {
    for (FileRegion region : regions) {
      if (region instanceof AbstractReferenceCounted) {
        ((AbstractReferenceCounted) region).deallocate();
      }
    }
  }

  @Override
  public AbstractFileRegion touch() {
    super.touch();
    for (FileRegion region : regions) {
      region.touch();
    }
    return this;
  }

  @Override
  public AbstractFileRegion touch(Object hint) {
    super.touch(hint);
    for (FileRegion region : regions) {
      region.touch(hint);
    }
    return this;
  }
}
