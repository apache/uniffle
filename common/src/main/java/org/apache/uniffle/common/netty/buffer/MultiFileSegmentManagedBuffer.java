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

package org.apache.uniffle.common.netty.buffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.FileRegion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.netty.protocol.CompositeFileRegion;

/** A wrapper of multiple {@link FileSegmentManagedBuffer}, used for combine shuffle index files. */
public class MultiFileSegmentManagedBuffer extends ManagedBuffer {

  private static final Logger LOG = LoggerFactory.getLogger(MultiFileSegmentManagedBuffer.class);
  private final List<ManagedBuffer> managedBuffers;

  public MultiFileSegmentManagedBuffer(List<ManagedBuffer> managedBuffers) {
    this.managedBuffers = managedBuffers;
  }

  @Override
  public int size() {
    return managedBuffers.stream().mapToInt(ManagedBuffer::size).sum();
  }

  @Override
  public ByteBuf byteBuf() {
    return Unpooled.wrappedBuffer(this.nioByteBuffer());
  }

  @Override
  public ByteBuffer nioByteBuffer() {
    ByteBuffer merged = ByteBuffer.allocate(size());
    for (ManagedBuffer managedBuffer : managedBuffers) {
      ByteBuffer buffer = managedBuffer.nioByteBuffer();
      merged.put(buffer.slice());
    }
    merged.flip();
    return merged;
  }

  @Override
  public ManagedBuffer retain() {
    return this;
  }

  @Override
  public ManagedBuffer release() {
    return this;
  }

  @Override
  public Object convertToNetty() {
    List<FileRegion> fileRegions = new ArrayList<>(managedBuffers.size());
    for (ManagedBuffer managedBuffer : managedBuffers) {
      Object object = managedBuffer.convertToNetty();
      if (object instanceof FileRegion) {
        fileRegions.add((FileRegion) object);
      }
    }
    return new CompositeFileRegion(fileRegions.toArray(new FileRegion[0]));
  }
}
