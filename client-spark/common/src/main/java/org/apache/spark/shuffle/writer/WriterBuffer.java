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

package org.apache.spark.shuffle.writer;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.UnsafeInput;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.serializer.kryo.KryoSerializerInstance;

public class WriterBuffer {

  private static final Logger LOG = LoggerFactory.getLogger(WriterBuffer.class);
  protected long copyTime = 0;
  protected byte[] buffer;
  protected int bufferSize;
  protected int nextOffset = 0;
  protected List<WrappedBuffer> buffers = Lists.newArrayList();
  protected int dataLength = 0;
  protected int memoryUsed = 0;
  protected long recordCount = 0;

  private long sortTime = 0;
  private final List<Record> records = new ArrayList();

  public WriterBuffer(int bufferSize) {
    this.bufferSize = bufferSize;
  }

  public void addRecord(byte[] recordBuffer, int length) {
    if (askForMemory(length)) {
      // buffer has data already, add buffer to list
      if (nextOffset > 0) {
        buffers.add(new WrappedBuffer(buffer, nextOffset));
        nextOffset = 0;
      }
      int newBufferSize = Math.max(length, bufferSize);
      buffer = new byte[newBufferSize];
      memoryUsed += newBufferSize;
    }

    try {
      System.arraycopy(recordBuffer, 0, buffer, nextOffset, length);
    } catch (Exception e) {
      LOG.error(
          "Unexpected exception for System.arraycopy, length["
              + length
              + "], nextOffset["
              + nextOffset
              + "], bufferSize["
              + bufferSize
              + "]");
      throw e;
    }

    nextOffset += length;
    dataLength += length;
    recordCount++;
  }

  public void addRecord(byte[] recordBuffer, int keyLength, int valueLength) {
    this.addRecord(recordBuffer, keyLength + valueLength);
    this.records.add(
        new Record(
            this.buffers.size(), nextOffset - keyLength - valueLength, keyLength, valueLength));
  }

  public boolean askForMemory(long length) {
    return buffer == null || nextOffset + length > bufferSize;
  }

  public byte[] getData() {
    byte[] data = new byte[dataLength];
    int offset = 0;
    long start = System.currentTimeMillis();
    for (WrappedBuffer wrappedBuffer : buffers) {
      System.arraycopy(wrappedBuffer.getBuffer(), 0, data, offset, wrappedBuffer.getSize());
      offset += wrappedBuffer.getSize();
    }
    // nextOffset is the length of current buffer used
    System.arraycopy(buffer, 0, data, offset, nextOffset);
    copyTime += System.currentTimeMillis() - start;
    return data;
  }

  public byte[] getData(KryoSerializerInstance instance, Comparator comparator) {
    if (comparator != null) {
      // deserialized key
      long start = System.currentTimeMillis();
      Kryo derKryo = null;
      try {
        derKryo = instance.borrowKryo();
        Input input = new UnsafeInput();
        for (Record record : records) {
          byte[] bytes =
              record.index == this.buffers.size() ? buffer : this.buffers.get(record.index).buffer;
          input.setBuffer(bytes, record.offset, record.keyLength);
          record.key = derKryo.readClassAndObject(input);
        }
      } catch (Throwable e) {
        throw new RssException(e);
      } finally {
        instance.releaseKryo(derKryo);
      }

      // sort by key
      this.records.sort(
          new Comparator<Record>() {
            @Override
            public int compare(Record r1, Record r2) {
              return comparator.compare(r1.key, r2.key);
            }
          });
      sortTime += System.currentTimeMillis() - start;
    }

    // write
    long start = System.currentTimeMillis();
    byte[] data = new byte[dataLength];
    int offset = 0;
    for (Record record : records) {
      byte[] bytes =
          record.index == buffers.size() ? buffer : buffers.get(record.index).getBuffer();
      System.arraycopy(bytes, record.offset, data, offset, record.keyLength + record.valueLength);
      offset += record.keyLength + record.valueLength;
    }
    copyTime += System.currentTimeMillis() - start;
    return data;
  }

  public int getDataLength() {
    return dataLength;
  }

  public long getCopyTime() {
    return copyTime;
  }

  public int getMemoryUsed() {
    return memoryUsed;
  }

  public long getRecordCount() {
    return recordCount;
  }

  public long getSortTime() {
    return sortTime;
  }

  private static final class WrappedBuffer {

    byte[] buffer;
    int size;

    WrappedBuffer(byte[] buffer, int size) {
      this.buffer = buffer;
      this.size = size;
    }

    public byte[] getBuffer() {
      return buffer;
    }

    public int getSize() {
      return size;
    }
  }

  private static final class Record {
    private final int index;
    private final int offset;
    private final int keyLength;
    private final int valueLength;
    private Object key = null;

    Record(int keyIndex, int offset, int keyLength, int valueLength) {
      this.index = keyIndex;
      this.offset = offset;
      this.keyLength = keyLength;
      this.valueLength = valueLength;
    }
  }
}
