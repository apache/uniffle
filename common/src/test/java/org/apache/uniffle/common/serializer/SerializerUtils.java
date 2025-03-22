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

package org.apache.uniffle.common.serializer;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Comparator;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.merger.Segment;
import org.apache.uniffle.common.merger.StreamedSegment;
import org.apache.uniffle.common.records.RecordsWriter;

public class SerializerUtils {

  public static class SomeClass {

    private String value;

    public SomeClass() {}

    public static SomeClass create(String value) {
      SomeClass sc = new SomeClass();
      sc.value = value;
      return sc;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SomeClass someClass = (SomeClass) o;
      return Objects.equal(value, someClass.value);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(value);
    }

    @Override
    public String toString() {
      return "SomeClass{" + "value='" + value + '\'' + '}';
    }
  }

  public static Object genData(Class tClass, int index) {
    if (tClass.equals(Text.class)) {
      return new Text(String.format("key%08d", index));
    } else if (tClass.equals(IntWritable.class)) {
      return new IntWritable(index);
    } else if (tClass.equals(String.class)) {
      return String.format("key%05d", index);
    } else if (tClass.equals(Integer.class)) {
      return Integer.valueOf(index);
    } else if (tClass.equals(SomeClass.class)) {
      return SomeClass.create(String.format("key%05d", index));
    } else if (tClass.equals(int.class)) {
      return index;
    }
    return null;
  }

  public static Class<?> getClassByName(String className) throws ClassNotFoundException {
    if (className.equals("int")) {
      return int.class;
    } else {
      return Class.forName(className);
    }
  }

  public static Comparator getComparator(Class tClass) {
    if (tClass.equals(Text.class)) {
      return new Text.Comparator();
    } else if (tClass.equals(IntWritable.class)) {
      return new IntWritable.Comparator();
    } else if (tClass.equals(String.class)) {
      return new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
          int i1 = Integer.valueOf(o1.substring(3));
          int i2 = Integer.valueOf(o2.substring(3));
          return i1 - i2;
        }
      };
    } else if (tClass.equals(Integer.class)) {
      return new Comparator<Integer>() {
        @Override
        public int compare(Integer o1, Integer o2) {
          return o1 - o2;
        }
      };
    } else if (tClass.equals(SomeClass.class)) {
      return new Comparator<SomeClass>() {
        @Override
        public int compare(SomeClass o1, SomeClass o2) {
          int i1 = Integer.valueOf(o1.value.substring(3));
          int i2 = Integer.valueOf(o2.value.substring(3));
          return i1 - i2;
        }
      };
    } else if (tClass.equals(int.class)) {
      return new Comparator<Integer>() {
        @Override
        public int compare(Integer o1, Integer o2) {
          return o1 - o2;
        }
      };
    }
    return null;
  }

  public static Segment genMemorySegment(
      RssConf rssConf,
      Class keyClass,
      Class valueClass,
      long blockId,
      int start,
      int interval,
      int length)
      throws IOException {
    return genMemorySegment(
        rssConf, keyClass, valueClass, blockId, start, interval, length, false, false);
  }

  public static Segment genMemorySegment(
      RssConf rssConf,
      Class keyClass,
      Class valueClass,
      long blockId,
      int start,
      int interval,
      int length,
      boolean raw,
      boolean direct)
      throws IOException {
    ByteBuf byteBuf =
        genSortedRecordBuffer(rssConf, keyClass, valueClass, start, interval, length, 1, direct);
    return new StreamedSegment(
        rssConf,
        SerInputStream.newInputStream(byteBuf),
        blockId,
        keyClass,
        valueClass,
        byteBuf.readableBytes(),
        raw);
  }

  public static Segment genFileSegment(
      RssConf rssConf,
      Class keyClass,
      Class valueClass,
      long blockId,
      int start,
      int interval,
      int length,
      File tmpDir,
      boolean raw)
      throws IOException {
    File file = new File(tmpDir, "data." + start);
    ByteBuf byteBuffer =
        genSortedRecordBuffer(rssConf, keyClass, valueClass, start, interval, length, 1);
    OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file));
    while (byteBuffer.readableBytes() > 0) {
      outputStream.write(byteBuffer.readByte());
    }
    outputStream.close();
    return new StreamedSegment(
        rssConf,
        SerInputStream.newInputStream(file),
        blockId,
        keyClass,
        valueClass,
        file.length(),
        raw);
  }

  public static ByteBuf genSortedRecordBuffer(
      RssConf rssConf,
      Class keyClass,
      Class valueClass,
      int start,
      int interval,
      int length,
      int replica)
      throws IOException {
    return genSortedRecordBuffer(
        rssConf, keyClass, valueClass, start, interval, length, replica, false);
  }

  public static ByteBuf genSortedRecordBuffer(
      RssConf rssConf,
      Class keyClass,
      Class valueClass,
      int start,
      int interval,
      int length,
      int replica,
      boolean direct)
      throws IOException {
    SerOutputStream output = new DynBufferSerOutputStream();
    RecordsWriter writer = new RecordsWriter(rssConf, output, keyClass, valueClass, false, false);
    writer.init();
    for (int i = 0; i < length; i++) {
      for (int j = 0; j < replica; j++) {
        writer.append(
            SerializerUtils.genData(keyClass, start + i * interval),
            SerializerUtils.genData(valueClass, start + i * interval));
      }
    }
    writer.close();
    ByteBuf heapBuf = output.toByteBuf();
    if (direct) {
      ByteBuf directBuf = Unpooled.directBuffer(heapBuf.readableBytes());
      directBuf.writeBytes(heapBuf);
      return directBuf;
    } else {
      return heapBuf;
    }
  }
}
