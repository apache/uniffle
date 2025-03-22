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

package org.apache.uniffle.client.shuffle;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.Credentials;

import org.apache.uniffle.client.record.Record;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.serializer.SerializerInstance;

class RecordCollector<K extends WritableComparable, V extends Writable>
    implements ReduceContext<K, V, K, V> {

  private final Iterator<
          Map.Entry<DataOutputBuffer, List<Record<DataOutputBuffer, DataOutputBuffer>>>>
      oldRecords;
  private final List<Record<DataOutputBuffer, DataOutputBuffer>> newRecords;

  private DataOutputBuffer currentKey;
  private List<Record<DataOutputBuffer, DataOutputBuffer>> currentValues;
  private final SerializerInstance serializerInstance;
  private final Class<K> keyClass;
  private final Class<V> valueClass;

  RecordCollector(
      Iterator<Map.Entry<DataOutputBuffer, List<Record<DataOutputBuffer, DataOutputBuffer>>>>
          oldRecords,
      List<Record<DataOutputBuffer, DataOutputBuffer>> newRecords,
      SerializerInstance serializerInstance,
      Class<K> keyClass,
      Class<V> valueClass) {
    this.oldRecords = oldRecords;
    this.newRecords = newRecords;
    this.serializerInstance = serializerInstance;
    this.keyClass = keyClass;
    this.valueClass = valueClass;
  }

  @Override
  public boolean nextKey() throws IOException, InterruptedException {
    boolean ret = oldRecords.hasNext();
    if (ret) {
      Map.Entry<DataOutputBuffer, List<Record<DataOutputBuffer, DataOutputBuffer>>> entry =
          oldRecords.next();
      currentKey = entry.getKey();
      currentValues = entry.getValue();
    }
    return ret;
  }

  @Override
  public Iterable<V> getValues() throws IOException, InterruptedException {
    Iterator<Record<DataOutputBuffer, DataOutputBuffer>> currentValuesIterator =
        currentValues.iterator();
    return new Iterable<V>() {
      @Override
      public Iterator<V> iterator() {
        return new Iterator<V>() {

          @Override
          public boolean hasNext() {
            return currentValuesIterator.hasNext();
          }

          @Override
          public V next() {
            try {
              DataOutputBuffer valueBuffer = currentValuesIterator.next().getValue();
              DataInputBuffer valueInputBuffer = new DataInputBuffer();
              valueInputBuffer.reset(valueBuffer.getData(), 0, valueBuffer.getLength());
              return serializerInstance.deserialize(valueInputBuffer, valueClass);
            } catch (IOException e) {
              throw new RssException(e);
            }
          }
        };
      }
    };
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return false;
  }

  @Override
  public K getCurrentKey() throws IOException, InterruptedException {
    DataInputBuffer keyInputBuffer = new DataInputBuffer();
    keyInputBuffer.reset(this.currentKey.getData(), 0, this.currentKey.getLength());
    return serializerInstance.deserialize(keyInputBuffer, keyClass);
  }

  @Override
  public V getCurrentValue() throws IOException, InterruptedException {
    return null;
  }

  @Override
  public void write(K key, V value) throws IOException, InterruptedException {
    DataOutputBuffer keyBuffer = new DataOutputBuffer();
    DataOutputBuffer valueBuffer = new DataOutputBuffer();
    serializerInstance.serialize(key, keyBuffer);
    serializerInstance.serialize(value, valueBuffer);
    this.newRecords.add(Record.create(keyBuffer, valueBuffer));
  }

  @Override
  public OutputCommitter getOutputCommitter() {
    return null;
  }

  @Override
  public TaskAttemptID getTaskAttemptID() {
    return null;
  }

  @Override
  public void setStatus(String s) {}

  @Override
  public String getStatus() {
    return null;
  }

  @Override
  public float getProgress() {
    return 0;
  }

  @Override
  public Counter getCounter(Enum<?> anEnum) {
    return null;
  }

  @Override
  public Counter getCounter(String s, String s1) {
    return null;
  }

  @Override
  public Configuration getConfiguration() {
    return null;
  }

  @Override
  public Credentials getCredentials() {
    return null;
  }

  @Override
  public JobID getJobID() {
    return null;
  }

  @Override
  public int getNumReduceTasks() {
    return 0;
  }

  @Override
  public Path getWorkingDirectory() throws IOException {
    return null;
  }

  @Override
  public Class<?> getOutputKeyClass() {
    return null;
  }

  @Override
  public Class<?> getOutputValueClass() {
    return null;
  }

  @Override
  public Class<?> getMapOutputKeyClass() {
    return null;
  }

  @Override
  public Class<?> getMapOutputValueClass() {
    return null;
  }

  @Override
  public String getJobName() {
    return null;
  }

  @Override
  public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
    return null;
  }

  @Override
  public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
    return null;
  }

  @Override
  public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
    return null;
  }

  @Override
  public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
    return null;
  }

  @Override
  public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
    return null;
  }

  @Override
  public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
    return null;
  }

  @Override
  public RawComparator<?> getSortComparator() {
    return null;
  }

  @Override
  public String getJar() {
    return null;
  }

  @Override
  public RawComparator<?> getCombinerKeyGroupingComparator() {
    return null;
  }

  @Override
  public RawComparator<?> getGroupingComparator() {
    return null;
  }

  @Override
  public boolean getJobSetupCleanupNeeded() {
    return false;
  }

  @Override
  public boolean getTaskCleanupNeeded() {
    return false;
  }

  @Override
  public boolean getProfileEnabled() {
    return false;
  }

  @Override
  public String getProfileParams() {
    return null;
  }

  @Override
  public Configuration.IntegerRanges getProfileTaskRange(boolean b) {
    return null;
  }

  @Override
  public String getUser() {
    return null;
  }

  @Override
  public boolean getSymlink() {
    return false;
  }

  @Override
  public Path[] getArchiveClassPaths() {
    return new Path[0];
  }

  @Override
  public URI[] getCacheArchives() throws IOException {
    return new URI[0];
  }

  @Override
  public URI[] getCacheFiles() throws IOException {
    return new URI[0];
  }

  @Override
  public Path[] getLocalCacheArchives() throws IOException {
    return new Path[0];
  }

  @Override
  public Path[] getLocalCacheFiles() throws IOException {
    return new Path[0];
  }

  @Override
  public Path[] getFileClassPaths() {
    return new Path[0];
  }

  @Override
  public String[] getArchiveTimestamps() {
    return new String[0];
  }

  @Override
  public String[] getFileTimestamps() {
    return new String[0];
  }

  @Override
  public int getMaxMapAttempts() {
    return 0;
  }

  @Override
  public int getMaxReduceAttempts() {
    return 0;
  }

  @Override
  public void progress() {}
}
