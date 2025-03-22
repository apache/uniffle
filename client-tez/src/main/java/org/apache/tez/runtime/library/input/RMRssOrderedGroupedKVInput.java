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

package org.apache.tez.runtime.library.input;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.tez.common.RssTezUtils;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.AbstractLogicalInput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.ProgressFailedException;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.MemoryUpdateCallbackHandler;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.RMRssShuffle;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.RssShuffle;
import org.apache.tez.runtime.library.common.sort.impl.TezRawKeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.exception.RssException;

import static org.apache.tez.common.RssTezConfig.RSS_SHUFFLE_DESTINATION_VERTEX_ID;
import static org.apache.tez.common.RssTezConfig.RSS_SHUFFLE_SOURCE_VERTEX_ID;

/**
 * {@link RMRssOrderedGroupedKVInput} in a {@link AbstractLogicalInput} which shuffles intermediate
 * sorted data, merges them and provides key/<values> to the consumer. This is typically used to
 * bring one partition of a set of partitioned distributed data to one consumer. The shuffle
 * operation brings all partitions to one place. These partitions are assumed to be sorted and are
 * merged sorted to merge them into a single input view.
 *
 * <p>The Copy and Merge will be triggered by the initialization - which is handled by the Tez
 * framework. Input is not consumable until the Copy and Merge are complete. Methods are provided to
 * check for this, as well as to wait for completion. Attempting to get a reader on a non-complete
 * input will block.
 */
@Public
public class RMRssOrderedGroupedKVInput extends AbstractLogicalInput {

  static final Logger LOG = LoggerFactory.getLogger(RMRssOrderedGroupedKVInput.class);

  protected TezRawKeyValueIterator rawIter = null;
  protected Configuration conf;
  protected RMRssShuffle shuffle;
  protected MemoryUpdateCallbackHandler memoryUpdateCallbackHandler;
  private int shuffleId;
  private ApplicationAttemptId applicationAttemptId;
  private final BlockingQueue<Event> pendingEvents = new LinkedBlockingQueue<>();
  private long firstEventReceivedTime = -1;

  private final AtomicBoolean isStarted = new AtomicBoolean(false);

  public RMRssOrderedGroupedKVInput(InputContext inputContext, int numPhysicalInputs) {
    super(inputContext, numPhysicalInputs);
  }

  @Override
  public synchronized List<Event> initialize() throws IOException {
    this.conf = TezUtils.createConfFromUserPayload(getContext().getUserPayload());

    if (this.getNumPhysicalInputs() == 0) {
      getContext().requestInitialMemory(0L, null);
      isStarted.set(true);
      getContext().inputIsReady();
      LOG.info(
          "input fetch not required since there are 0 physical inputs for input vertex: "
              + getContext().getSourceVertexName());
      return Collections.emptyList();
    }

    long initialMemoryRequest =
        RssShuffle.getInitialMemoryRequirement(conf, getContext().getTotalMemoryAvailableToTask());
    this.memoryUpdateCallbackHandler = new MemoryUpdateCallbackHandler();
    getContext().requestInitialMemory(initialMemoryRequest, memoryUpdateCallbackHandler);

    this.conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, getContext().getWorkDirs());

    TezTaskAttemptID taskAttemptId =
        TezTaskAttemptID.fromString(
            RssTezUtils.uniqueIdentifierToAttemptId(getContext().getUniqueIdentifier()));
    TezVertexID tezVertexID = taskAttemptId.getTaskID().getVertexID();
    TezDAGID tezDAGID = tezVertexID.getDAGId();
    int sourceVertexId = this.conf.getInt(RSS_SHUFFLE_SOURCE_VERTEX_ID, -1);
    int destinationVertexId = this.conf.getInt(RSS_SHUFFLE_DESTINATION_VERTEX_ID, -1);
    assert sourceVertexId != -1;
    assert destinationVertexId != -1;
    this.shuffleId =
        RssTezUtils.computeShuffleId(tezDAGID.getId(), sourceVertexId, destinationVertexId);
    this.applicationAttemptId =
        ApplicationAttemptId.newInstance(
            getContext().getApplicationId(), getContext().getDAGAttemptNumber());
    return Collections.emptyList();
  }

  @Override
  public synchronized void start() throws IOException {
    if (!isStarted.get()) {
      memoryUpdateCallbackHandler.validateUpdateReceived();
      // Start the shuffle - copy and merge
      shuffle = createRssShuffle();
      shuffle.run();
      List<Event> pending = new LinkedList<>();
      pendingEvents.drainTo(pending);
      if (pending.size() > 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "NoAutoStart delay in processing first event: "
                  + (System.currentTimeMillis() - firstEventReceivedTime));
        }
        shuffle.handleEvents(pending);
      }
      isStarted.set(true);
    }
  }

  @VisibleForTesting
  RMRssShuffle createRssShuffle() throws IOException {
    return new RMRssShuffle(
        getContext(), conf, getNumPhysicalInputs(), shuffleId, applicationAttemptId);
  }

  @Override
  public synchronized List<Event> close() throws IOException {
    if (this.getNumPhysicalInputs() != 0 && rawIter != null) {
      rawIter.close();
    }
    if (shuffle != null) {
      shuffle.shutdown();
    }

    long dataSize =
        getContext().getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_DECOMPRESSED).getValue();
    getContext().getStatisticsReporter().reportDataSize(dataSize);
    long inputRecords =
        getContext().getCounters().findCounter(TaskCounter.REDUCE_INPUT_RECORDS).getValue();
    getContext().getStatisticsReporter().reportItemsProcessed(inputRecords);

    return Collections.emptyList();
  }

  /**
   * Get a KVReader for the Input. This method will block until the input is ready - i.e. the copy
   * and merge stages are complete. Users can use the isInputReady method to check if the input is
   * ready, which gives an indication of whether this method will block or not.
   *
   * <p>NOTE: All values for the current K-V pair must be read prior to invoking moveToNext. Once
   * moveToNext() is called, the valueIterator from the previous K-V pair will throw an Exception
   *
   * @return a KVReader over the sorted input.
   * @throws {@link IOInterruptedException} if IO was performing a blocking operation and was
   *     interrupted
   */
  @Override
  public KeyValuesReader getReader() throws Exception {
    synchronized (this) {
      if (getNumPhysicalInputs() == 0) {
        return new KeyValuesReader() {
          @Override
          public boolean next() throws IOException {
            getContext().notifyProgress();
            hasCompletedProcessing();
            completedProcessing = true;
            return false;
          }

          @Override
          public Object getCurrentKey() throws IOException {
            throw new RssException("No data available in Input");
          }

          @Override
          public Iterable<Object> getCurrentValues() throws IOException {
            throw new RssException("No data available in Input");
          }
        };
      }
    }
    shuffle.waitForEvents();
    org.apache.uniffle.client.record.reader.KeyValuesReader keyValuesReader =
        shuffle.getKeyValuesReader();
    return new KeyValuesReader() {
      @Override
      public boolean next() throws IOException {
        return keyValuesReader.next();
      }

      @Override
      public Object getCurrentKey() throws IOException {
        return keyValuesReader.getCurrentKey();
      }

      @Override
      public Iterable<Object> getCurrentValues() throws IOException {
        return keyValuesReader.getCurrentValues();
      }
    };
  }

  @Override
  public float getProgress() throws ProgressFailedException, InterruptedException {
    // TODO: add progress
    return super.getProgress();
  }

  @Override
  public void handleEvents(List<Event> inputEvents) throws IOException {
    RMRssShuffle shuffleLocalRef;
    synchronized (this) {
      if (getNumPhysicalInputs() == 0) {
        throw new RssException("No input events expected as numInputs is 0");
      }
      if (!isStarted.get()) {
        if (firstEventReceivedTime == -1) {
          firstEventReceivedTime = System.currentTimeMillis();
        }
        pendingEvents.addAll(inputEvents);
        return;
      }
      shuffleLocalRef = shuffle;
    }
    shuffleLocalRef.handleEvents(inputEvents);
  }

  private static final Set<String> CONF_KEYS = new HashSet<String>();

  static {
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_IO_FILE_BUFFER_SIZE);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_FACTOR);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMBINE_MIN_SPILLS);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMBINER_CLASS);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_USE_ASYNC_HTTP);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_FAILURES_LIMIT);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_MAX_TASK_OUTPUT_AT_ONCE);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_NOTIFY_READERROR);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_CONNECT_TIMEOUT);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_ENABLED);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_KEEP_ALIVE_MAX_CONNECTIONS);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_BUFFER_SIZE);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_ENABLE_SSL);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_VERIFY_DISK_CHECKSUM);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMTOMEM_SEGMENTS);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_ENABLE_MEMTOMEM);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_SOURCE_ATTEMPT_ABORT_LIMIT);
    CONF_KEYS.add(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_ACCEPTABLE_HOST_FETCH_FAILURE_FRACTION);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MIN_FAILURES_PER_HOST);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MAX_STALL_TIME_FRACTION);
    CONF_KEYS.add(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MAX_ALLOWED_FAILED_FETCH_ATTEMPT_FRACTION);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MIN_REQUIRED_PROGRESS_FRACTION);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FAILED_CHECK_SINCE_LAST_COMPLETION);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCHER_USE_SHARED_POOL);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_INPUT_POST_MERGE_BUFFER_PERCENT);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_GROUP_COMPARATOR_CLASS);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_COMPARATOR_CLASS);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_SECONDARY_COMPARATOR_CLASS);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_CONVERT_USER_PAYLOAD_TO_HISTORY_TEXT);
    CONF_KEYS.add(TezConfiguration.TEZ_COUNTERS_MAX);
    CONF_KEYS.add(TezConfiguration.TEZ_COUNTERS_GROUP_NAME_MAX_LENGTH);
    CONF_KEYS.add(TezConfiguration.TEZ_COUNTERS_COUNTER_NAME_MAX_LENGTH);
    CONF_KEYS.add(TezConfiguration.TEZ_COUNTERS_MAX_GROUPS);
    CONF_KEYS.add(TezRuntimeConfiguration.TEZ_RUNTIME_CLEANUP_FILES_ON_INTERRUPT);
    CONF_KEYS.add(Constants.TEZ_RUNTIME_TASK_MEMORY);
    CONF_KEYS.add(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID);
  }

  @InterfaceAudience.Private
  public static Set<String> getConfigurationKeySet() {
    return Collections.unmodifiableSet(CONF_KEYS);
  }
}
