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

package org.apache.uniffle.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.tez.common.RssTezConfig;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.examples.OrderedWordCount;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.uniffle.common.ClientType;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.server.buffer.ShuffleBufferType;

import static org.apache.uniffle.server.ShuffleServerConf.SERVER_MERGE_ENABLE;

public class RMTezOrderedWordCountTest extends TezIntegrationTestBase {

  private String inputPath = "rm_ordered_word_count_input";
  private String outputPath = "rm_ordered_word_count_output";
  private List<String> wordTable =
      Lists.newArrayList(
          "apple", "banana", "fruit", "cherry", "Chinese", "America", "Japan", "tomato");

  @BeforeAll
  public static void setupServers() throws Exception {
    ShuffleServerConf serverConf = new ShuffleServerConf();
    serverConf.set(SERVER_MERGE_ENABLE, true);
    serverConf.set(ShuffleServerConf.SERVER_SHUFFLE_BUFFER_TYPE, ShuffleBufferType.SKIP_LIST);
    TezIntegrationTestBase.setupServers(serverConf);
  }

  @Test
  public void orderedWordCountTest() throws Exception {
    generateInputFile();
    run();
  }

  public void run() throws Exception {
    // 1 Run original Tez examples
    TezConfiguration appConf = new TezConfiguration(miniTezCluster.getConfig());
    updateCommonConfiguration(appConf);
    runTezApp(appConf, getTestTool(), getTestArgs("origin"));
    final String originPath = getOutputDir("origin");

    // Run RSS tests with different configurations
    runRemoteMergeRssTest(ClientType.GRPC, "rss-grpc", originPath);
    runRemoteMergeRssTest(ClientType.GRPC_NETTY, "rss-netty", originPath);
  }

  private void runRemoteMergeRssTest(ClientType clientType, String testName, String originPath)
      throws Exception {
    TezConfiguration appConf = new TezConfiguration(miniTezCluster.getConfig());
    appConf.set(RssTezConfig.RSS_REMOTE_MERGE_ENABLE, "true");
    updateRssConfiguration(appConf, clientType);
    appendAndUploadRssJars(appConf);
    runTezApp(appConf, getTestTool(), getTestArgs(testName));
    verifyResults(originPath, getOutputDir(testName));
  }

  private void generateInputFile() throws Exception {
    // For ordered word count, the key of last ordered sorter is the summation of word, the value is
    // the word. So it means this key may not be unique. Because Sorter can only make sure key is
    // sorted, so the second column (word column) may be not sorted.
    // To keep pace with verifyResults, here make sure summation of word is unique number.
    FSDataOutputStream outputStream = fs.create(new Path(inputPath));
    Random random = new Random();
    Set<Integer> used = new HashSet();
    List<String> outputList = new ArrayList<>();
    int index = 0;
    while (index < wordTable.size()) {
      int summation = random.nextInt(50);
      if (used.contains(summation)) {
        continue;
      }
      used.add(summation);
      for (int i = 0; i < summation; i++) {
        outputList.add(wordTable.get(index));
      }
      index++;
    }
    Collections.shuffle(outputList);
    for (String word : outputList) {
      outputStream.writeBytes(word + "\n");
    }
    outputStream.close();
  }

  @Override
  public Tool getTestTool() {
    return new OrderedWordCount();
  }

  @Override
  public String[] getTestArgs(String uniqueOutputName) {
    return new String[] {inputPath, outputPath + "/" + uniqueOutputName, "2"};
  }

  @Override
  public String getOutputDir(String uniqueOutputName) {
    return outputPath + "/" + uniqueOutputName;
  }
}
