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

import java.util.List;
import java.util.Random;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.tez.examples.WordCount;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TezWordCountTest extends TezIntegrationTestBase {

  private String inputPath = "word_count_input";
  private String outputPath = "word_count_output";
  private List<String> wordTable =
      Lists.newArrayList(
          "apple", "banana", "fruit", "cherry", "Chinese", "America", "Japan", "tomato");

  @BeforeAll
  public static void setupServers() throws Exception {
    TezIntegrationTestBase.setupServers(null);
  }

  @Test
  public void wordCountTest() throws Exception {
    generateInputFile();
    run();
  }

  private void generateInputFile() throws Exception {
    FSDataOutputStream outputStream = fs.create(new Path(inputPath));
    Random random = new Random();
    for (int i = 0; i < 100; i++) {
      int index = random.nextInt(wordTable.size());
      String str = wordTable.get(index) + "\n";
      outputStream.writeBytes(str);
    }
    outputStream.close();
  }

  @Override
  public Tool getTestTool() {
    return new WordCount();
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
