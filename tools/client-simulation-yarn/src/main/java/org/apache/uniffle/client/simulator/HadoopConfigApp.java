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

package org.apache.uniffle.client.simulator;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;

public class HadoopConfigApp {

  private final Configuration conf;
  private Map<String, String> localConf = new HashMap<>();

  public HadoopConfigApp(Configuration conf) {
    this.conf = conf;
  }

  public int run(String[] args) {
    Options options = new Options();

    Option confOption =
        OptionBuilder.withArgName("conf")
            .withLongOpt("conf")
            .hasArg()
            .withDescription("Path to the configuration file with key=value pairs")
            .create("c");
    options.addOption(confOption);

    Option defineOption =
        OptionBuilder.withArgName("key=value")
            .withLongOpt("define")
            .hasArgs(2)
            .withValueSeparator()
            .withDescription("Define a key-value pair configuration")
            .create("D");
    options.addOption(defineOption);

    CommandLineParser parser = new GnuParser();
    CommandLine cmd;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.err.println("Failed to parse command line arguments: " + e.getMessage());
      return 1;
    }

    // handle --conf option
    if (cmd.hasOption("conf")) {
      String confFile = cmd.getOptionValue("conf");
      try {
        loadConfigurationFromFile(localConf, confFile);
      } catch (IOException e) {
        System.err.println("Error loading configuration from file: " + e.getMessage());
        return 1;
      }
    }

    // handle -D option
    if (cmd.hasOption("define")) {
      Properties properties = cmd.getOptionProperties("D");
      for (Map.Entry<Object, Object> entry : properties.entrySet()) {
        localConf.put(entry.getKey().toString(), entry.getValue().toString());
      }
    }

    for (Map.Entry<String, String> entry : localConf.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    return 0;
  }

  protected void loadConfigurationFromFile(Map<String, String> map, String filePath)
      throws IOException {
    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(new FileInputStream(filePath), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.isEmpty() || line.startsWith("#")) {
          continue; // Skip empty lines and comments
        }
        int eq = line.indexOf('=');
        if (eq > 0) {
          String key = line.substring(0, eq).trim();
          String value = line.substring(eq + 1).trim();
          map.put(key, value);
        } else {
          System.err.println("Invalid configuration line: " + line);
        }
      }
    }
  }

  public Map<String, String> getLocalConf() {
    return localConf;
  }

  protected static String writeConfigurationToString(Map<String, String> map) {
    StringBuilder stringBuilder = new StringBuilder();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      stringBuilder.append(entry.getKey()).append("=").append(entry.getValue()).append("\n");
    }
    return stringBuilder.toString().trim();
  }

  public static void main(String[] args) {
    HadoopConfigApp app = new HadoopConfigApp(new Configuration());
    int exitCode = app.run(args);
    System.exit(exitCode);
  }
}
