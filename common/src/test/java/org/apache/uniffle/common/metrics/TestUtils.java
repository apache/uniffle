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

package org.apache.uniffle.common.metrics;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

public class TestUtils {

  private TestUtils() {}

  public static String httpGet(String urlString) throws IOException {
    URL url = new URL(urlString);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    return getResponseStr(conn);
  }

  public static String httpPost(String urlString) throws IOException {
    return httpPost(urlString, null);
  }

  public static String httpPost(String urlString, String postData) throws IOException {
    return httpPost(urlString, postData, null);
  }

  public static String httpPost(String urlString, String postData, Map<String, String> headers)
      throws IOException {
    URL url = new URL(urlString);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setDoOutput(true);
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Content-type", "application/json");
    if (headers != null) {
      for (Map.Entry<String, String> entry : headers.entrySet()) {
        conn.setRequestProperty(entry.getKey(), entry.getValue());
      }
    }
    try (OutputStream outputStream = conn.getOutputStream()) {
      if (postData != null) {
        outputStream.write(postData.getBytes());
      }
      return getResponseStr(conn);
    }
  }

  private static String getResponseStr(HttpURLConnection conn) throws IOException {
    StringBuilder responseContent = new StringBuilder();
    InputStream inputStream =
        conn.getResponseCode() == 200 ? conn.getInputStream() : conn.getErrorStream();
    try (BufferedReader in = new BufferedReader(new InputStreamReader(inputStream))) {
      String inputLine;
      while ((inputLine = in.readLine()) != null) {
        responseContent.append(inputLine);
      }
    }
    return responseContent.toString();
  }
}
