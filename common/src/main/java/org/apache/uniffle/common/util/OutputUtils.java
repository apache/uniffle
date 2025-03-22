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

package org.apache.uniffle.common.util;

import java.util.List;

public class OutputUtils {

  /**
   * Convert a list of number into a segment string.
   *
   * @param numbers the list of number
   * @return the segment string
   */
  public static String listToSegment(List<Integer> numbers) {
    return listToSegment(numbers, 1, Long.MAX_VALUE);
  }

  /**
   * Convert a list of number into a segment string.
   *
   * @param numbers the list of number
   * @param threshold if the segment size is larger than this threshold, it will be converted into a
   *     segment string
   * @return the segment string
   */
  public static String listToSegment(List<Integer> numbers, long threshold) {
    return listToSegment(numbers, threshold, Long.MAX_VALUE);
  }

  /**
   * Convert a list of number into a segment string.
   *
   * @param numbers the list of number
   * @param threshold if the segment size is larger than this threshold, it will be converted into a
   *     segment string
   * @param limit the maximum number of elements in the segment
   * @return the segment string
   */
  public static String listToSegment(List<Integer> numbers, long threshold, long limit) {
    if (numbers == null || numbers.isEmpty()) {
      return "[]";
    }
    if (threshold < 1 || numbers.size() <= threshold) {
      return numbers.toString();
    }

    StringBuilder result = new StringBuilder();
    int start = numbers.get(0);
    int end = start;

    long rangeCount = 0;
    for (int i = 1; i < numbers.size(); i++) {
      if (numbers.get(i) == numbers.get(i - 1) + 1) {
        end = numbers.get(i);
      } else {
        if (rangeCount < limit) {
          appendRange(result, start, end);
        }
        rangeCount++;
        start = numbers.get(i);
        end = start;
      }
    }
    rangeCount++;
    if (rangeCount < limit) {
      // Append the last range
      appendRange(result, start, end);
    } else {
      result.append("...").append(rangeCount - limit).append(" more ranges...");
    }

    return result.toString();
  }

  private static void appendRange(StringBuilder result, int start, int end) {
    if (result.length() > 0) {
      result.append(", ");
    }
    if (start == end) {
      result.append(start);
    } else {
      result.append("[").append(start).append("~").append(end).append("]");
    }
  }
}
