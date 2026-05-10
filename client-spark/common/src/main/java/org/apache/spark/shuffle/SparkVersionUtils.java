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

package org.apache.spark.shuffle;

import org.apache.spark.package$;
import org.apache.spark.util.VersionUtils;

public class SparkVersionUtils {
  public static final String SPARK_VERSION = package$.MODULE$.SPARK_VERSION();
  public static final int MAJOR_VERSION;
  public static final int MINOR_VERSION;

  static {
    int majorVersion;
    int minorVersion;
    try {
      majorVersion = VersionUtils.majorVersion(SPARK_VERSION);
      minorVersion = VersionUtils.minorVersion(SPARK_VERSION);
    } catch (Exception e) {
      // ignore, just in case some wild spark version is passed.
      majorVersion = -1;
      minorVersion = -1;
    }
    MAJOR_VERSION = majorVersion;
    MINOR_VERSION = minorVersion;
  }

  public static boolean isSpark2() {
    return MAJOR_VERSION == 2;
  }

  public static boolean isSpark3() {
    return MAJOR_VERSION == 3;
  }

  public static boolean isSpark4() {
    return MAJOR_VERSION == 4;
  }

  public static boolean isSpark320() {
    return SPARK_VERSION.matches("^3.2.0([^\\d].*)?$");
  }
}
