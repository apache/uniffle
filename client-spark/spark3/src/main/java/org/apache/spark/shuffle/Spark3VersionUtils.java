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

public class Spark3VersionUtils extends SparkVersionUtils {
  public static final String SPARK_VERSION_SHORT = package$.MODULE$.SPARK_VERSION_SHORT();

  public static boolean isSpark320() {
    return SPARK_VERSION_SHORT.equals("3.2.0");
  }

  public static boolean isSparkVersionAtLeast(String target) {
    int targetMajor = VersionUtils.majorVersion(target);
    int targetMinor = VersionUtils.minorVersion(target);

    if (MAJOR_VERSION > targetMajor) {
      return true;
    } else if (MAJOR_VERSION == targetMajor) {
      return MINOR_VERSION >= targetMinor;
    } else {
      return false;
    }
  }
}
