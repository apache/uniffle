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

package org.apache.uniffle.common.web.resource;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ConfVO {
  private Map<String, String> update = Collections.emptyMap();
  private List<String> delete = Collections.emptyList();

  public List<String> getDelete() {
    return delete;
  }

  public void setDelete(List<String> delete) {
    this.delete = delete;
  }

  public Map<String, String> getUpdate() {
    return update;
  }

  public void setUpdate(Map<String, String> update) {
    this.update = update;
  }

  @Override
  public String toString() {
    return "ConfVO{" + "update=" + update + ", delete=" + delete + '}';
  }
}
