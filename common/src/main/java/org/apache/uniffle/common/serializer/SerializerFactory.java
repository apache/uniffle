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

package org.apache.uniffle.common.serializer;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.exception.RssException;

import static org.apache.uniffle.common.config.RssBaseConf.RSS_IO_SERIALIZATIONS;

public class SerializerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(SerializerFactory.class);

  private List<Serializer> serializers = new ArrayList<Serializer>();

  RssConf conf;

  public SerializerFactory(RssConf conf) {
    this.conf = conf;
    for (String serializerName : StringUtils.split(conf.get(RSS_IO_SERIALIZATIONS), ",")) {
      add(conf, serializerName);
    }
  }

  private void add(RssConf conf, String serializerName) {
    try {
      Class<? extends Serializer> sClass =
          (Class<? extends Serializer>) ClassUtils.getClass(serializerName);
      Constructor<? extends Serializer> constructor = sClass.getConstructor(RssConf.class);
      Serializer serializer = constructor.newInstance(conf);
      serializers.add(serializer);
    } catch (Exception e) {
      LOG.warn("Construct Serialization fail, caused by ", e);
      throw new RssException(e);
    }
  }

  public Serializer getSerializer(Class c) {
    for (Serializer serializer : serializers) {
      if (serializer.accept(c)) {
        return serializer;
      }
    }
    return null;
  }
}
