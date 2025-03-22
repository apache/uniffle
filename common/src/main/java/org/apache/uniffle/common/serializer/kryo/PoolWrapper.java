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

package org.apache.uniffle.common.serializer.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.pool.KryoCallback;
import com.esotericsoftware.kryo.pool.KryoPool;

import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.common.config.RssConf;

public class PoolWrapper implements KryoPool {

  private KryoPool pool;
  private final boolean registrationRequired;
  private final boolean referenceTracking;

  public PoolWrapper(RssConf conf) {
    this.pool = newKryoPool();
    this.registrationRequired = conf.getBoolean(RssBaseConf.RSS_KRYO_REGISTRATION_REQUIRED);
    this.referenceTracking = conf.getBoolean(RssBaseConf.RSS_KRYO_REFERENCE_TRACKING);
  }

  private KryoPool newKryoPool() {
    return new Builder(() -> newKryo()).softReferences().build();
  }

  public Kryo newKryo() {
    Kryo kryo = new Kryo();
    kryo.setRegistrationRequired(this.registrationRequired);
    kryo.setReferences(this.referenceTracking);
    return kryo;
  }

  @Override
  public Kryo borrow() {
    return pool.borrow();
  }

  @Override
  public void release(Kryo kryo) {
    pool.release(kryo);
  }

  @Override
  public <T> T run(KryoCallback<T> callback) {
    return pool.run(callback);
  }

  public void reset() {
    pool = newKryoPool();
  }
}
