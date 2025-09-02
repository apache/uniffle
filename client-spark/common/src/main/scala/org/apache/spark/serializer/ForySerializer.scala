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

package org.apache.spark.serializer

import org.apache.fory.config.{CompatibleMode, Language}
import org.apache.fory.io.ForyInputStream
import org.apache.fory.{Fory, ThreadLocalFory}
import org.apache.spark.internal.Logging

import java.io.{InputStream, OutputStream, Serializable}
import java.nio.ByteBuffer
import scala.reflect.ClassTag

@SerialVersionUID(1L)
class ForySerializer extends org.apache.spark.serializer.Serializer
  with Logging
  with Serializable {

  override def newInstance(): SerializerInstance = new ForySerializerInstance()

  override def supportsRelocationOfSerializedObjects: Boolean = true

}

class ForySerializerInstance extends org.apache.spark.serializer.SerializerInstance {

  private val fury = Fory.builder()
    .withLanguage(Language.JAVA)
    .withRefTracking(true)
    .withCompatibleMode(CompatibleMode.SCHEMA_CONSISTENT)
    .requireClassRegistration(false)
    .buildThreadLocalFory()

  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    val bytes = fury.serialize(t.asInstanceOf[AnyRef])
    ByteBuffer.wrap(bytes)
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    fury.deserialize(bytes).asInstanceOf[T]
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = {
    // Fury handles class loading internally, so we can use the standard deserialize method
    deserialize[T](bytes)
  }

  override def serializeStream(s: OutputStream): SerializationStream = {
    new ForySerializationStream(fury, s)
  }

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new ForyDeserializationStream(fury, s)
  }
}

class ForySerializationStream(fury: ThreadLocalFory, outputStream: OutputStream)
  extends org.apache.spark.serializer.SerializationStream {

  private var closed = false

  override def writeObject[T: ClassTag](t: T): SerializationStream = {
    if (closed) {
      throw new IllegalStateException("Stream is closed")
    }
    fury.serialize(outputStream, t)
    this
  }

  override def flush(): Unit = {
    if (!closed) {
      outputStream.flush()
    }
  }

  override def close(): Unit = {
    if (!closed) {
      try {
        outputStream.close()
      } finally {
        closed = true
      }
    }
  }
}

class ForyDeserializationStream(fury: ThreadLocalFory, inputStream: InputStream)
  extends org.apache.spark.serializer.DeserializationStream {

  private var closed = false
  private val foryStream = new ForyInputStream(inputStream)

  override def readObject[T: ClassTag](): T = {
    if (closed) {
      throw new IllegalStateException("Stream is closed")
    }
    fury.deserialize(foryStream).asInstanceOf[T]
  }

  override def close(): Unit = {
    if (!closed) {
      try {
        foryStream.close()
      } finally {
        closed = true
      }
    }
  }
}