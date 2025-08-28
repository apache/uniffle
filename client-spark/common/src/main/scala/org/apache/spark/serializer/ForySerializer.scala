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

import org.apache.fory.{Fory, ThreadLocalFory}
import org.apache.fory.config.{CompatibleMode, Language}
import org.apache.spark.internal.Logging

import java.io.{InputStream, OutputStream, Serializable}
import java.nio.ByteBuffer
import scala.reflect.ClassTag

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
    .buildThreadLocalFory();

  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    val bytes = fury.serialize(t.asInstanceOf[AnyRef])
    ByteBuffer.wrap(bytes)
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    val array = if (bytes.hasArray) {
      val offset = bytes.arrayOffset() + bytes.position()
      val length = bytes.remaining()
      java.util.Arrays.copyOfRange(bytes.array(), offset, offset + length)
    } else {
      val array = new Array[Byte](bytes.remaining())
      bytes.get(array)
      array
    }
    fury.deserialize(array).asInstanceOf[T]
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

  private val out = outputStream
  private var closed = false

  override def writeObject[T: ClassTag](t: T): SerializationStream = {
    if (closed) {
      throw new IllegalStateException("Stream is closed")
    }
    
    val bytes = fury.serialize(t.asInstanceOf[AnyRef])
    // Write length first, then data
    writeInt(bytes.length)
    out.write(bytes)
    this
  }

  private def writeInt(value: Int): Unit = {
    out.write((value >>> 24) & 0xFF)
    out.write((value >>> 16) & 0xFF)
    out.write((value >>> 8) & 0xFF)
    out.write(value & 0xFF)
  }

  override def flush(): Unit = {
    if (!closed) {
      out.flush()
    }
  }

  override def close(): Unit = {
    if (!closed) {
      try {
        out.close()
      } finally {
        closed = true
      }
    }
  }
}

class ForyDeserializationStream(fury: ThreadLocalFory, inputStream: InputStream)
  extends org.apache.spark.serializer.DeserializationStream {

  private val in = inputStream
  private var closed = false

  override def readObject[T: ClassTag](): T = {
    if (closed) {
      throw new IllegalStateException("Stream is closed")
    }

    try {
      val length = readInt()
      if (length < 0) {
        throw new java.io.EOFException("Reached end of stream")
      }
      
      val bytes = new Array[Byte](length)
      var bytesRead = 0
      while (bytesRead < length) {
        val read = in.read(bytes, bytesRead, length - bytesRead)
        if (read == -1) {
          throw new java.io.EOFException("Unexpected end of stream")
        }
        bytesRead += read
      }
      
      fury.deserialize(bytes).asInstanceOf[T]
    } catch {
      case _: java.io.EOFException =>
        throw new java.io.EOFException("Reached end of stream")
    }
  }

  private def readInt(): Int = {
    val b1 = in.read()
    val b2 = in.read()
    val b3 = in.read()
    val b4 = in.read()

    if ((b1 | b2 | b3 | b4) < 0) {
      throw new java.io.EOFException()
    }

    (b1 << 24) + (b2 << 16) + (b3 << 8) + b4
  }

  override def close(): Unit = {
    if (!closed) {
      try {
        in.close()
      } finally {
        closed = true
      }
    }
  }
}