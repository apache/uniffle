package org.apache.spark.serializer

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import scala.collection.mutable
import scala.reflect.ClassTag

class ForySerializerTest extends AnyFunSuite with Matchers {

  test("ForySerializer should create new instance") {
    val serializer = new ForySerializer()
    val instance = serializer.newInstance()
    
    instance should not be null
    instance shouldBe a[ForySerializerInstance]
  }

  test("ForySerializer should support relocation of serialized objects") {
    val serializer = new ForySerializer()
    serializer.supportsRelocationOfSerializedObjects shouldBe true
  }

  test("ForySerializerInstance should serialize and deserialize simple primitive types") {
    val instance = new ForySerializerInstance()
    
    // Test String
    testSerializeDeserialize(instance, "Hello, Fory!")
    
    // Test Integer  
    testSerializeDeserialize(instance, Integer.valueOf(42))
    
    // Test Long
    testSerializeDeserialize(instance, java.lang.Long.valueOf(123456789L))
    
    // Test Double
    testSerializeDeserialize(instance, java.lang.Double.valueOf(3.14159))
    
    // Test Boolean
    testSerializeDeserialize(instance, java.lang.Boolean.valueOf(true))
    testSerializeDeserialize(instance, java.lang.Boolean.valueOf(false))
  }

  test("ForySerializerInstance should serialize and deserialize simple collections") {
    val instance = new ForySerializerInstance()
    
    // Test simple Java ArrayList
    val javaList = new java.util.ArrayList[String]()
    javaList.add("apple")
    javaList.add("banana")
    javaList.add("cherry")
    testSerializeDeserialize(instance, javaList)
    
    // Test simple Java HashMap
    val javaMap = new java.util.HashMap[String, String]()
    javaMap.put("key1", "value1")
    javaMap.put("key2", "value2")
    testSerializeDeserialize(instance, javaMap)
  }

  test("ForySerializerInstance should serialize and deserialize simple case class") {
    val instance = new ForySerializerInstance()

    // Simple class without complex dependencies
    val data = new SimpleData("test", 123)

    testSerializeDeserialize(instance, data)
  }

  test("ForySerializationStream should serialize and deserialize simple objects") {
    val instance = new ForySerializerInstance()
    val baos = new ByteArrayOutputStream()
    val stream = instance.serializeStream(baos)

    // Write simple objects only
    val objects = List("Hello", Integer.valueOf(123), java.lang.Boolean.valueOf(true))
    objects.foreach { obj =>
      stream.writeObject(obj)(getClassTag(obj))
    }
    stream.flush()
    stream.close()

    // Read back the objects
    val bais = new ByteArrayInputStream(baos.toByteArray)
    val deserStream = instance.deserializeStream(bais)

    val readObjects = mutable.ListBuffer[Any]()
    try {
      while (true) {
        readObjects += deserStream.readObject[Any]()
      }
    } catch {
      case _: java.io.EOFException => // Expected when reaching end
    }
    deserStream.close()

    readObjects.toList should contain theSameElementsInOrderAs objects
  }

  test("ForySerializationStream should handle empty stream") {
    val instance = new ForySerializerInstance()
    val baos = new ByteArrayOutputStream()
    val stream = instance.serializeStream(baos)
    stream.flush()
    stream.close()

    val bais = new ByteArrayInputStream(baos.toByteArray)
    val deserStream = instance.deserializeStream(bais)

    intercept[java.io.EOFException] {
      deserStream.readObject[String]()
    }

    deserStream.close()
  }

  test("ForySerializationStream should handle stream operations after close") {
    val instance = new ForySerializerInstance()
    val baos = new ByteArrayOutputStream()
    val stream = instance.serializeStream(baos)

    stream.close()

    // Writing after close should throw exception
    intercept[IllegalStateException] {
      stream.writeObject("test")
    }
  }

  test("ForyDeserializationStream should handle stream operations after close") {
    val instance = new ForySerializerInstance()
    val bais = new ByteArrayInputStream(Array.empty[Byte])
    val deserStream = instance.deserializeStream(bais)

    deserStream.close()

    // Reading after close should throw exception
    intercept[IllegalStateException] {
      deserStream.readObject[String]()
    }
  }

  test("ForySerializerInstance should handle null values") {
    val instance = new ForySerializerInstance()

    val serialized = instance.serialize[AnyRef](null)
    val deserialized = instance.deserialize[AnyRef](serialized)
    deserialized should be(null)
  }

  test("ForySerializerInstance should handle byte arrays") {
    val instance = new ForySerializerInstance()

    val byteArray = Array[Byte](1, 2, 3, 4, 5)
    testSerializeDeserialize(instance, byteArray)
  }

  test("ForySerializerInstance should handle large strings") {
    val instance = new ForySerializerInstance()

    // Create a large string
    val largeString = "x" * 10000
    testSerializeDeserialize(instance, largeString)
  }

  private def testSerializeDeserialize[T](instance: ForySerializerInstance, obj: T)(implicit ct: ClassTag[T]): Unit = {
    val serialized = instance.serialize(obj)
    val deserialized = instance.deserialize[T](serialized)

    if (obj != null && obj.getClass.isArray) {
      // Special handling for arrays since they don't implement equals properly
      if (obj.isInstanceOf[Array[Byte]]) {
        obj.asInstanceOf[Array[Byte]] should equal(deserialized.asInstanceOf[Array[Byte]])
      } else {
        obj.asInstanceOf[Array[_]].toList should equal(deserialized.asInstanceOf[Array[_]].toList)
      }
    } else {
      deserialized should equal(obj)
    }
  }

  private def getClassTag[T](obj: T): ClassTag[T] = {
    if (obj == null) {
      ClassTag.AnyRef.asInstanceOf[ClassTag[T]]
    } else {
      ClassTag(obj.getClass).asInstanceOf[ClassTag[T]]
    }
  }
}
// SimpleData is a normal class with equals/hashCode overridden for test assertions
class SimpleData(val name: String, val value: Int) {
  override def equals(other: Any): Boolean = other match {
    case that: SimpleData => this.name == that.name && this.value == that.value
    case _ => false
  }
  override def hashCode(): Int = {
    31 * name.hashCode + value
  }
  override def toString: String = s"SimpleData($name, $value)"
}