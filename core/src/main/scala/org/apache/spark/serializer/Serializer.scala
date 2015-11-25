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

import java.io._
import java.nio.ByteBuffer
import javax.annotation.concurrent.NotThreadSafe

import scala.reflect.ClassTag

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.annotation.{DeveloperApi, Private}
import org.apache.spark.util.{Utils, ByteBufferInputStream, NextIterator}

/**
 * :: DeveloperApi ::
 * A serializer. Because some serialization libraries are not thread safe, this class is used to
 * create [[org.apache.spark.serializer.SerializerInstance]] objects that do the actual
 * serialization and are guaranteed to only be called from one thread at a time.
 * 因为第三方序列化工具可能不是线程安全的,因此使用该类创建一个序列化实例,使用该实例去真正实例化数据,该类保证了仅仅一个线程调用该序列化对象
 *
 * Implementations of this trait should implement:
 * 要实现以下接口
 * 1. a zero-arg constructor or a constructor that accepts a [[org.apache.spark.SparkConf]]
 * as parameter. If both constructors are defined, the latter takes precedence.
 * 构造函数要接收一个无参数的构造函数,或者SparkConf为参数的构造函数
 * 2. Java serialization interface.
 *  要实现java的序列化接口
 * Note that serializers are not required to be wire-compatible across different versions of Spark.
 * 注意该序列化类不要求兼容不同的spark版本
 * They are intended to be used to serialize/de-serialize data within a single Spark application.
 * 
 */
@DeveloperApi
abstract class Serializer {

  /**
   * Default ClassLoader to use in deserialization. Implementations of [[Serializer]] should
   * make sure it is using this when set.
   */
  @volatile protected var defaultClassLoader: Option[ClassLoader] = None

  /**
   * Sets a class loader for the serializer to use in deserialization.
   *
   * @return this Serializer object
   */
  def setDefaultClassLoader(classLoader: ClassLoader): Serializer = {
    defaultClassLoader = Some(classLoader)
    this
  }

  /** Creates a new [[SerializerInstance]]. */
  def newInstance(): SerializerInstance

  /**
   * :: Private ::
   * Returns true if this serializer supports relocation of its serialized objects and false
   * otherwise. This should return true if and only if reordering the bytes of serialized objects
   * in serialization stream output is equivalent to having re-ordered those elements prior to
   * serializing them. More specifically, the following should hold if a serializer supports
   * relocation:
   *
   * {{{
   * serOut.open()
   * position = 0
   * serOut.write(obj1)
   * serOut.flush()
   * position = # of bytes writen to stream so far
   * obj1Bytes = output[0:position-1]
   * serOut.write(obj2)
   * serOut.flush()
   * position2 = # of bytes written to stream so far
   * obj2Bytes = output[position:position2-1]
   * serIn.open([obj2bytes] concatenate [obj1bytes]) should return (obj2, obj1)
   * }}}
   *
   * In general, this property should hold for serializers that are stateless and that do not
   * write special metadata at the beginning or end of the serialization stream.
   *
   * This API is private to Spark; this method should not be overridden in third-party subclasses
   * or called in user code and is subject to removal in future Spark releases.
   *
   * See SPARK-7311 for more details.
   */
  @Private
  private[spark] def supportsRelocationOfSerializedObjects: Boolean = false
}


@DeveloperApi
object Serializer {
  def getSerializer(serializer: Serializer): Serializer = {
    if (serializer == null) SparkEnv.get.serializer else serializer
  }

  def getSerializer(serializer: Option[Serializer]): Serializer = {
    serializer.getOrElse(SparkEnv.get.serializer)
  }
}


/**
 * :: DeveloperApi ::
 * An instance of a serializer, for use by one thread at a time.
 *
 * It is legal to create multiple serialization / deserialization streams from the same
 * SerializerInstance as long as those streams are all used within the same thread.
 * 定义序列化与反序列化接口
 */
@DeveloperApi
@NotThreadSafe
abstract class SerializerInstance {
  def serialize[T: ClassTag](t: T): ByteBuffer //序列化,将对象转换成ByteBuffer

  def deserialize[T: ClassTag](bytes: ByteBuffer): T //反序列化,将ByteBuffer转换成对象

  def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T //反序列化,将ByteBuffer转换成对象

  def serializeStream(s: OutputStream): SerializationStream //序列化,将OutputStream转换成SerializationStream

  def deserializeStream(s: InputStream): DeserializationStream//反序列化,将SerializationStream转换成OutputStream
}

/**
 * :: DeveloperApi ::
 * A stream for writing serialized objects.
 * 序列化对象
 */
@DeveloperApi
abstract class SerializationStream {
  /** The most general-purpose method to write an object. */
  def writeObject[T: ClassTag](t: T): SerializationStream
  /** Writes the object representing the key of a key-value pair. */
  def writeKey[T: ClassTag](key: T): SerializationStream = writeObject(key)
  /** Writes the object representing the value of a key-value pair. */
  def writeValue[T: ClassTag](value: T): SerializationStream = writeObject(value)
  def flush(): Unit
  def close(): Unit

  def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
    while (iter.hasNext) {
      writeObject(iter.next())
    }
    this
  }
}


/**
 * :: DeveloperApi ::
 * A stream for reading serialized objects.
 * 反序列化
 */
@DeveloperApi
abstract class DeserializationStream {
  /** The most general-purpose method to read an object. */
  def readObject[T: ClassTag](): T
  /** Reads the object representing the key of a key-value pair. */
  def readKey[T: ClassTag](): T = readObject[T]()
  /** Reads the object representing the value of a key-value pair. */
  def readValue[T: ClassTag](): T = readObject[T]()
  def close(): Unit

  /**
   * Read the elements of this stream through an iterator. This can only be called once, as
   * reading each element will consume data from the input source.
   */
  def asIterator: Iterator[Any] = new NextIterator[Any] {
    override protected def getNext() = {
      try {
        readObject[Any]()
      } catch {
        case eof: EOFException =>
          finished = true
          null
      }
    }

    override protected def close() {
      DeserializationStream.this.close()
    }
  }

  /**
   * Read the elements of this stream through an iterator over key-value pairs. This can only be
   * called once, as reading each element will consume data from the input source.
   */
  def asKeyValueIterator: Iterator[(Any, Any)] = new NextIterator[(Any, Any)] {
    override protected def getNext() = {
      try {
        (readKey[Any](), readValue[Any]())
      } catch {
        case eof: EOFException => {
          finished = true
          null
        }
      }
    }

    override protected def close() {
      DeserializationStream.this.close()
    }
  }
}
