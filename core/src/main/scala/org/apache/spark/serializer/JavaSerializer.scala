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

import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.ByteBufferInputStream
import org.apache.spark.util.Utils

//java版本的序列化与反序列化
/**
 * @out 序列化后的对象输出到该输出流中
 * @counterReset 序列化多少个对象后,要进行重置
 */
private[spark] class JavaSerializationStream(
    out: OutputStream,
    counterReset: Int,//多少个元素后进行重置
    extraDebugInfo: Boolean) //是否要debug详细信息
  extends SerializationStream {
  private val objOut = new ObjectOutputStream(out) //java的序列化对象
  private var counter = 0 //当前已经序列化了多少个对象

  /**
   * Calling reset to avoid memory leak:
   * http://stackoverflow.com/questions/1281549/memory-leak-traps-in-the-java-standard-api
   * But only call it every 100th time to avoid bloated serialization streams (when
   * the stream 'resets' object class descriptions have to be re-written)
   * 将一个java系列化对象写入到输出流中
   */
  def writeObject[T: ClassTag](t: T): SerializationStream = {
    try {
      objOut.writeObject(t)
    } catch {
      case e: NotSerializableException if extraDebugInfo =>
        throw SerializationDebugger.improveException(t, e)
    }
    counter += 1
    if (counterReset > 0 && counter >= counterReset) {//重置流
      objOut.reset()
      counter = 0
    }
    this
  }

  def flush() { objOut.flush() }
  def close() { objOut.close() }
}

//JAVA对象反序列化  输入参数是有序列化内容的字节数组流
private[spark] class JavaDeserializationStream(in: InputStream, loader: ClassLoader)
  extends DeserializationStream {

  private val objIn = new ObjectInputStream(in) {
    override def resolveClass(desc: ObjectStreamClass): Class[_] = {//序列化成什么类型的对象,即javaBean
      // scalastyle:off classforname
      Class.forName(desc.getName, false, loader)
      // scalastyle:on classforname
    }
  }

  def readObject[T: ClassTag](): T = objIn.readObject().asInstanceOf[T] //读取一个对象
  def close() { objIn.close() }
}

//序列化
private[spark] class JavaSerializerInstance(
    counterReset: Int,
    extraDebugInfo: Boolean,
    defaultClassLoader: ClassLoader)
  extends SerializerInstance {

  override def serialize[T: ClassTag](t: T): ByteBuffer = {//将对象序列化成字节数组,存储在ByteBuffer中
    val bos = new ByteArrayOutputStream()
    val out = serializeStream(bos)
    out.writeObject(t)//序列化
    out.close()
    ByteBuffer.wrap(bos.toByteArray)//返回序列化结果
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {//将字节数组反序列化成对象
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis)//反序列化
    in.readObject()//返回对象
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = {//将字节数组反序列化成对象
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis, loader)
    in.readObject()
  }

  //序列化
  override def serializeStream(s: OutputStream): SerializationStream = {
    new JavaSerializationStream(s, counterReset, extraDebugInfo)
  }

  //反序列化
  override def deserializeStream(s: InputStream): DeserializationStream = {
    new JavaDeserializationStream(s, defaultClassLoader)
  }

  //反序列化
  def deserializeStream(s: InputStream, loader: ClassLoader): DeserializationStream = {
    new JavaDeserializationStream(s, loader)
  }
}

/**
 * :: DeveloperApi ::
 * A Spark serializer that uses Java's built-in serialization.
 *
 * Note that this serializer is not guaranteed to be wire-compatible across different versions of
 * Spark. It is intended to be used to serialize/de-serialize data within a single
 * Spark application.
 */
@DeveloperApi
class JavaSerializer(conf: SparkConf) extends Serializer with Externalizable {
  /**
  当序列化方式使用JavaSerializer时，序列化器会缓存对象以免写入冗余的数据，但这会使垃圾回收器停止对这些对象进行垃圾收集。所以当使用reset序列化器后就会使垃圾回收器重新收集那些旧对象。该值设置为-1则表示禁止周期性的reset，默认情况下每100个对象就会被reset一次序列化器
    */
  private var counterReset = conf.getInt("spark.serializer.objectStreamReset", 100)
  private var extraDebugInfo = conf.getBoolean("spark.serializer.extraDebugInfo", true)

  protected def this() = this(new SparkConf())  // For deserialization only

  override def newInstance(): SerializerInstance = {
    val classLoader = defaultClassLoader.getOrElse(Thread.currentThread.getContextClassLoader)
    new JavaSerializerInstance(counterReset, extraDebugInfo, classLoader)
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    out.writeInt(counterReset)
    out.writeBoolean(extraDebugInfo)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    counterReset = in.readInt()
    extraDebugInfo = in.readBoolean()
  }
}
