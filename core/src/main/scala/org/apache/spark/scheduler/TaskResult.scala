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

package org.apache.spark.scheduler

import java.io._
import java.nio.ByteBuffer

import scala.collection.Map
import scala.collection.mutable

import org.apache.spark.SparkEnv
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.storage.BlockId
import org.apache.spark.util.Utils

// Task result. Also contains updates to accumulator variables.
private[spark] sealed trait TaskResult[T]

/** A reference to a DirectTaskResult that has been stored in the worker's BlockManager.
  * 间接的一个引用,已经被存储在worker的数据块系统里面了
  **/
private[spark] case class IndirectTaskResult[T](blockId: BlockId, size: Int)
  extends TaskResult[T] with Serializable

/** A TaskResult that contains the task's return value and accumulator updates.
  *  一个任务的结果,包含任务的返回值以及更新的累加器
  **/
private[spark]
class DirectTaskResult[T](var valueBytes: ByteBuffer, var accumUpdates: Map[Long, Any],
    var metrics: TaskMetrics)
  extends TaskResult[T] with Externalizable {

  private var valueObjectDeserialized = false //true表示该对象已经被反序列化了
  private var valueObject: T = _ //反序列化后的对象

  def this() = this(null.asInstanceOf[ByteBuffer], null, null)

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {

    out.writeInt(valueBytes.remaining);//valueBytes字节数组还有多少个位置尚未被使用
    Utils.writeByteBuffer(valueBytes, out) //将valueBytes的内容写入选择前valueBytes.remaining个写入到out中

    //写入统计信息
    out.writeInt(accumUpdates.size)
    for ((key, value) <- accumUpdates) {
      out.writeLong(key)
      out.writeObject(value)
    }
    //写入最后数据
    out.writeObject(metrics)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {

    val blen = in.readInt()
    val byteVal = new Array[Byte](blen)
    in.readFully(byteVal)//从输入流中读取byteVal个字节
    valueBytes = ByteBuffer.wrap(byteVal)

    val numUpdates = in.readInt
    if (numUpdates == 0) {
      accumUpdates = null
    } else {
      val _accumUpdates = mutable.Map[Long, Any]()
      for (i <- 0 until numUpdates) {
        _accumUpdates(in.readLong()) = in.readObject()
      }
      accumUpdates = _accumUpdates
    }
    metrics = in.readObject().asInstanceOf[TaskMetrics]
    valueObjectDeserialized = false
  }

  /**
   * When `value()` is called at the first time, it needs to deserialize `valueObject` from
   * `valueBytes`. It may cost dozens of seconds for a large instance. So when calling `value` at
   * the first time, the caller should avoid to block other threads.
   *
   * After the first time, `value()` is trivial and just returns the deserialized `valueObject`.
   */
  def value(): T = {
    if (valueObjectDeserialized) {//true表示该对象已经被反序列化了
      valueObject
    } else {
      // This should not run when holding a lock because it may cost dozens of seconds for a large
      // value.
      val resultSer = SparkEnv.get.serializer.newInstance()
      valueObject = resultSer.deserialize(valueBytes) //对字节数组进行反序列化
      valueObjectDeserialized = true //说明反序列化成功
      valueObject
    }
  }
}
