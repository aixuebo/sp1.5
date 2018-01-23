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

package org.apache.spark.storage

import java.util.UUID

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * Identifies a particular Block of data, usually associated with a single file.
 * 表示一个特别的数据块,通常关联一个单独的文件
 * A Block can be uniquely identified by its filename, but each type of Block has a different
 * set of keys which produce its unique name.
 * 一个数据块的唯一标示是他的文件名,但是每一个类型的数据块仅仅名字唯一即可
 *
 * If your BlockId should be serializable, be sure to add it to the BlockId.apply() method.
 */
@DeveloperApi
sealed abstract class BlockId {
  /** A globally unique identifier for this Block. Can be used for ser/de. 全局唯一的名称*/
  def name: String //是block文件的文件名

  // convenience methods 获取RDD数据块对象
  def asRDDId: Option[RDDBlockId] = if (isRDD) Some(asInstanceOf[RDDBlockId]) else None
  def isRDD: Boolean = isInstanceOf[RDDBlockId]//是RDD数据块
  def isShuffle: Boolean = isInstanceOf[ShuffleBlockId]
  def isBroadcast: Boolean = isInstanceOf[BroadcastBlockId]//是广播数据块

  override def toString: String = name
  override def hashCode: Int = name.hashCode

  //数据块的name必须相同
  override def equals(other: Any): Boolean = other match {
    case o: BlockId => getClass == o.getClass && name.equals(o.name)
    case _ => false
  }
}

//RDD使用rdd的ID和拆分ID即可
@DeveloperApi
case class RDDBlockId(rddId: Int, splitIndex: Int) extends BlockId {
  override def name: String = "rdd_" + rddId + "_" + splitIndex
}

// Format of the shuffle block ids (including data and index) should be kept in sync with
// org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getBlockData().
//一个shufffleid表示该阶段是一个shuffler阶段,那么RDD有多少个partition,就有多少个map,有多少个reduce,每一个map就有多少个输出分区,因此一个map在shuffer阶段的输出就是若干个ShuffleBlockId
@DeveloperApi
case class ShuffleBlockId(shuffleId: Int, mapId: Int, reduceId: Int) extends BlockId {
  override def name: String = "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId
}

@DeveloperApi
case class ShuffleDataBlockId(shuffleId: Int, mapId: Int, reduceId: Int) extends BlockId {
  override def name: String = "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId + ".data"
}

@DeveloperApi
case class ShuffleIndexBlockId(shuffleId: Int, mapId: Int, reduceId: Int) extends BlockId {
  override def name: String = "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId + ".index"
}

//使用广播ID以及文件域即可区分
@DeveloperApi
case class BroadcastBlockId(broadcastId: Long, field: String = "") extends BlockId {
  override def name: String = "broadcast_" + broadcastId + (if (field == "") "" else "_" + field)
}

@DeveloperApi
case class TaskResultBlockId(taskId: Long) extends BlockId {
  override def name: String = "taskresult_" + taskId
}

//代表一个stream的数据块,同一个stream持有多个数据块,但是streamId是一样的,
//uniqueid初始化的时候是持有的时间戳
@DeveloperApi
case class StreamBlockId(streamId: Int, uniqueId: Long) extends BlockId {
  override def name: String = "input-" + streamId + "-" + uniqueId
}

/** Id associated with temporary local data managed as blocks. Not serializable.
  * 临时数据块
  **/
private[spark] case class TempLocalBlockId(id: UUID) extends BlockId {
  override def name: String = "temp_local_" + id
}

/** Id associated with temporary shuffle data managed as blocks. Not serializable. */
private[spark] case class TempShuffleBlockId(id: UUID) extends BlockId {
  override def name: String = "temp_shuffle_" + id
}

// Intended only for testing purposes
private[spark] case class TestBlockId(id: String) extends BlockId {
  override def name: String = "test_" + id
}

@DeveloperApi
object BlockId {
  
  //每种类型的命名正则表达式
  val RDD = "rdd_([0-9]+)_([0-9]+)".r
  val SHUFFLE = "shuffle_([0-9]+)_([0-9]+)_([0-9]+)".r
  val SHUFFLE_DATA = "shuffle_([0-9]+)_([0-9]+)_([0-9]+).data".r
  val SHUFFLE_INDEX = "shuffle_([0-9]+)_([0-9]+)_([0-9]+).index".r
  val BROADCAST = "broadcast_([0-9]+)([_A-Za-z0-9]*)".r
  val TASKRESULT = "taskresult_([0-9]+)".r
  val STREAM = "input-([0-9]+)-([0-9]+)".r
  val TEST = "test_(.*)".r

  /** Converts a BlockId "name" String back into a BlockId.
    * RDD和SHUFFLE都是上面定义的正则表达式匹配模式
    *
    * 参数是字符串,不是整数
    */
  def apply(id: String): BlockId = id match {
    case RDD(rddId, splitIndex) =>
      RDDBlockId(rddId.toInt, splitIndex.toInt)
    case SHUFFLE(shuffleId, mapId, reduceId) =>
      ShuffleBlockId(shuffleId.toInt, mapId.toInt, reduceId.toInt)
    case SHUFFLE_DATA(shuffleId, mapId, reduceId) =>
      ShuffleDataBlockId(shuffleId.toInt, mapId.toInt, reduceId.toInt)
    case SHUFFLE_INDEX(shuffleId, mapId, reduceId) =>
      ShuffleIndexBlockId(shuffleId.toInt, mapId.toInt, reduceId.toInt)
    case BROADCAST(broadcastId, field) =>
      BroadcastBlockId(broadcastId.toLong, field.stripPrefix("_"))
    case TASKRESULT(taskId) =>
      TaskResultBlockId(taskId.toLong)
    case STREAM(streamId, uniqueId) =>
      StreamBlockId(streamId.toInt, uniqueId.toLong)
    case TEST(value) =>
      TestBlockId(value)
    case _ =>
      throw new IllegalStateException("Unrecognized BlockId: " + id)
  }
}
