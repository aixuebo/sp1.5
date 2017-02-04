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

package org.apache.spark.streaming.scheduler

import org.apache.spark.storage.StreamBlockId
import org.apache.spark.streaming.receiver.{ReceivedBlockStoreResult, WriteAheadLogBasedStoreResult}
import org.apache.spark.streaming.util.WriteAheadLogRecordHandle

/** Information about blocks received by the receiver
  * 接收到的一个数据块的信息,是一个基本单位,一个streaming每次接受,都产生一个该对象
  **/
private[streaming] case class ReceivedBlockInfo(
    streamId: Int,//哪个stream
    numRecords: Option[Long],//该数据块有多少条数据
    metadataOption: Option[Any],//该数据块的元数据信息内容.比如是kafka的话,会有topic-partition以及offset等信息
    blockStoreResult: ReceivedBlockStoreResult //存储后的结果
  ) {

  require(numRecords.isEmpty || numRecords.get >= 0, "numRecords must not be negative")

  @volatile private var _isBlockIdValid = true //数据块是否有效

  def blockId: StreamBlockId = blockStoreResult.blockId

  def walRecordHandleOption: Option[WriteAheadLogRecordHandle] = { //数据块对应的头文件
    blockStoreResult match {
      case walStoreResult: WriteAheadLogBasedStoreResult => Some(walStoreResult.walRecordHandle)
      case _ => None
    }
  }

  /** Is the block ID valid, that is, is the block present in the Spark executors. */
  def isBlockIdValid(): Boolean = _isBlockIdValid

  /**
   * Set the block ID as invalid. This is useful when it is known that the block is not present
   * in the Spark executors.
    * 设置数据块失效
   */
  def setBlockIdInvalid(): Unit = {
    _isBlockIdValid = false
  }
}

