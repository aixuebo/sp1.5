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

package org.apache.spark.streaming.dstream

import scala.reflect.ClassTag

import org.apache.spark.rdd.{BlockRDD, RDD}
import org.apache.spark.storage.BlockId
import org.apache.spark.streaming.rdd.WriteAheadLogBackedBlockRDD
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.scheduler.rate.RateEstimator
import org.apache.spark.streaming.scheduler.{ReceivedBlockInfo, RateController, StreamInputInfo}
import org.apache.spark.streaming.util.WriteAheadLogUtils
import org.apache.spark.streaming.{StreamingContext, Time}

/**
 * Abstract class for defining any [[org.apache.spark.streaming.dstream.InputDStream]]
 * that has to start a receiver on worker nodes to receive external data.
 * 该对象可以在worker节点上接收数据
 *
 * Specific implementations of ReceiverInputDStream must
 * define `the getReceiver()` function that gets the receiver object of type
 * [[org.apache.spark.streaming.receiver.Receiver]] that will be sent
 * to the workers to receive data.
 * 当特别指定ReceiverInputDStream类型的实现类时,必须实现getReceiver方法, 获取一个Receiver对象将会被发送到worker节点去真正接收数据
 *
 * @param ssc_ Streaming context that will execute this input stream
 * @tparam T Class type of the object of this stream,注意,T也可以是一个元组对象,表示最终接收到的信息是什么对象类型的
 *
 */
abstract class ReceiverInputDStream[T: ClassTag](@transient ssc_ : StreamingContext)
  extends InputDStream[T](ssc_) {

  /**
   * Asynchronously maintains & sends new rate limits to the receiver through the receiver tracker.
   */
  override protected[streaming] val rateController: Option[RateController] = {
    if (RateController.isBackPressureEnabled(ssc.conf)) {
      Some(new ReceiverRateController(id, RateEstimator.create(ssc.conf, ssc.graph.batchDuration)))
    } else {
      None
    }
  }

  /**
   * Gets the receiver object that will be sent to the worker nodes
   * to receive data. This method needs to defined by any specific implementation
   * of a ReceiverInputDStream.
   * 定义如何接收数据
   */
  def getReceiver(): Receiver[T]

  // Nothing to start or stop as both taken care of by the ReceiverTracker.
  def start() {}

  def stop() {}

  /**
   * Generates RDDs with blocks received by the receiver of this stream.
   * 返回RDD内容
   **/
  override def compute(validTime: Time): Option[RDD[T]] = {
    val blockRDD = {

      if (validTime < graph.startTime) {//基本上不应该发生,因此返回空的RDD
        // If this is called for any time before the start time of the context,
        // then this returns an empty RDD. This may happen when recovering from a
        // driver failure without any write ahead log to recover pre-failure data.
        new BlockRDD[T](ssc.sc, Array.empty)
      } else {
        // Otherwise, ask the tracker for all the blocks that have been allocated to this stream
        // for this batch
        val receiverTracker = ssc.scheduler.receiverTracker
        val blockInfos = receiverTracker.getBlocksOfBatch(validTime).getOrElse(id, Seq.empty) //返回该时间点接收到的该streaming对应的数据块信息

        // Register the input blocks information into InputInfoTracker
        val inputInfo = StreamInputInfo(id, blockInfos.flatMap(_.numRecords).sum) //获取该数据块对应的记录数
        ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)

        // Create the BlockRDD
        createBlockRDD(validTime, blockInfos)
      }
    }
    Some(blockRDD)
  }

  //接收到的数据块集合
  private[streaming] def createBlockRDD(time: Time, blockInfos: Seq[ReceivedBlockInfo]): RDD[T] = {

    if (blockInfos.nonEmpty) {//说明接收到的数据块是非空的
      val blockIds = blockInfos.map { _.blockId.asInstanceOf[BlockId] }.toArray //获取数据块ID集合

      // Are WAL record handles present with all the blocks
      val areWALRecordHandlesPresent = blockInfos.forall { _.walRecordHandleOption.nonEmpty } //所有的数据块都有日志

      if (areWALRecordHandlesPresent) {//存在日志内容
        // If all the blocks have WAL record handle, then create a WALBackedBlockRDD 说明所有的数据块都有日志
        val isBlockIdValid = blockInfos.map { _.isBlockIdValid() }.toArray
        val walRecordHandles = blockInfos.map { _.walRecordHandleOption.get }.toArray
        new WriteAheadLogBackedBlockRDD[T](
          ssc.sparkContext, blockIds, walRecordHandles, isBlockIdValid)
      } else {//说明不是所有的数据块都有日志
        // Else, create a BlockRDD. However, if there are some blocks with WAL info but not
        // others then that is unexpected and log a warning accordingly.
        if (blockInfos.find(_.walRecordHandleOption.nonEmpty).nonEmpty) { //是否有一部分数据块有日志
          if (WriteAheadLogUtils.enableReceiverLog(ssc.conf)) {
            logError("Some blocks do not have Write Ahead Log information; " +
              "this is unexpected and data may not be recoverable after driver failures")
          } else {
            logWarning("Some blocks have Write Ahead Log information; this is unexpected")
          }
        }
        val validBlockIds = blockIds.filter { id =>
          ssc.sparkContext.env.blockManager.master.contains(id) //有效的数据块ID
        }
        if (validBlockIds.size != blockIds.size) {
          logWarning("Some blocks could not be recovered as they were not found in memory. " +
            "To prevent such data loss, enabled Write Ahead Log (see programming guide " +
            "for more details.")
        }
        new BlockRDD[T](ssc.sc, validBlockIds) //有效的数据块组装成返回值
      }
    } else {//说明数据块没有
      // If no block is ready now, creating WriteAheadLogBackedBlockRDD or BlockRDD
      // according to the configuration
      if (WriteAheadLogUtils.enableReceiverLog(ssc.conf)) {//返回空的结果集
        new WriteAheadLogBackedBlockRDD[T](
          ssc.sparkContext, Array.empty, Array.empty, Array.empty)
      } else {
        new BlockRDD[T](ssc.sc, Array.empty) //返回空的结果集
      }
    }
  }

  /**
   * A RateController that sends the new rate to receivers, via the receiver tracker.
   */
  private[streaming] class ReceiverRateController(id: Int, estimator: RateEstimator)
      extends RateController(id, estimator) {
    override def publish(rate: Long): Unit =
      ssc.scheduler.receiverTracker.sendRateUpdate(id, rate)
  }
}

