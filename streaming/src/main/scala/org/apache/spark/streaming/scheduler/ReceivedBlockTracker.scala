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

import java.nio.ByteBuffer

import scala.collection.mutable
import scala.language.implicitConversions

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.streaming.Time
import org.apache.spark.streaming.util.{WriteAheadLog, WriteAheadLogUtils}
import org.apache.spark.util.{Clock, Utils}
import org.apache.spark.{Logging, SparkConf}

/** Trait representing any event in the ReceivedBlockTracker that updates its state. */
private[streaming] sealed trait ReceivedBlockTrackerLogEvent

private[streaming] case class BlockAdditionEvent(receivedBlockInfo: ReceivedBlockInfo) //表示一个streaming接收到了一个数据信息
  extends ReceivedBlockTrackerLogEvent

private[streaming] case class BatchAllocationEvent(time: Time, allocatedBlocks: AllocatedBlocks) //表示此时刻产生了一个批处理
  extends ReceivedBlockTrackerLogEvent

private[streaming] case class BatchCleanupEvent(times: Seq[Time]) //清理此时刻前的数据信息
  extends ReceivedBlockTrackerLogEvent


/** Class representing the blocks of all the streams allocated to a batch
  * 该类表示一个批处理
  **/
private[streaming]
case class AllocatedBlocks(streamIdToAllocatedBlocks: Map[Int, Seq[ReceivedBlockInfo]]) {//参数是该批处理包含的每一个streaming对应的接收到的数据信息
  def getBlocksOfStream(streamId: Int): Seq[ReceivedBlockInfo] = {//就是map的get方法,返回streaming在该批处理中对应的数据内容
    streamIdToAllocatedBlocks.getOrElse(streamId, Seq.empty)
  }
}

/**
 * Class that keep track of all the received blocks, and allocate them to batches
 * when required. All actions taken by this class can be saved to a write ahead log
 * (if a checkpoint directory has been provided), so that the state of the tracker
 * (received blocks and block-to-batch allocations) can be recovered after driver failure.
 * 覆盖开启了checkpoint,则行为的状态都会记录在log日志中,因此可以在driver失败后,记录仍然可以被恢复
 * Note that when any instance of this class is created with a checkpoint directory,
 * it will try reading events from logs in the directory.
  *
  * 该类功能
  * 1.收集每一个streaming发过来的数据信息内容
  * 2.如果需要,将接收的事件记录到日志系统中,方便后续系统恢复
  * 3.产生真正意义的批处理对象
 */
private[streaming] class ReceivedBlockTracker(
    conf: SparkConf,
    hadoopConf: Configuration,
    streamIds: Seq[Int],//要处理的数据流集合
    clock: Clock,
    recoverFromWriteAheadLog: Boolean,//是否从日志系统中进行恢复,true表示此时要进行数据恢复
    checkpointDirOption: Option[String]) //是否记录日志,后续从日志系统中恢复数据
  extends Logging {

  //表示一个队列
  private type ReceivedBlockQueue = mutable.Queue[ReceivedBlockInfo]

  //每一个streaming分配一个队列--此时还尚未分配批处理
  private val streamIdToUnallocatedBlockQueues = new mutable.HashMap[Int, ReceivedBlockQueue]

  //每一次时间对应的分配的数据块映射--记录已经分配好的批处理信息
  private val timeToAllocatedBlocks = new mutable.HashMap[Time, AllocatedBlocks]

  //创建日志系统
  private val writeAheadLogOption = createWriteAheadLog()

  private var lastAllocatedBatchTime: Time = null //最后一次时间

  // Recover block information from write ahead logs
  if (recoverFromWriteAheadLog) {//从日志系统中恢复数据信息
    recoverPastEvents()
  }

  /** Add received block. This event will get written to the write ahead log (if enabled). 可能记录到日志系统中
    * 添加一个streaming对应接收到的信息
    **/
  def addBlock(receivedBlockInfo: ReceivedBlockInfo): Boolean = synchronized {
    try {
      writeToLog(BlockAdditionEvent(receivedBlockInfo))//写日志
      getReceivedBlockQueue(receivedBlockInfo.streamId) += receivedBlockInfo //存放到队列中
      logDebug(s"Stream ${receivedBlockInfo.streamId} received " +
        s"block ${receivedBlockInfo.blockStoreResult.blockId}")
      true
    } catch {
      case e: Exception =>
        logError(s"Error adding block $receivedBlockInfo", e)
        false
    }
  }

  /**
   * Allocate all unallocated blocks to the given batch.
   * This event will get written to the write ahead log (if enabled). 可能记录到日志系统中
    * 分配一个批处理
    *
   */
  def allocateBlocksToBatch(batchTime: Time): Unit = synchronized {
    if (lastAllocatedBatchTime == null || batchTime > lastAllocatedBatchTime) {//上一次批处理时间为null,说明是第一次,或者此时时间比上一次时间大了,则进行批处理
      //获取该批处理的数据内容
      val streamIdToBlocks = streamIds.map { streamId =>
          (streamId, getReceivedBlockQueue(streamId).dequeueAll(x => true)) //将所有数据进都出队列,x表示元素内容,无论元素内容是什么都返回true,因为dequeueAll就是需要一个filter函数作为参数
      }.toMap

      val allocatedBlocks = AllocatedBlocks(streamIdToBlocks)//组成一个批处理对象

      writeToLog(BatchAllocationEvent(batchTime, allocatedBlocks)) //写日志
      timeToAllocatedBlocks(batchTime) = allocatedBlocks //记录分配的批处理
      lastAllocatedBatchTime = batchTime//设置最后一次批处理时间
      allocatedBlocks //返回批处理对象
    } else {//不进行批处理
      // This situation occurs when:
      // 1. WAL is ended with BatchAllocationEvent, but without BatchCleanupEvent,
      // possibly processed batch job or half-processed batch job need to be processed again,
      // so the batchTime will be equal to lastAllocatedBatchTime.
      // 2. Slow checkpointing makes recovered batch time older than WAL recovered
      // lastAllocatedBatchTime.
      // This situation will only occurs in recovery time.
      logInfo(s"Possibly processed batch $batchTime need to be processed again in WAL recovery")
    }
  }

  /** Get the blocks allocated to the given batch.
    * 获取在batchTime时间点的批处理中,每一个streaming对应的数据信息
    **/
  def getBlocksOfBatch(batchTime: Time): Map[Int, Seq[ReceivedBlockInfo]] = synchronized {
    timeToAllocatedBlocks.get(batchTime).map { _.streamIdToAllocatedBlocks }.getOrElse(Map.empty)
  }

  /** Get the blocks allocated to the given batch and stream.
    * 获取在一个时间点--一个streaming对对应的数据内容
    **/
  def getBlocksOfBatchAndStream(batchTime: Time, streamId: Int): Seq[ReceivedBlockInfo] = {
    synchronized {
      timeToAllocatedBlocks.get(batchTime).map {
        _.getBlocksOfStream(streamId)
      }.getOrElse(Seq.empty)
    }
  }

  /** Check if any blocks are left to be allocated to batches.
    * true 表示有未分配到批处理的数据信息
    **/
  def hasUnallocatedReceivedBlocks: Boolean = synchronized {
    !streamIdToUnallocatedBlockQueues.values.forall(_.isEmpty)
  }

  /**
   * Get blocks that have been added but not yet allocated to any batch. This method
   * is primarily used for testing.
    * 获取该streaming中未分配到批处理的数据信息
   */
  def getUnallocatedBlocks(streamId: Int): Seq[ReceivedBlockInfo] = synchronized {
    getReceivedBlockQueue(streamId).toSeq
  }

  /**
   * Clean up block information of old batches. If waitForCompletion is true, this method
   * returns only after the files are cleaned up.
    * 清理已经存在的批处理数据数据
    * 参数waitForCompletion 表示是否等待全部清理完
   */
  def cleanupOldBatches(cleanupThreshTime: Time, waitForCompletion: Boolean): Unit = synchronized {
    require(cleanupThreshTime.milliseconds < clock.getTimeMillis())
    val timesToCleanup = timeToAllocatedBlocks.keys.filter { _ < cleanupThreshTime }.toSeq //过滤,找到要清理的数据批处理集合
    logInfo("Deleting batches " + timesToCleanup)
    writeToLog(BatchCleanupEvent(timesToCleanup)) //写日志
    timeToAllocatedBlocks --= timesToCleanup //内存减少
    writeAheadLogOption.foreach(_.clean(cleanupThreshTime.milliseconds, waitForCompletion)) //日志去清理
  }

  /** Stop the block tracker. */
  def stop() {
    writeAheadLogOption.foreach { _.close() }
  }

  /**
   * Recover all the tracker actions from the write ahead logs to recover the state (unallocated
   * and allocated block info) prior to failure.
    * 从日志系统中恢复数据信息
   */
  private def recoverPastEvents(): Unit = synchronized {
    // Insert the recovered block information
    def insertAddedBlock(receivedBlockInfo: ReceivedBlockInfo) { //恢复添加数据信息
      logTrace(s"Recovery: Inserting added block $receivedBlockInfo")
      receivedBlockInfo.setBlockIdInvalid()
      getReceivedBlockQueue(receivedBlockInfo.streamId) += receivedBlockInfo
    }

    // Insert the recovered block-to-batch allocations and clear the queue of received blocks
    // (when the blocks were originally allocated to the batch, the queue must have been cleared).
    //添加一个批处理
    def insertAllocatedBatch(batchTime: Time, allocatedBlocks: AllocatedBlocks) {
      logTrace(s"Recovery: Inserting allocated batch for time $batchTime to " +
        s"${allocatedBlocks.streamIdToAllocatedBlocks}")
      streamIdToUnallocatedBlockQueues.values.foreach { _.clear() } //清空内容
      lastAllocatedBatchTime = batchTime //设置最后批处理时间
      timeToAllocatedBlocks.put(batchTime, allocatedBlocks) //追加批处理
    }

    // Cleanup the batch allocations 恢复清理的批处理
    def cleanupBatches(batchTimes: Seq[Time]) {
      logTrace(s"Recovery: Cleaning up batches $batchTimes")
      timeToAllocatedBlocks --= batchTimes //内存映射修改即可,不需要真正去清理日志
    }

    //读取日志
    writeAheadLogOption.foreach { writeAheadLog =>
      logInfo(s"Recovering from write ahead logs in ${checkpointDirOption.get}")
      import scala.collection.JavaConversions._
      //读取所有的日志内容
      writeAheadLog.readAll().foreach { byteBuffer => //日志内容
        logTrace("Recovering record " + byteBuffer)
        Utils.deserialize[ReceivedBlockTrackerLogEvent](
          byteBuffer.array, Thread.currentThread().getContextClassLoader) match { //对日志进行反序列化,反序列化的结果是ReceivedBlockTrackerLogEvent对象
          case BlockAdditionEvent(receivedBlockInfo) =>
            insertAddedBlock(receivedBlockInfo)
          case BatchAllocationEvent(time, allocatedBlocks) =>
            insertAllocatedBatch(time, allocatedBlocks)
          case BatchCleanupEvent(batchTimes) =>
            cleanupBatches(batchTimes)
        }
      }
    }
  }

  /** Write an update to the tracker to the write ahead log
    * 向日志中记录事件
    **/
  private def writeToLog(record: ReceivedBlockTrackerLogEvent) {
    if (isWriteAheadLogEnabled) {
      logDebug(s"Writing to log $record")
      writeAheadLogOption.foreach { logManager =>
        logManager.write(ByteBuffer.wrap(Utils.serialize(record)), clock.getTimeMillis())
      }
    }
  }

  /** Get the queue of received blocks belonging to a particular stream
    * 获取队列中数据
    **/
  private def getReceivedBlockQueue(streamId: Int): ReceivedBlockQueue = {
    streamIdToUnallocatedBlockQueues.getOrElseUpdate(streamId, new ReceivedBlockQueue)
  }

  /** Optionally create the write ahead log manager only if the feature is enabled */
  private def createWriteAheadLog(): Option[WriteAheadLog] = {
    checkpointDirOption.map { checkpointDir =>
      val logDir = ReceivedBlockTracker.checkpointDirToLogDir(checkpointDirOption.get) //产生日志文件
      WriteAheadLogUtils.createLogForDriver(conf, logDir, hadoopConf)
    }
  }

  /** Check if the write ahead log is enabled. This is only used for testing purposes.
    * true 表示需要记录日志事件
    **/
  private[streaming] def isWriteAheadLogEnabled: Boolean = writeAheadLogOption.nonEmpty
}

private[streaming] object ReceivedBlockTracker {
  def checkpointDirToLogDir(checkpointDir: String): String = {
    new Path(checkpointDir, "receivedBlockMetadata").toString
  }
}
