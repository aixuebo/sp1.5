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

package org.apache.spark.streaming.receiver

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.{existentials, postfixOps}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.storage._
import org.apache.spark.streaming.receiver.WriteAheadLogBasedBlockHandler._
import org.apache.spark.streaming.util.{WriteAheadLogRecordHandle, WriteAheadLogUtils}
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils}
import org.apache.spark.{Logging, SparkConf, SparkException}

/** Trait that represents the metadata related to storage of blocks
  * 表示存储数据块后的返回结果
  **/
private[streaming] trait ReceivedBlockStoreResult {
  // Any implementation of this trait will store a block id 去存储该数据块
  def blockId: StreamBlockId
  // Any implementation of this trait will have to return the number of records 存储的数据块有多少条记录
  def numRecords: Option[Long]
}

/** Trait that represents a class that handles the storage of blocks received by receiver */
private[streaming] trait ReceivedBlockHandler {

  /** Store a received block with the given block id and return related metadata
    * 存储一个数据块  以及该数据块对应的内容
    **/
  def storeBlock(blockId: StreamBlockId, receivedBlock: ReceivedBlock): ReceivedBlockStoreResult

  /** Cleanup old blocks older than the given threshold time
    * 清理老的数据块信息
    **/
  def cleanupOldBlocks(threshTime: Long)
}


/**
 * Implementation of [[org.apache.spark.streaming.receiver.ReceivedBlockStoreResult]]
 * that stores the metadata related to storage of blocks using
 * [[org.apache.spark.streaming.receiver.BlockManagerBasedBlockHandler]]
 */
private[streaming] case class BlockManagerBasedStoreResult(
      blockId: StreamBlockId, numRecords: Option[Long])
  extends ReceivedBlockStoreResult


/**
 * Implementation of a [[org.apache.spark.streaming.receiver.ReceivedBlockHandler]] which
 * stores the received blocks into a block manager with the specified storage level.
 */
private[streaming] class BlockManagerBasedBlockHandler(
    blockManager: BlockManager, storageLevel: StorageLevel)
  extends ReceivedBlockHandler with Logging {

  def storeBlock(blockId: StreamBlockId, block: ReceivedBlock): ReceivedBlockStoreResult = {

    var numRecords = None: Option[Long]

    val putResult: Seq[(BlockId, BlockStatus)] = block match {
      case ArrayBufferBlock(arrayBuffer) => //说明接收的数据块是数组
        numRecords = Some(arrayBuffer.size.toLong) //行数就是数组的大小
        blockManager.putIterator(blockId, arrayBuffer.iterator, storageLevel,
          tellMaster = true) //存储数组内容
      case IteratorBlock(iterator) => //说明接收的是一个迭代器
        val countIterator = new CountingIterator(iterator) //该迭代器可以计算记录行数
        val putResult = blockManager.putIterator(blockId, countIterator, storageLevel,
          tellMaster = true)
        numRecords = countIterator.count
        putResult
      case ByteBufferBlock(byteBuffer) => //如果接收到的是一个字节数组
        blockManager.putBytes(blockId, byteBuffer, storageLevel, tellMaster = true)
      case o =>
        throw new SparkException(
          s"Could not store $blockId to block manager, unexpected block type ${o.getClass.getName}")
    }
    if (!putResult.map { _._1 }.contains(blockId)) {
      throw new SparkException(
        s"Could not store $blockId to block manager with storage level $storageLevel")
    }
    BlockManagerBasedStoreResult(blockId, numRecords)
  }

  def cleanupOldBlocks(threshTime: Long) {
    // this is not used as blocks inserted into the BlockManager are cleared by DStream's clearing
    // of BlockRDDs.
  }
}


/**
 * Implementation of [[org.apache.spark.streaming.receiver.ReceivedBlockStoreResult]]
 * that stores the metadata related to storage of blocks using
 * [[org.apache.spark.streaming.receiver.WriteAheadLogBasedBlockHandler]]
 */
private[streaming] case class WriteAheadLogBasedStoreResult(
    blockId: StreamBlockId,
    numRecords: Option[Long],
    walRecordHandle: WriteAheadLogRecordHandle
  ) extends ReceivedBlockStoreResult


/**
 * Implementation of a [[org.apache.spark.streaming.receiver.ReceivedBlockHandler]] which
 * stores the received blocks in both, a write ahead log and a block manager.
 * 既存储到数据块中,也存储到log日志中
 */
private[streaming] class WriteAheadLogBasedBlockHandler(
    blockManager: BlockManager,
    streamId: Int,
    storageLevel: StorageLevel,
    conf: SparkConf,
    hadoopConf: Configuration,
    checkpointDir: String,
    clock: Clock = new SystemClock
  ) extends ReceivedBlockHandler with Logging {

  private val blockStoreTimeout = conf.getInt(
    "spark.streaming.receiver.blockStoreTimeout", 30).seconds

  private val effectiveStorageLevel = {
    if (storageLevel.deserialized) {
      logWarning(s"Storage level serialization ${storageLevel.deserialized} is not supported when" +
        s" write ahead log is enabled, change to serialization false")
    }
    if (storageLevel.replication > 1) {
      logWarning(s"Storage level replication ${storageLevel.replication} is unnecessary when " +
        s"write ahead log is enabled, change to replication 1")
    }

    StorageLevel(storageLevel.useDisk, storageLevel.useMemory, storageLevel.useOffHeap, false, 1)
  }

  if (storageLevel != effectiveStorageLevel) {
    logWarning(s"User defined storage level $storageLevel is changed to effective storage level " +
      s"$effectiveStorageLevel when write ahead log is enabled")
  }

  // Write ahead log manages
  private val writeAheadLog = WriteAheadLogUtils.createLogForReceiver(
    conf, checkpointDirToLogDir(checkpointDir, streamId), hadoopConf)

  // For processing futures used in parallel block storing into block manager and write ahead log
  // # threads = 2, so that both writing to BM and WAL can proceed in parallel
  implicit private val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonFixedThreadPool(2, this.getClass.getSimpleName))

  /**
   * This implementation stores the block into the block manager as well as a write ahead log.
   * It does this in parallel, using Scala Futures, and returns only after the block has
   * been stored in both places.
   * 同时向log和数据块管理器存储数据,是并行的去存储的数据,scala的Future保证了两者一定会都完成后才会被返回
   */
  def storeBlock(blockId: StreamBlockId, block: ReceivedBlock): ReceivedBlockStoreResult = {

    var numRecords = None: Option[Long]
    // Serialize the block so that it can be inserted into both 对数据块内容进行序列化
    val serializedBlock = block match {
      case ArrayBufferBlock(arrayBuffer) =>
        numRecords = Some(arrayBuffer.size.toLong)
        blockManager.dataSerialize(blockId, arrayBuffer.iterator)
      case IteratorBlock(iterator) =>
        val countIterator = new CountingIterator(iterator)
        val serializedBlock = blockManager.dataSerialize(blockId, countIterator)
        numRecords = countIterator.count
        serializedBlock
      case ByteBufferBlock(byteBuffer) =>
        byteBuffer
      case _ =>
        throw new Exception(s"Could not push $blockId to block manager, unexpected block type")
    }

    //并行的去写入两个地方
    // Store the block in block manager 写入字节数组
    val storeInBlockManagerFuture = Future {
      val putResult =
        blockManager.putBytes(blockId, serializedBlock, effectiveStorageLevel, tellMaster = true)
      if (!putResult.map { _._1 }.contains(blockId)) {
        throw new SparkException(
          s"Could not store $blockId to block manager with storage level $storageLevel")
      }
    }

    // Store the block in write ahead log 写入到日志中
    val storeInWriteAheadLogFuture = Future {
      writeAheadLog.write(serializedBlock, clock.getTimeMillis())
    }

    // Combine the futures, wait for both to complete, and return the write ahead log record handle
    //scala的Future特性,必须两个Future都成功了,才会被返回
    //zip返回两个结果集,然后警告map处理,只要第二个Future结果集
    val combinedFuture = storeInBlockManagerFuture.zip(storeInWriteAheadLogFuture).map(_._2)
    val walRecordHandle = Await.result(combinedFuture, blockStoreTimeout) //等候返回结果
    WriteAheadLogBasedStoreResult(blockId, numRecords, walRecordHandle)
  }

  def cleanupOldBlocks(threshTime: Long) {
    writeAheadLog.clean(threshTime, false)
  }

  def stop() {
    writeAheadLog.close()
    executionContext.shutdown()
  }
}

private[streaming] object WriteAheadLogBasedBlockHandler {
  def checkpointDirToLogDir(checkpointDir: String, streamId: Int): String = {
    new Path(checkpointDir, new Path("receivedData", streamId.toString)).toString
  }
}

/**
 * A utility that will wrap the Iterator to get the count
 * 迭代数据,并且记录迭代器的记录条数
 */
private[streaming] class CountingIterator[T](iterator: Iterator[T]) extends Iterator[T] {
   private var _count = 0

   private def isFullyConsumed: Boolean = !iterator.hasNext

   def hasNext(): Boolean = iterator.hasNext

   def count(): Option[Long] = {
     if (isFullyConsumed) Some(_count) else None
   }

   def next(): T = {
    _count += 1
    iterator.next()
   }
}
