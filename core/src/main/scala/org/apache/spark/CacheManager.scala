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

package org.apache.spark

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.storage._

/**
 * Spark class responsible for passing RDDs partition contents to the BlockManager and making
 * sure a node doesn't load two copies of an RDD at once.
 */
private[spark] class CacheManager(blockManager: BlockManager) extends Logging {

  /** Keys of RDD partitions that are being computed/loaded.
    * 正在被计算或者加载的RDD-partition的集合
    **/
  private val loading = new mutable.HashSet[RDDBlockId]

  /** Gets or computes an RDD partition. Used by RDD.iterator() when an RDD is cached.
    * 获取rdd的一个分区,返回该分区的一行一行数据的迭代器
    **/
  def getOrCompute[T](
      rdd: RDD[T],//指定RDD
      partition: Partition,//需要的分区信息
      context: TaskContext,//哪个任务执行该分区
      storageLevel: StorageLevel): Iterator[T] = {

    val key = RDDBlockId(rdd.id, partition.index)//该rdd的一个分片名称
    logDebug(s"Looking for partition $key")
    blockManager.get(key) match {//说明本地有该数据块
      case Some(blockResult) =>
        // Partition is already materialized, so just return its values 该数据块已经物化了,
        val existingMetrics = context.taskMetrics
          .getInputMetricsForReadMethod(blockResult.readMethod)
        existingMetrics.incBytesRead(blockResult.bytes)//增加统计量

        val iter = blockResult.data.asInstanceOf[Iterator[T]]//返回该数据块对应的迭代信息,即一行一行的迭代器
        new InterruptibleIterator[T](context, iter) {
          override def next(): T = {
            existingMetrics.incRecordsRead(1)//记录增加了多少行数据
            delegate.next()
          }
        }
      case None =>
        //说明本地没有数据块,要去加载回来
        // Acquire a lock for loading this partition 要求一个锁,表示正在加载这个partition的数据
        // If another thread already holds the lock, wait for it to finish return its results 如果其他线程也已经持有该锁了,则等待他们返回即可
        val storedValues = acquireLockForPartition[T](key)
        if (storedValues.isDefined) {//说明数据块已经加载回来了,因为其他线程已经申请了该数据块了,只需要等待其他线程将数据块取回来即可
          return new InterruptibleIterator[T](context, storedValues.get)//直接返回属具体数据块内容
        }

        // Otherwise, we have to load the partition ourselves 我们这个线程要去加载这个数据块
        try {
          logInfo(s"Partition $key not found, computing it")
          val computedValues = rdd.computeOrReadCheckpoint(partition, context)

          // If the task is running locally, do not persist the result
          if (context.isRunningLocally) {
            return computedValues
          }

          // Otherwise, cache the values and keep track of any updates in block statuses
          val updatedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
          val cachedValues = putInBlockManager(key, computedValues, storageLevel, updatedBlocks)
          val metrics = context.taskMetrics
          val lastUpdatedBlocks = metrics.updatedBlocks.getOrElse(Seq[(BlockId, BlockStatus)]())
          metrics.updatedBlocks = Some(lastUpdatedBlocks ++ updatedBlocks.toSeq)
          new InterruptibleIterator(context, cachedValues)

        } finally {
          loading.synchronized {
            loading.remove(key)
            loading.notifyAll()
          }
        }
    }
  }

  /**
   * Acquire a loading lock for the partition identified by the given block ID.
   *
   * If the lock is free, just acquire it and return None. Otherwise, another thread is already
   * loading the partition, so we wait for it to finish and return the values loaded by the thread.
   * 获取一个partition数据,先枷锁,返回该partition的数据内容迭代器
   */
  private def acquireLockForPartition[T](id: RDDBlockId): Option[Iterator[T]] = {
    loading.synchronized {
      if (!loading.contains(id)) {//将该rdd-partition数据块添加到集合,表示正在加载中
        // If the partition is free, acquire its lock to compute its value
        loading.add(id)
        None
      } else {
        // Otherwise, wait for another thread to finish and return its result
        logInfo(s"Another thread is loading $id, waiting for it to finish...")
        while (loading.contains(id)) {//说明已经在有其他线程加载该数据块了
          try {
            loading.wait()//等待
          } catch {
            case e: Exception =>
              logWarning(s"Exception while waiting for another thread to load $id", e)
          }
        }
        logInfo(s"Finished waiting for $id")
        val values = blockManager.get(id)
        if (!values.isDefined) {
          /* The block is not guaranteed to exist even after the other thread has finished.
           * For instance, the block could be evicted after it was put, but before our get.
           * In this case, we still need to load the partition ourselves. */
          logInfo(s"Whoever was loading $id failed; we'll try it ourselves")
          loading.add(id)
        }
        values.map(_.data.asInstanceOf[Iterator[T]])//将value结果转换成仅要迭代器的部分数据
      }
    }
  }

  /**
   * Cache the values of a partition, keeping track of any updates in the storage statuses of
   * other blocks along the way.
   *
   * The effective storage level refers to the level that actually specifies BlockManager put
   * behavior, not the level originally specified by the user. This is mainly for forcing a
   * MEMORY_AND_DISK partition to disk if there is not enough room to unroll the partition,
   * while preserving the the original semantics of the RDD as specified by the application.
   */
  private def putInBlockManager[T](
      key: BlockId,
      values: Iterator[T],
      level: StorageLevel,
      updatedBlocks: ArrayBuffer[(BlockId, BlockStatus)],
      effectiveStorageLevel: Option[StorageLevel] = None): Iterator[T] = {

    val putLevel = effectiveStorageLevel.getOrElse(level)
    if (!putLevel.useMemory) {//不是存储在内存中
      /*
       * This RDD is not to be cached in memory, so we can just pass the computed values as an
       * iterator directly to the BlockManager rather than first fully unrolling it in memory.
       */
      updatedBlocks ++=
        blockManager.putIterator(key, values, level, tellMaster = true, effectiveStorageLevel)
      blockManager.get(key) match {
        case Some(v) => v.data.asInstanceOf[Iterator[T]]
        case None =>
          logInfo(s"Failure to store $key")
          throw new BlockException(key, s"Block manager failed to return cached value for $key!")
      }
    } else {//存储在内存中
      /*
       * This RDD is to be cached in memory. In this case we cannot pass the computed values
       * to the BlockManager as an iterator and expect to read it back later. This is because
       * we may end up dropping a partition from memory store before getting it back.
       *
       * In addition, we must be careful to not unroll the entire partition in memory at once.
       * Otherwise, we may cause an OOM exception if the JVM does not have enough space for this
       * single partition. Instead, we unroll the values cautiously, potentially aborting and
       * dropping the partition to disk if applicable.
       */
      blockManager.memoryStore.unrollSafely(key, values, updatedBlocks) match {
        case Left(arr) =>
          // We have successfully unrolled the entire partition, so cache it in memory
          updatedBlocks ++=
            blockManager.putArray(key, arr, level, tellMaster = true, effectiveStorageLevel)
          arr.iterator.asInstanceOf[Iterator[T]]
        case Right(it) =>
          // There is not enough space to cache this partition in memory
          val returnValues = it.asInstanceOf[Iterator[T]]
          if (putLevel.useDisk) {
            logWarning(s"Persisting partition $key to disk instead.")
            val diskOnlyLevel = StorageLevel(useDisk = true, useMemory = false,
              useOffHeap = false, deserialized = false, putLevel.replication)
            putInBlockManager[T](key, returnValues, level, updatedBlocks, Some(diskOnlyLevel))
          } else {
            returnValues
          }
      }
    }
  }

}
