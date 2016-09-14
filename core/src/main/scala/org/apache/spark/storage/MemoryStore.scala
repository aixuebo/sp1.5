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

import java.nio.ByteBuffer
import java.util.LinkedHashMap

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.TaskContext
import org.apache.spark.util.{SizeEstimator, Utils}
import org.apache.spark.util.collection.SizeTrackingVector

//每一个数据块对应一个内存实体
//存储的内容包括:具体对象,内容大小,是否使用反序列化
//该对象相当于DiskStore中的file文件存储的数据,只是这个是在内存存储,所以第一个对象大多数情况下是buffer
private case class MemoryEntry(value: Any, size: Long, deserialized: Boolean)

/**
 * Stores blocks in memory, either as Arrays of deserialized Java objects or as
 * serialized ByteBuffers.
 * 将数据块信息存储到内存里
 * 
 * @maxMemory 表示内存的最大使用量,即所有的数据块存储最多使用的内存资源
 */
private[spark] class MemoryStore(blockManager: BlockManager, maxMemory: Long)
  extends BlockStore(blockManager) {

  private val conf = blockManager.conf
  //每一个数据块对应一个内存实体,key是blockId,value是该数据块对应的实体,先进先出队列
  private val entries = new LinkedHashMap[BlockId, MemoryEntry](32, 0.75f, true)

  @volatile private var currentMemory = 0L //当前已经使用的内存量

  // Ensure only one thread is putting, and if necessary, dropping blocks at any given time
  //锁对象,确保仅有一个线程可以存放数据
  private val accountingLock = new Object

  // A mapping from taskAttemptId to amount of memory used for unrolling a block (in bytes)
  // All accesses of this map are assumed to have manually synchronized on `accountingLock`
  /**
   * 存储当前该taskId已经被占用的用于unrolling blocks的内存大小
   * key是尝试任务ID,value是占用unrolling blocks的内存大小
   */
  private val unrollMemoryMap = mutable.HashMap[Long, Long]()
  // Same as `unrollMemoryMap`, but for pending unroll memory as defined below.
  // Pending unroll memory refers to the intermediate memory occupied by a task
  // after the unroll but before the actual putting of the block in the cache.
  // This chunk of memory is expected to be released *as soon as* we finish
  // caching the corresponding block as opposed to until after the task finishes.
  // This is only used if a block is successfully unrolled in its entirety in
  // memory (SPARK-4777).
  private val pendingUnrollMemoryMap = mutable.HashMap[Long, Long]()

  /**
   * The amount of space ensured for unrolling values in memory, shared across all cores.
   * This space is not reserved in advance, but allocated dynamically by dropping existing blocks.
   */
  private val maxUnrollMemory: Long = {
    val unrollFraction = conf.getDouble("spark.storage.unrollFraction", 0.2)
    (maxMemory * unrollFraction).toLong
  }

  // Initial memory to request before unrolling any block
  //为每一个数据块都预先分配一部分空间
  private val unrollMemoryThreshold: Long =
    conf.getLong("spark.storage.unrollMemoryThreshold", 1024 * 1024)

  //最大存储内存量 肯定比初始化的大,这部分应该不会发生
  if (maxMemory < unrollMemoryThreshold) {
    logWarning(s"Max memory ${Utils.bytesToString(maxMemory)} is less than the initial memory " +
      s"threshold ${Utils.bytesToString(unrollMemoryThreshold)} needed to store a block in " +
      s"memory. Please configure Spark with more memory.")
  }

  //打印此时我们设置的内存存储资源的总大小是多少
  logInfo("MemoryStore started with capacity %s".format(Utils.bytesToString(maxMemory)))

  /** Free memory not occupied by existing blocks. Note that this does not include unroll memory. */
  def freeMemory: Long = maxMemory - currentMemory //剩余内存量

  //获取数据块对应的字节大小
  override def getSize(blockId: BlockId): Long = {
    entries.synchronized {
      entries.get(blockId).size
    }
  }

  /**
   * 将blockId对应的字节内容_bytes
   * 注意
   * 1.该字节内容可以是压缩文件,因此需要解压缩处理
   * 2.该字节内容可能需要反序列化处理
   */
  override def putBytes(blockId: BlockId, _bytes: ByteBuffer, level: StorageLevel): PutResult = {
    // Work on a duplicate - since the original input might be used elsewhere.
    //去做一个深度拷贝,因为原始的输入byte可能被其他地方使用
    /**
     * duplicate()方法用于创建一个与原始缓冲区共享内容的新缓冲区。新缓冲区的position，limit，mark和capacity都初始化为原始缓冲区的索引值，然而，它们的这些值是相互独立的。
     */
    val bytes = _bytes.duplicate()
    bytes.rewind() //将bytes的position设置为0,limit不更改,即表示可以去从头读取数据了
    if (level.deserialized) {//存储级别需要反序列化
      val values = blockManager.dataDeserialize(blockId, bytes)//对bytes进行反序列化处理,返回一个一个对象的迭代器
      putIterator(blockId, values, level, returnValues = true)
    } else {//没有经过反序列化处理
      val putAttempt = tryToPut(blockId, bytes, bytes.limit, deserialized = false)
      PutResult(bytes.limit(), Right(bytes.duplicate()), putAttempt.droppedBlocks)
    }
  }

  /**
   * Use `size` to test if there is enough space in MemoryStore. If so, create the ByteBuffer and
   * put it into MemoryStore. Otherwise, the ByteBuffer won't be created.
   *
   * The caller should guarantee that `size` is correct.
   */
  def putBytes(blockId: BlockId, size: Long, _bytes: () => ByteBuffer): PutResult = {
    // Work on a duplicate - since the original input might be used elsewhere.
    lazy val bytes = _bytes().duplicate().rewind().asInstanceOf[ByteBuffer]
    val putAttempt = tryToPut(blockId, () => bytes, size, deserialized = false)
    val data =
      if (putAttempt.success) {
        assert(bytes.limit == size)
        Right(bytes.duplicate())
      } else {
        null
      }
    PutResult(size, data, putAttempt.droppedBlocks)
  }

  override def putArray(
      blockId: BlockId,
      values: Array[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {
    if (level.deserialized) {
      val sizeEstimate = SizeEstimator.estimate(values.asInstanceOf[AnyRef])
      val putAttempt = tryToPut(blockId, values, sizeEstimate, deserialized = true)
      PutResult(sizeEstimate, Left(values.iterator), putAttempt.droppedBlocks)
    } else {
      val bytes = blockManager.dataSerialize(blockId, values.iterator)
      val putAttempt = tryToPut(blockId, bytes, bytes.limit, deserialized = false)
      PutResult(bytes.limit(), Right(bytes.duplicate()), putAttempt.droppedBlocks)
    }
  }

  override def putIterator(
      blockId: BlockId,
      values: Iterator[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {
    putIterator(blockId, values, level, returnValues, allowPersistToDisk = true)
  }

  /**
   * Attempt to put the given block in memory store.
   *
   * There may not be enough space to fully unroll the iterator in memory, in which case we
   * optionally drop the values to disk if
   *   (1) the block's storage level specifies useDisk, and
   *   (2) `allowPersistToDisk` is true.
   *
   * One scenario in which `allowPersistToDisk` is false is when the BlockManager reads a block
   * back from disk and attempts to cache it in memory. In this case, we should not persist the
   * block back on disk again, as it is already in disk store.
   */
  private[storage] def putIterator(
      blockId: BlockId,
      values: Iterator[Any],
      level: StorageLevel,
      returnValues: Boolean,
      allowPersistToDisk: Boolean): PutResult = {
    val droppedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
    val unrolledValues = unrollSafely(blockId, values, droppedBlocks)
    unrolledValues match {
      case Left(arrayValues) =>
        // Values are fully unrolled in memory, so store them as an array
        val res = putArray(blockId, arrayValues, level, returnValues)
        droppedBlocks ++= res.droppedBlocks
        PutResult(res.size, res.data, droppedBlocks)
      case Right(iteratorValues) =>
        // Not enough space to unroll this block; drop to disk if applicable
        if (level.useDisk && allowPersistToDisk) {
          logWarning(s"Persisting block $blockId to disk instead.")
          val res = blockManager.diskStore.putIterator(blockId, iteratorValues, level, returnValues)
          PutResult(res.size, res.data, droppedBlocks)
        } else {
          PutResult(0, Left(iteratorValues), droppedBlocks)
        }
    }
  }

  override def getBytes(blockId: BlockId): Option[ByteBuffer] = {
    val entry = entries.synchronized {
      entries.get(blockId)
    }
    if (entry == null) {
      None
    } else if (entry.deserialized) {
      Some(blockManager.dataSerialize(blockId, entry.value.asInstanceOf[Array[Any]].iterator))
    } else {
      Some(entry.value.asInstanceOf[ByteBuffer].duplicate()) // Doesn't actually copy the data
    }
  }

  //获取给定数据块的内容
  override def getValues(blockId: BlockId): Option[Iterator[Any]] = {
    val entry = entries.synchronized {
      entries.get(blockId)
    }
    if (entry == null) {
      None
    } else if (entry.deserialized) {
      Some(entry.value.asInstanceOf[Array[Any]].iterator)
    } else {
      val buffer = entry.value.asInstanceOf[ByteBuffer].duplicate() // Doesn't actually copy data
      Some(blockManager.dataDeserialize(blockId, buffer))
    }
  }

  //删除内存映射,并且恢复当前内存使用量
  override def remove(blockId: BlockId): Boolean = {
    entries.synchronized {
      val entry = entries.remove(blockId)
      if (entry != null) {
        currentMemory -= entry.size
        logDebug(s"Block $blockId of size ${entry.size} dropped from memory (free $freeMemory)")
        true
      } else {
        false
      }
    }
  }

  //清除所有内存数据
  override def clear() {
    entries.synchronized {
      entries.clear()
      currentMemory = 0
    }
    logInfo("MemoryStore cleared")
  }

  /**
   * Unroll the given block in memory safely.
   * 在内存里面展开给定的数据块
   * 因为数据块存储的时候,有可能存储的是序列化的内容,因此需要反序列化在内存里面,因此需要占用一定内存空间
   * The safety of this operation refers to avoiding potential OOM exceptions caused by
   * unrolling the entirety of the block in memory at once. This is achieved by periodically
   * checking whether the memory restrictions for unrolling blocks are still satisfied,
   * stopping immediately if not. This check is a safeguard against the scenario in which
   * there is not enough free memory to accommodate the entirety of a single block.
   *
   * This method returns either an array with the contents of the entire block or an iterator
   * containing the values of the block (if the array would have exceeded available memory).
   */
  def unrollSafely(
      blockId: BlockId,
      values: Iterator[Any],
      droppedBlocks: ArrayBuffer[(BlockId, BlockStatus)])
    : Either[Array[Any], Iterator[Any]] = {

    // Number of elements unrolled so far
    var elementsUnrolled = 0
    // Whether there is still enough memory for us to continue unrolling this block
    var keepUnrolling = true
    // Initial per-task memory to request for unrolling blocks (bytes). Exposed for testing.
    val initialMemoryThreshold = unrollMemoryThreshold
    // How often to check whether we need to request more memory
    val memoryCheckPeriod = 16
    // Memory currently reserved by this task for this particular unrolling operation
    var memoryThreshold = initialMemoryThreshold
    // Memory to request as a multiple of current vector size
    val memoryGrowthFactor = 1.5
    // Previous unroll memory held by this task, for releasing later (only at the very end)
    val previousMemoryReserved = currentUnrollMemoryForThisTask
    // Underlying vector for unrolling the block
    var vector = new SizeTrackingVector[Any]

    // Request enough memory to begin unrolling预定一部分空间,返回值是boolean类型的,true表示预定内存成功
    keepUnrolling = reserveUnrollMemoryForThisTask(initialMemoryThreshold)

    if (!keepUnrolling) {//说明预定内存失败,打印日志
      logWarning(s"Failed to reserve initial memory threshold of " +
        s"${Utils.bytesToString(initialMemoryThreshold)} for computing block $blockId in memory.")
    }

    // Unroll this block safely, checking whether we have exceeded our threshold periodically
    try {
      while (values.hasNext && keepUnrolling) {//说明预定的内存是足够的
        vector += values.next()
        if (elementsUnrolled % memoryCheckPeriod == 0) {
          // If our vector's size has exceeded the threshold, request more memory
          val currentSize = vector.estimateSize()
          if (currentSize >= memoryThreshold) {
            val amountToRequest = (currentSize * memoryGrowthFactor - memoryThreshold).toLong
            // Hold the accounting lock, in case another thread concurrently puts a block that
            // takes up the unrolling space we just ensured here
            accountingLock.synchronized {
              if (!reserveUnrollMemoryForThisTask(amountToRequest)) {
                // If the first request is not granted, try again after ensuring free space
                // If there is still not enough space, give up and drop the partition
                val spaceToEnsure = maxUnrollMemory - currentUnrollMemory
                if (spaceToEnsure > 0) {
                  val result = ensureFreeSpace(blockId, spaceToEnsure)
                  droppedBlocks ++= result.droppedBlocks
                }
                keepUnrolling = reserveUnrollMemoryForThisTask(amountToRequest)
              }
            }
            // New threshold is currentSize * memoryGrowthFactor
            memoryThreshold += amountToRequest
          }
        }
        elementsUnrolled += 1
      }

      if (keepUnrolling) {
        // We successfully unrolled the entirety of this block
        Left(vector.toArray)
      } else {
        // We ran out of space while unrolling the values for this block
        logUnrollFailureMessage(blockId, vector.estimateSize())
        Right(vector.iterator ++ values)
      }

    } finally {
      // If we return an array, the values returned will later be cached in `tryToPut`.
      // In this case, we should release the memory after we cache the block there.
      // Otherwise, if we return an iterator, we release the memory reserved here
      // later when the task finishes.
      if (keepUnrolling) {
        accountingLock.synchronized {
          val amountToRelease = currentUnrollMemoryForThisTask - previousMemoryReserved
          releaseUnrollMemoryForThisTask(amountToRelease)
          reservePendingUnrollMemoryForThisTask(amountToRelease)
        }
      }
    }
  }

  /**
   * Return the RDD ID that a given block ID is from, or None if it is not an RDD block.
   * 获取数据块的rddId
   */
  private def getRddId(blockId: BlockId): Option[Int] = {
    blockId.asRDDId.map(_.rddId)
  }

  private def tryToPut(
      blockId: BlockId,
      value: Any,
      size: Long,
      deserialized: Boolean): ResultWithDroppedBlocks = {
    tryToPut(blockId, () => value, size, deserialized)
  }

  /**
   * Try to put in a set of values, if we can free up enough space. The value should either be
   * an Array if deserialized is true or a ByteBuffer otherwise. Its (possibly estimated) size
   * must also be passed by the caller.
   *
   * `value` will be lazily created. If it cannot be put into MemoryStore or disk, `value` won't be
   * created to avoid OOM since it may be a big ByteBuffer.
   *
   * Synchronize on `accountingLock` to ensure that all the put requests and its associated block
   * dropping is done by only on thread at a time. Otherwise while one thread is dropping
   * blocks to free memory for one block, another thread may use up the freed space for
   * another block.
   *
   * Return whether put was successful, along with the blocks dropped in the process.
   */
  private def tryToPut(
      blockId: BlockId,
      value: () => Any,
      size: Long,
      deserialized: Boolean): ResultWithDroppedBlocks = {

    /* TODO: Its possible to optimize the locking by locking entries only when selecting blocks
     * to be dropped. Once the to-be-dropped blocks have been selected, and lock on entries has
     * been released, it must be ensured that those to-be-dropped blocks are not double counted
     * for freeing up more space for another block that needs to be put. Only then the actually
     * dropping of blocks (and writing to disk if necessary) can proceed in parallel. */

    var putSuccess = false
    val droppedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]

    accountingLock.synchronized {
      val freeSpaceResult = ensureFreeSpace(blockId, size)
      val enoughFreeSpace = freeSpaceResult.success
      droppedBlocks ++= freeSpaceResult.droppedBlocks

      if (enoughFreeSpace) {
        val entry = new MemoryEntry(value(), size, deserialized)
        entries.synchronized {
          entries.put(blockId, entry)
          currentMemory += size
        }
        val valuesOrBytes = if (deserialized) "values" else "bytes"
        logInfo("Block %s stored as %s in memory (estimated size %s, free %s)".format(
          blockId, valuesOrBytes, Utils.bytesToString(size), Utils.bytesToString(freeMemory)))
        putSuccess = true
      } else {
        // Tell the block manager that we couldn't put it in memory so that it can drop it to
        // disk if the block allows disk storage.
        lazy val data = if (deserialized) {
          Left(value().asInstanceOf[Array[Any]])
        } else {
          Right(value().asInstanceOf[ByteBuffer].duplicate())
        }
        val droppedBlockStatus = blockManager.dropFromMemory(blockId, () => data)
        droppedBlockStatus.foreach { status => droppedBlocks += ((blockId, status)) }
      }
      // Release the unroll memory used because we no longer need the underlying Array
      releasePendingUnrollMemoryForThisTask()
    }
    ResultWithDroppedBlocks(putSuccess, droppedBlocks)
  }

  /**
   * Try to free up a given amount of space to store a particular block, but can fail if
   * either the block is bigger than our memory or it would require replacing another block
   * from the same RDD (which leads to a wasteful cyclic replacement pattern for RDDs that
   * don't fit into memory that we want to avoid).
   * 尝试去空出来一定内存空间,去存储参数BlockId,但是也能失败,例如当数据块太大,没有足够内存可以被释放
   * Assume that `accountingLock` is held by the caller to ensure only one thread is dropping
   * blocks. Otherwise, the freed space may fill up before the caller puts in their new value.
   *
   * Return whether there is enough free space, along with the blocks dropped in the process.
   * 确保有足够空间
   * @space 表示要添加的数据块所需要的字节大小
   * @blockIdToAdd 表示要添加的数据块
   * 
   */
  private def ensureFreeSpace(
      blockIdToAdd: BlockId,
      space: Long): ResultWithDroppedBlocks = {
    //记录日志,当前需要多少内存,当前内存以及总内存
    logInfo(s"ensureFreeSpace($space) called with curMem=$currentMemory, maxMem=$maxMemory")

    //释放的数据块集合
    val droppedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]

    //大过了内存限制,要抛异常
    if (space > maxMemory) {
      logInfo(s"Will not store $blockIdToAdd as it is larger than our memory limit")
      return ResultWithDroppedBlocks(success = false, droppedBlocks)
    }

    // Take into account the amount of memory currently occupied by unrolling blocks
    // and minus the pending unroll memory for that block on current thread.
    val taskAttemptId = currentTaskAttemptId()
    //当前剩余内存 - 当前所有任务中 被unrolling blocks操作占用的总内存空间 + 当前该任务等待的内存使用量
    val actualFreeMemory = freeMemory - currentUnrollMemory +
      pendingUnrollMemoryMap.getOrElse(taskAttemptId, 0L)

    if (actualFreeMemory < space) { //空间不足
      val rddToAdd = getRddId(blockIdToAdd) //获取RDDid
      val selectedBlocks = new ArrayBuffer[BlockId] //选择释放的数据块ID
      var selectedMemory = 0L //选择释放的数据块ID的内存集合

      // This is synchronized to ensure that the set of entries is not changed
      // (because of getValue or getBytes) while traversing the iterator, as that
      // can lead to exceptions.
      entries.synchronized {
        val iterator = entries.entrySet().iterator()
        while (actualFreeMemory + selectedMemory < space && iterator.hasNext) {//如果内存仍然不足,则继续释放内存
          val pair = iterator.next() //每一个数据块对应一个内存实体,key是blockId,value是该数据块对应的实体
          val blockId = pair.getKey
          //如果rddToAdd是空,说明该要添加的数据块不是RDD,因此可以删除队列第一个元素
          //如果rddToAdd不是空,但是不是要添加的RDD对应的数据块,因此也可以删除队列的第一个元素
          if (rddToAdd.isEmpty || rddToAdd != getRddId(blockId)) {
            selectedBlocks += blockId //累加要释放的数据块ID
            selectedMemory += pair.getValue.size //累加数据块释放的内存
          }
        }
      }

      if (actualFreeMemory + selectedMemory >= space) { //有空间可以去释放,并且释放后可以满足存储数据块
        logInfo(s"${selectedBlocks.size} blocks selected for dropping") //打印日志,多少个数据块要被释放掉
        for (blockId <- selectedBlocks) { //循环每一个释放的数据块
          val entry = entries.synchronized { entries.get(blockId) } //要释放的实体 BlockId, MemoryEntry
          // This should never be null as only one task should be dropping
          // blocks and removing entries. However the check is still here for
          // future safety.
          if (entry != null) {
            val data = if (entry.deserialized) {
              Left(entry.value.asInstanceOf[Array[Any]])
            } else {
              Right(entry.value.asInstanceOf[ByteBuffer].duplicate())
            }
            val droppedBlockStatus = blockManager.dropFromMemory(blockId, data) //释放该数据块ID与之对应的数据内容,返回数据块的状态
            droppedBlockStatus.foreach { status => droppedBlocks += ((blockId, status)) }
          }
        }
        return ResultWithDroppedBlocks(success = true, droppedBlocks)
      } else {//打印日志,说明不能释放该空间.因此无法添加该数据块到内存
        logInfo(s"Will not store $blockIdToAdd as it would require dropping another block " +
          "from the same RDD")
        return ResultWithDroppedBlocks(success = false, droppedBlocks)
      }
    }
    ResultWithDroppedBlocks(success = true, droppedBlocks)
  }

  //是否包含该数据块
  override def contains(blockId: BlockId): Boolean = {
    entries.synchronized { entries.containsKey(blockId) }
  }

  //获取当前执行任务的ID
  private def currentTaskAttemptId(): Long = {
    // In case this is called on the driver, return an invalid task attempt id.
    Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(-1L)
  }

  /**
   * Reserve additional memory for unrolling blocks used by this task.
   * Return whether the request is granted.
   * 设置当前线程一部分内存空间,属于预定空间
   */
  def reserveUnrollMemoryForThisTask(memory: Long): Boolean = {
    accountingLock.synchronized {
      val granted = freeMemory > currentUnrollMemory + memory
      if (granted) {
        val taskAttemptId = currentTaskAttemptId()
        unrollMemoryMap(taskAttemptId) = unrollMemoryMap.getOrElse(taskAttemptId, 0L) + memory
      }
      granted
    }
  }

  /**
   * Release memory used by this task for unrolling blocks.
   * If the amount is not specified, remove the current task's allocation altogether.
   * 对当前任务释放一部分内存
   */
  def releaseUnrollMemoryForThisTask(memory: Long = -1L): Unit = {
    val taskAttemptId = currentTaskAttemptId()
    accountingLock.synchronized {
      if (memory < 0) {
        unrollMemoryMap.remove(taskAttemptId)
      } else {
        unrollMemoryMap(taskAttemptId) = unrollMemoryMap.getOrElse(taskAttemptId, memory) - memory
        // If this task claims no more unroll memory, release it completely
        if (unrollMemoryMap(taskAttemptId) <= 0) {
          unrollMemoryMap.remove(taskAttemptId)
        }
      }
    }
  }

  /**
   * Reserve the unroll memory of current unroll successful block used by this task
   * until actually put the block into memory entry.
   */
  def reservePendingUnrollMemoryForThisTask(memory: Long): Unit = {
    val taskAttemptId = currentTaskAttemptId()
    accountingLock.synchronized {
       pendingUnrollMemoryMap(taskAttemptId) =
         pendingUnrollMemoryMap.getOrElse(taskAttemptId, 0L) + memory
    }
  }

  /**
   * Release pending unroll memory of current unroll successful block used by this task
   */
  def releasePendingUnrollMemoryForThisTask(): Unit = {
    val taskAttemptId = currentTaskAttemptId()
    accountingLock.synchronized {
      pendingUnrollMemoryMap.remove(taskAttemptId)
    }
  }

  /**
   * Return the amount of memory currently occupied for unrolling blocks across all tasks.
   * 返回当前所有任务中 被unrolling blocks操作占用的总内存空间
   */
  def currentUnrollMemory: Long = accountingLock.synchronized {
    unrollMemoryMap.values.sum + pendingUnrollMemoryMap.values.sum
  }

  /**
   * Return the amount of memory currently occupied for unrolling blocks by this task.
   * 返回当前该taskId已经被占用的用于unrolling blocks的内存大小
   */
  def currentUnrollMemoryForThisTask: Long = accountingLock.synchronized {
    unrollMemoryMap.getOrElse(currentTaskAttemptId(), 0L)
  }

  /**
   * Return the number of tasks currently unrolling blocks.
   * 返回用于unrolling blocks的任务总数
   */
  def numTasksUnrolling: Int = accountingLock.synchronized { unrollMemoryMap.keys.size }

  /**
   * Log information about current memory usage.
   */
  def logMemoryUsage(): Unit = {
    val blocksMemory = currentMemory //当前使用内存量
    val unrollMemory = currentUnrollMemory // 返回当前所有任务中 被unrolling blocks操作占用的总内存空间
    val totalMemory = blocksMemory + unrollMemory
    logInfo(
      s"Memory use = ${Utils.bytesToString(blocksMemory)} (blocks) + " +
      s"${Utils.bytesToString(unrollMemory)} (scratch space shared across " +
      s"$numTasksUnrolling tasks(s)) = ${Utils.bytesToString(totalMemory)}. " +
      s"Storage limit = ${Utils.bytesToString(maxMemory)}."
    )
  }

  /**
   * Log a warning for failing to unroll a block.
   *
   * @param blockId ID of the block we are trying to unroll.
   * @param finalVectorSize Final size of the vector before unrolling failed.
   */
  def logUnrollFailureMessage(blockId: BlockId, finalVectorSize: Long): Unit = {
    logWarning(
      s"Not enough space to cache $blockId in memory! " +
      s"(computed ${Utils.bytesToString(finalVectorSize)} so far)"
    )
    logMemoryUsage()
  }
}

//结果
private[spark] case class ResultWithDroppedBlocks(
    success: Boolean,//是否成功
    droppedBlocks: Seq[(BlockId, BlockStatus)])//丢弃了哪些数据块
