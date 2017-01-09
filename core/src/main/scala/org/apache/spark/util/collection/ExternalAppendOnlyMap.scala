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

package org.apache.spark.util.collection

import java.io._
import java.util.Comparator

import scala.collection.BufferedIterator
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.google.common.io.ByteStreams

import org.apache.spark.{Logging, SparkEnv, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.serializer.{DeserializationStream, Serializer}
import org.apache.spark.storage.{BlockId, BlockManager}
import org.apache.spark.util.collection.ExternalAppendOnlyMap.HashComparator
import org.apache.spark.executor.ShuffleWriteMetrics

/**
 * :: DeveloperApi ::
 * An append-only map that spills sorted content to disk when there is insufficient space for it
 * to grow.
 * 也是一个类似hash table的map内存映射
 * 当没有足够的空间增长的时候,要将排序后的内容存储到磁盘上
 *
 * This map takes two passes over the data:
 *
 *   (1) Values are merged into combiners, which are sorted and spilled to disk as necessary value被合并到combiners里面,然后combiners排序后写入到磁盘上
 *   (2) Combiners are read from disk and merged together 从磁盘上读取combiners数据,然后进行合并
 *
 * The setting of the spill threshold faces the following trade-off: 
 * If the spill threshold is too high, the in-memory map may occupy more memory than is available, resulting in OOM.
 * 如果spill溢出入口设置的太高了,在映射都会存储在内存中,容易发生OOM异常
 * 
 * However, if the spill threshold is too low, we spill frequently and incur unnecessary disk
 * writes. This may lead to a performance regression compared to the normal case of using the
 * non-spilling AppendOnlyMap.
 * 然而如果溢出入口设置太低,我们溢出到磁盘太频繁,将会引发不必要的磁盘频繁写操作,这会影响性能
 *
 * Two parameters control the memory threshold:
 * 两个参数去控制内存的溢出值
 *   `spark.shuffle.memoryFraction` specifies the collective amount of memory used for storing
 *   these maps as a fraction of the executor's total memory. Since each concurrently running
 *   task maintains one map, the actual threshold for each map is this quantity divided by the
 *   number of running tasks.
 *
 *   `spark.shuffle.safetyFraction` specifies an additional margin of safety as a fraction of
 *   this threshold, in case map size estimation is not sufficiently accurate.
 *   
 *   
 *   
 *   该类表示外部存储类似hash tale的结构
 */
@DeveloperApi
class ExternalAppendOnlyMap[K, V, C](
    createCombiner: V => C, //V转换成C
    mergeValue: (C, V) => C, //合并,将相同的key对应的value进行合并,c是以前合并的值,V是新的value值,返回值是合并后的C
    mergeCombiners: (C, C) => C, //最终合并,c与c进行合并,生成C
    serializer: Serializer = SparkEnv.get.serializer,
    blockManager: BlockManager = SparkEnv.get.blockManager)
  extends Iterable[(K, C)] //返回值就是K 和合并后的C
  with Serializable
  with Logging
  with Spillable[SizeTracker] {

  private var currentMap = new SizeTrackingAppendOnlyMap[K, C]
  private val spilledMaps = new ArrayBuffer[DiskMapIterator] //追加DiskMapIterator对象,DiskMapIterator对象是写入的磁盘内容
  private val sparkConf = SparkEnv.get.conf
  private val diskBlockManager = blockManager.diskBlockManager

  /**
   * Size of object batches when reading/writing from serializers.
   *
   * Objects are written in batches, with each batch using its own serialization stream. This
   * cuts down on the size of reference-tracking maps constructed when deserializing a stream.
   *
   * NOTE: Setting this too low can cause excessive copying when serializing, since some serializers
   * grow internal data structures by growing + copying every time the number of objects doubles.
   * 超过这些key-value条记录就产生一个数据块文件
   */
  private val serializerBatchSize = sparkConf.getLong("spark.shuffle.spill.batchSize", 10000)

  // Number of bytes spilled in total
  private var _diskBytesSpilled = 0L //该任务多少字节被写入了
  def diskBytesSpilled: Long = _diskBytesSpilled

  // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
  private val fileBufferSize =
    sparkConf.getSizeAsKb("spark.shuffle.file.buffer", "32k").toInt * 1024

  // Write metrics for current spill
  private var curWriteMetrics: ShuffleWriteMetrics = _

  // Peak size of the in-memory map observed so far, in bytes
  private var _peakMemoryUsedBytes: Long = 0L
  def peakMemoryUsedBytes: Long = _peakMemoryUsedBytes

  private val keyComparator = new HashComparator[K]
  private val ser = serializer.newInstance()

  /**
   * Insert the given key and value into the map.
   * 添加一个 key-value键值对
   */
  def insert(key: K, value: V): Unit = {
    insertAll(Iterator((key, value)))
  }

  /**
   * Insert the given iterator of keys and values into the map.
   *
   * When the underlying map needs to grow, check if the global pool of shuffle memory has
   * enough room for this to happen. If so, allocate the memory required to grow the map;
   * otherwise, spill the in-memory map to disk.
   *
   * The shuffle memory usage of the first trackMemoryThreshold entries is not tracked.
   * 添加一组 key-value的迭代器
   */
  def insertAll(entries: Iterator[Product2[K, V]]): Unit = {
    // An update function for the map that we reuse across entries to avoid allocating
    // a new closure each time
    var curEntry: Product2[K, V] = null //迭代的每一个key-value
    
    //参数hadVal true表示已经存在该key,参数oldVal是该key对应的value值
    val update: (Boolean, C) => C = (hadVal, oldVal) => {
      if (hadVal) {//存在key,需要合并value
       mergeValue(oldVal, curEntry._2) 
      }else {//不存在key,则创建value
        createCombiner(curEntry._2) 
      }
    }

    while (entries.hasNext) {
      curEntry = entries.next() //遍历key-value
      val estimatedSize = currentMap.estimateSize()
      if (estimatedSize > _peakMemoryUsedBytes) {
        _peakMemoryUsedBytes = estimatedSize
      }
      if (maybeSpill(currentMap, estimatedSize)) {//说明已经要写入到磁盘上了
        currentMap = new SizeTrackingAppendOnlyMap[K, C]
      }
      currentMap.changeValue(curEntry._1, update)
      addElementsRead()//说明添加了一个元素
    }
  }

  /**
   * Insert the given iterable of keys and values into the map.
   *
   * When the underlying map needs to grow, check if the global pool of shuffle memory has
   * enough room for this to happen. If so, allocate the memory required to grow the map;
   * otherwise, spill the in-memory map to disk.
   *
   * The shuffle memory usage of the first trackMemoryThreshold entries is not tracked.
   */
  def insertAll(entries: Iterable[Product2[K, V]]): Unit = {
    insertAll(entries.iterator)
  }

  /**
   * Sort the existing contents of the in-memory map and spill them to a temporary file on disk.
   * 真正如何将数据写入到磁盘上
   */
  override protected[this] def spill(collection: SizeTracker): Unit = {
    val (blockId, file) = diskBlockManager.createTempLocalBlock() //创建一个数据块以及对应的文件
    curWriteMetrics = new ShuffleWriteMetrics() //统计对象
    var writer = blockManager.getDiskWriter(blockId, file, ser, fileBufferSize, curWriteMetrics) //创建一个输出流
    var objectsWritten = 0 //写入多少个key-value信息

    // List of batch sizes (bytes) in the order they are written to disk 每一次写入磁盘时候写入多少个字节
    val batchSizes = new ArrayBuffer[Long]

    // Flush the disk writer's contents to disk, and update relevant variables
    def flush(): Unit = {
      val w = writer
      writer = null
      w.commitAndClose()
      _diskBytesSpilled += curWriteMetrics.shuffleBytesWritten
      batchSizes.append(curWriteMetrics.shuffleBytesWritten) //每一次写入磁盘时候写入多少个字节
      objectsWritten = 0
    }

    var success = false
    try {
      val it = currentMap.destructiveSortedIterator(keyComparator) //按照key排序
      while (it.hasNext) {
        val kv = it.next()
        writer.write(kv._1, kv._2) //按照顺序写入key-value信息
        objectsWritten += 1

        if (objectsWritten == serializerBatchSize) {//刷新一下磁盘
          flush()
          curWriteMetrics = new ShuffleWriteMetrics()
          writer = blockManager.getDiskWriter(blockId, file, ser, fileBufferSize, curWriteMetrics) //重新创建一个输出流
        }
      }

      if (objectsWritten > 0) {
        flush()
      } else if (writer != null) {
        val w = writer
        writer = null
        w.revertPartialWritesAndClose()
      }
      success = true
    } finally {
      if (!success) {
        // This code path only happens if an exception was thrown above before we set success;
        // close our stuff and let the exception be thrown further
        if (writer != null) {
          writer.revertPartialWritesAndClose()
        }
        if (file.exists()) {
          file.delete()
        }
      }
    }

    spilledMaps.append(new DiskMapIterator(file, blockId, batchSizes))
  }

  /**
   * Return an iterator that merges the in-memory map with the spilled maps.
   * If no spill has occurred, simply return the in-memory map's iterator.
   */
  override def iterator: Iterator[(K, C)] = {
    if (spilledMaps.isEmpty) {
      currentMap.iterator
    } else {
      new ExternalIterator()
    }
  }

  /**
   * An iterator that sort-merges (K, C) pairs from the in-memory map and the spilled maps
   * 从外部环境中 获取ley-value
   */
  private class ExternalIterator extends Iterator[(K, C)] {

    // A queue that maintains a buffer for each stream we are currently merging
    // This queue maintains the invariant that it only contains non-empty buffers
    //最终内存堆对象
    private val mergeHeap = new mutable.PriorityQueue[StreamBuffer]

    // Input streams are derived both from the in-memory map and spilled maps on disk
    // The in-memory map is sorted in place, while the spilled maps are already in sorted order
    private val sortedMap = currentMap.destructiveSortedIterator(keyComparator) //对内存的数据进行排序
    private val inputStreams = (Seq(sortedMap) ++ spilledMaps).map(it => it.buffered) //内存的数据+磁盘的数据,共同组成了一个输入流集合

    //每一个流
    inputStreams.foreach { it =>
      val kcPairs = new ArrayBuffer[(K, C)]
      readNextHashCode(it, kcPairs) //添加数据到buf中
      if (kcPairs.length > 0) {
        mergeHeap.enqueue(new StreamBuffer(it, kcPairs))
      }
    }

    /**
     * Fill a buffer with the next set of keys with the same hash code from a given iterator. We
     * read streams one hash code at a time to ensure we don't miss elements when they are merged.
     *
     * Assumes the given iterator is in sorted order of hash code.
     *
     * @param it iterator to read from
     * @param buf buffer to write the results into
     * 添加数据到buf中
     */
    private def readNextHashCode(it: BufferedIterator[(K, C)], buf: ArrayBuffer[(K, C)]): Unit = {
      if (it.hasNext) {
        var kc = it.next()
        buf += kc
        val minHash = hashKey(kc)//该队列中最小的key
        while (it.hasNext && it.head._1.hashCode() == minHash) {//与最小的key相同的都加入到buff中
          kc = it.next()
          buf += kc
        }
      }
    }

    /**
     * If the given buffer contains a value for the given key, merge that value into
     * baseCombiner and remove the corresponding (K, C) pair from the buffer.
     * 对key相同的数据进行合并,返回合并后的数据
     */
    private def mergeIfKeyExists(key: K, baseCombiner: C, buffer: StreamBuffer): C = {
      var i = 0
      while (i < buffer.pairs.length) {
        val pair = buffer.pairs(i)
        if (pair._1 == key) {
          // Note that there's at most one pair in the buffer with a given key, since we always
          // merge stuff in a map before spilling, so it's safe to return after the first we find
          removeFromBuffer(buffer.pairs, i)
          return mergeCombiners(baseCombiner, pair._2)
        }
        i += 1
      }
      baseCombiner
    }

    /**
     * Remove the index'th element from an ArrayBuffer in constant time, swapping another element
     * into its place. This is more efficient than the ArrayBuffer.remove method because it does
     * not have to shift all the elements in the array over. It works for our array buffers because
     * we don't care about the order of elements inside, we just want to search them for a key.
     * 从队列中获取一个元素
     */
    private def removeFromBuffer[T](buffer: ArrayBuffer[T], index: Int): T = {
      val elem = buffer(index)
      buffer(index) = buffer(buffer.size - 1)  // This also works if index == buffer.size - 1 最后一个元素上升到第一个,避免数组内移动数据,因为数组内的所有数据都一样
      buffer.reduceToSize(buffer.size - 1)
      elem
    }

    /**
     * Return true if there exists an input stream that still has unvisited pairs.
     * 有堆存在,就说明还有数据
     */
    override def hasNext: Boolean = mergeHeap.length > 0

    /**
     * Select a key with the minimum hash, then combine all values with the same key from all
     * input streams.
     * 返回最终的key-c,相当于reduce操作
     */
    override def next(): (K, C) = {
      if (mergeHeap.length == 0) {
        throw new NoSuchElementException
      }
      // Select a key from the StreamBuffer that holds the lowest key hash
      val minBuffer = mergeHeap.dequeue() //获取存在最小元素的队列
      val minPairs = minBuffer.pairs //缓存的元素
      val minHash = minBuffer.minKeyHash //最小的key
      val minPair = removeFromBuffer(minPairs, 0) //获取一个元素
      val minKey = minPair._1 //key
      var minCombiner = minPair._2 //c对象
      assert(hashKey(minPair) == minHash)

      // For all other streams that may have this key (i.e. have the same minimum key hash),
      // merge in the corresponding value (if any) from that stream
      //循环其他的流,其他流也可能会有最小的key,
      val mergedBuffers = ArrayBuffer[StreamBuffer](minBuffer)
      while (mergeHeap.length > 0 && mergeHeap.head.minKeyHash == minHash) {//有相同key的队列
        val newBuffer = mergeHeap.dequeue()//出队列
        minCombiner = mergeIfKeyExists(minKey, minCombiner, newBuffer)
        mergedBuffers += newBuffer
      }

      // Repopulate each visited stream buffer and add it back to the queue if it is non-empty
      mergedBuffers.foreach { buffer =>
        if (buffer.isEmpty) {//继续读取数据
          readNextHashCode(buffer.iterator, buffer.pairs)
        }
        if (!buffer.isEmpty) {//如果数据不是空,则添加到队列中
          mergeHeap.enqueue(buffer)
        }
      }

      (minKey, minCombiner)
    }

    /**
     * A buffer for streaming from a map iterator (in-memory or on-disk) sorted by key hash.
     * Each buffer maintains all of the key-value pairs with what is currently the lowest hash
     * code among keys in the stream. There may be multiple keys if there are hash collisions.
     * Note that because when we spill data out, we only spill one value for each key, there is
     * at most one element for each key.
     *
     * StreamBuffers are ordered by the minimum key hash currently available in their stream so
     * that we can put them into a heap and sort that.
     */
    private class StreamBuffer(
        val iterator: BufferedIterator[(K, C)],//每一个队列的迭代器
        val pairs: ArrayBuffer[(K, C)])//最小的key相同的数据缓存池
      extends Comparable[StreamBuffer] {

      def isEmpty: Boolean = pairs.length == 0 //没有最小元素

      // Invalid if there are no more pairs in this stream 最小的key
      def minKeyHash: Int = {
        assert(pairs.length > 0)
        hashKey(pairs.head)
      }

      //按照key做比较进行排序
      override def compareTo(other: StreamBuffer): Int = {
        // descending order because mutable.PriorityQueue dequeues the max, not the min
        if (other.minKeyHash < minKeyHash) -1 else if (other.minKeyHash == minKeyHash) 0 else 1
      }
    }
  }

  /**
   * An iterator that returns (K, C) pairs in sorted order from an on-disk map
   * 返回(k,c)键值对的迭代器
   */
  private class DiskMapIterator(file: File, blockId: BlockId, batchSizes: ArrayBuffer[Long])
    extends Iterator[(K, C)]
  {
    private val batchOffsets = batchSizes.scanLeft(0L)(_ + _)  // Size will be batchSize.length + 1 一共写了多少个字节到磁盘上
    assert(file.length() == batchOffsets.last,
      "File length is not equal to the last batch offset:\n" +
      s"    file length = ${file.length}\n" +
      s"    last batch offset = ${batchOffsets.last}\n" +
      s"    all batch offsets = ${batchOffsets.mkString(",")}"
    ) //校验 文件大小就是写入多少个字节,他们的关系是一样的

    private var batchIndex = 0  // Which batch we're in 当前处理到第几个小文件段了
    private var fileStream: FileInputStream = null

    // An intermediate stream that reads from exactly one batch
    // This guards against pre-fetching and other arbitrary behavior of higher level streams
    private var deserializeStream = nextBatchStream() //每次获取一个小文件段
    private var nextItem: (K, C) = null //下一个元素
    private var objectsRead = 0 //读取key-value的个数

    /**
     * Construct a stream that reads only from the next batch.
     */
    private def nextBatchStream(): DeserializationStream = {
      // Note that batchOffsets.length = numBatches + 1 since we did a scan above; check whether
      // we're still in a valid batch.
      if (batchIndex < batchOffsets.length - 1) {//说明没有到最后
        if (deserializeStream != null) {//关闭以前的文件段
          deserializeStream.close()
          fileStream.close()
          deserializeStream = null
          fileStream = null
        }

        val start = batchOffsets(batchIndex) //重新打开一个新的文件段流
        fileStream = new FileInputStream(file)
        fileStream.getChannel.position(start)
        batchIndex += 1

        val end = batchOffsets(batchIndex)

        assert(end >= start, "start = " + start + ", end = " + end +
          ", batchOffsets = " + batchOffsets.mkString("[", ", ", "]"))

        val bufferedStream = new BufferedInputStream(ByteStreams.limit(fileStream, end - start))
        val compressedStream = blockManager.wrapForCompression(blockId, bufferedStream)
        ser.deserializeStream(compressedStream) //对流进行反序列化处理
      } else {
        // No more batches left
        cleanup()
        null
      }
    }

    /**
     * Return the next (K, C) pair from the deserialization stream.
     *
     * If the current batch is drained, construct a stream for the next batch and read from it.
     * If no more pairs are left, return null.
     */
    private def readNextItem(): (K, C) = {
      try {
        //读取key-value
        val k = deserializeStream.readKey().asInstanceOf[K]
        val c = deserializeStream.readValue().asInstanceOf[C]
        val item = (k, c)
        objectsRead += 1
        if (objectsRead == serializerBatchSize) {//切换下一个文件段
          objectsRead = 0
          deserializeStream = nextBatchStream()
        }
        item
      } catch {
        case e: EOFException =>
          cleanup()
          null
      }
    }

    override def hasNext: Boolean = {
      if (nextItem == null) {
        if (deserializeStream == null) {//说明没有数据流了
          return false
        }
        nextItem = readNextItem()
      }
      nextItem != null
    }

    override def next(): (K, C) = {
      val item = if (nextItem == null) readNextItem() else nextItem
      if (item == null) {
        throw new NoSuchElementException
      }
      nextItem = null
      item
    }

    private def cleanup() {
      batchIndex = batchOffsets.length  // Prevent reading any other batch
      val ds = deserializeStream //关闭流
      if (ds != null) {
        ds.close()
        deserializeStream = null
      }
      if (fileStream != null) {
        fileStream.close()
        fileStream = null
      }
      if (file.exists()) {//删除文件
        file.delete()
      }
    }

    val context = TaskContext.get()
    // context is null in some tests of ExternalAppendOnlyMapSuite because these tests don't run in
    // a TaskContext.
    if (context != null) {
      context.addTaskCompletionListener(context => cleanup())
    }
  }

  /** Convenience function to hash the given (K, C) pair by the key. 
   * 对key进行hash运算,返回key的hash值  
   **/
  private def hashKey(kc: (K, C)): Int = ExternalAppendOnlyMap.hash(kc._1)
}

//外部存储类似hash tale的结构
private[spark] object ExternalAppendOnlyMap {

  /**
   * Return the hash code of the given object. If the object is null, return a special hash code.
   * 返回一个对象的hash值
   */
  private def hash[T](obj: T): Int = {
    if (obj == null) 0 else obj.hashCode()
  }

  /**
   * A comparator which sorts arbitrary keys based on their hash codes.
   * 按照hash值进行比较
   */
  private class HashComparator[K] extends Comparator[K] {
    def compare(key1: K, key2: K): Int = {
      val hash1 = hash(key1)
      val hash2 = hash(key2)
      if (hash1 < hash2) -1 else if (hash1 == hash2) 0 else 1
    }
  }
}
