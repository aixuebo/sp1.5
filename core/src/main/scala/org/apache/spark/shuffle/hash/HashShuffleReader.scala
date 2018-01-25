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

package org.apache.spark.shuffle.hash

import org.apache.spark._
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleReader}
import org.apache.spark.storage.{BlockManager, ShuffleBlockFetcherIterator}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

/**
 * 读取某一个map的输出shuffler结果
 * ********************************以下内容很耗费内存********************************
 */
private[spark] class HashShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],//shuffle的定义信息
    startPartition: Int,//读取哪个partition的数据,即读取哪个map的数据
    endPartition: Int,//读取到哪个partition结束
    context: TaskContext,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
  extends ShuffleReader[K, C] with Logging {

  //hash shuffle仅仅支持抓取一个partition
  require(endPartition == startPartition + 1,
    "Hash shuffle currently only supports fetching one partition")

  private val dep = handle.dependency

  /** Read the combined key-values for this reduce task
    * 读取一个shuffle的结果,返回key-value的键值对元组迭代器
    **/
  override def read(): Iterator[Product2[K, C]] = {
    val blockFetcherItr = new ShuffleBlockFetcherIterator(//该迭代器表示如何从网络上抓去数据块回来
      context,
      blockManager.shuffleClient,
      blockManager,
      mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition),//获取该shuffle要抓去的reduce的数据块所在节点
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024) //抓去流量

    // Wrap the streams for compression based on configuration 如果数据是压缩的,对数据解压缩
    //对每一个数据块转换成解压缩后的数据流
    //该流返回的是每一个block数据块的集合
    val wrappedStreams = blockFetcherItr.map { case (blockId, inputStream) =>
      blockManager.wrapForCompression(blockId, inputStream) //如果数据块是压缩的,要对其解压缩
    }

    //实例化一个反序列化对象
    val ser = Serializer.getSerializer(dep.serializer)
    val serializerInstance = ser.newInstance()

    // Create a key/value iterator for each stream 对每一个流创建key-value元组对象的迭代器
    //对解压缩后的数据流进行反序列化,得到反序列化后的key-value信息
    val recordIter = wrappedStreams.flatMap { wrappedStream => //循环每一个数据块
      // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
      // NextIterator. The NextIterator makes sure that close() is called on the
      // underlying InputStream when all records have been read.
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator //反序列化---将一个数据块反序列化成kv的数据流迭代器
    }

    // Update the context task metrics for each record read.
    val readMetrics = context.taskMetrics.createShuffleReadMetricsForDependency()
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map(record => {
        readMetrics.incRecordsRead(1)
        record
      }),
      context.taskMetrics().updateShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    //********************************以下内容很耗费内存********************************

    //对reduce的抓去结果进行merge操作
    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {//map端combine
        // We are reading values that are already combined
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context) //对收取到的map的数据进行先聚合merge操作
      } else {
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // Sort the output if there is a sort ordering defined.
    dep.keyOrdering match {//排序输出
      case Some(keyOrd: Ordering[K]) =>
        //key是排序对象,因此要对key进行排序
        // Create an ExternalSorter to sort the data. Note that if spark.shuffle.spill is disabled,
        // the ExternalSorter won't spill to disk.
        val sorter = new ExternalSorter[K, C, C](ordering = Some(keyOrd), serializer = Some(ser))
        sorter.insertAll(aggregatedIter)//把合并后的迭代器进行排序处理
        context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
        context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
        context.internalMetricsToAccumulators(
          InternalAccumulator.PEAK_EXECUTION_MEMORY).add(sorter.peakMemoryUsedBytes)
        sorter.iterator
      case None =>
        aggregatedIter //key不是排序对象,则将结果输出即可
    }
  }
}
