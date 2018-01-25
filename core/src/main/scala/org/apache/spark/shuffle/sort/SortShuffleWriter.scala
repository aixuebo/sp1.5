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

package org.apache.spark.shuffle.sort

import org.apache.spark._
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{IndexShuffleBlockResolver, ShuffleWriter, BaseShuffleHandle}
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.collection.ExternalSorter

//每一个map都对应一个该writer对象
private[spark] class SortShuffleWriter[K, V, C](
    shuffleBlockResolver: IndexShuffleBlockResolver,
    handle: BaseShuffleHandle[K, V, C],
    mapId: Int,//第几个mapper
    context: TaskContext)
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency

  private val blockManager = SparkEnv.get.blockManager

  private var sorter: SortShuffleFileWriter[K, V] = null

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false

  private var mapStatus: MapStatus = null

  private val writeMetrics = new ShuffleWriteMetrics()
  context.taskMetrics.shuffleWriteMetrics = Some(writeMetrics)

  /** Write a bunch of records to this task's output */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    sorter = if (dep.mapSideCombine) {
      //表示在map端要进行一次merge合并,以及排序操作
      require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
      new ExternalSorter[K, V, C](
        dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
    } else if (SortShuffleWriter.shouldBypassMergeSort(
        SparkEnv.get.conf, dep.partitioner.numPartitions, aggregator = None, keyOrdering = None)) {
      // If there are fewer than spark.shuffle.sort.bypassMergeThreshold partitions and we don't
      // need local aggregation and sorting, write numPartitions files directly and just concatenate
      // them at the end. This avoids doing serialization and deserialization twice to merge
      // together the spilled files, which would happen with the normal code path. The downside is
      // having multiple files open at a time and thus more memory allocated to buffers.
      new BypassMergeSortShuffleWriter[K, V](SparkEnv.get.conf, blockManager, dep.partitioner,
        writeMetrics, Serializer.getSerializer(dep.serializer))
    } else {
      // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
      // care whether the keys get sorted in each partition; that will be done on the reduce side
      // if the operation being run is sortByKey.
      //说明map端不需要排序,也不需要聚合,那么就直接输出就可以,最终到reduce端进行排序
      new ExternalSorter[K, V, V](
        aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
    }
    sorter.insertAll(records) //将map的结果进行排序

    // Don't bother including the time to open the merged output file in the shuffle write time,
    // because it just opens a single file, so is typically too fast to measure accurately
    // (see SPARK-3570).
    val outputFile = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId) //找到该shuffle在该map上的数据块文件
    val blockId = ShuffleBlockId(dep.shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)
    val partitionLengths = sorter.writePartitionedFile(blockId, context, outputFile) //文件内容输出到一个文件中
    shuffleBlockResolver.writeIndexFile(dep.shuffleId, mapId, partitionLengths)//写入索引文件

    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths)
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        return Option(mapStatus)
      } else {
        // The map task failed, so delete our output data.
        shuffleBlockResolver.removeDataByMap(dep.shuffleId, mapId)
        return None
      }
    } finally {
      // Clean up our sorter, which may have its own intermediate files
      if (sorter != null) {
        val startTime = System.nanoTime()
        sorter.stop()
        context.taskMetrics.shuffleWriteMetrics.foreach(
          _.incShuffleWriteTime(System.nanoTime - startTime))
        sorter = null
      }
    }
  }
}

private[spark] object SortShuffleWriter {

  /**
  此时task会为每个下游task都创建一个临时磁盘文件，并将数据按key进行hash然后根据key的hash值，将key写入对应的磁盘文件之中。当然，写入磁盘文件时也是先写入内存缓冲，缓冲写满之后再溢写到磁盘文件的。最后，同样会将所有临时磁盘文件都合并成一个磁盘文件，并创建一个单独的索引文件。
　　该过程的磁盘写机制其实跟未经优化的HashShuffleManager是一模一样的，因为都要创建数量惊人的磁盘文件，只是在最后会做一个磁盘文件的合并而已。因此少量的最终磁盘文件，也让该机制相对未经优化的HashShuffleManager来说，shuffle read的性能会更好。
　　而该机制与普通SortShuffleManager运行机制的不同在于：第一，磁盘写机制不同；第二，不会进行排序。也就是说，启用该机制的最大好处在于，shuffle write过程中，不需要进行数据的排序操作，也就节省掉了这部分的性能开销。
   */
  //是否要进行排序
  def shouldBypassMergeSort(
      conf: SparkConf,
      numPartitions: Int,
      aggregator: Option[Aggregator[_, _, _]],
      keyOrdering: Option[Ordering[_]]): Boolean = {
    val bypassMergeThreshold: Int = conf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200)
    //完全兼容hash方式的shuffle,本身不需要map上聚合,也不需要排序,同时reduce的数量又很小,那么一个map的所有的reduce产生产生一个文件即可
    numPartitions <= bypassMergeThreshold && aggregator.isEmpty && keyOrdering.isEmpty
  }
}
