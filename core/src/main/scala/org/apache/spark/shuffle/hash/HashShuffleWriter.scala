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
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle._
import org.apache.spark.storage.DiskBlockObjectWriter

/**
 * 如何将map的输出写入到多个reduce数据文件中,方便reduce到该节点来获取结果,期间map端可以进行combiner操作
 * @param shuffleBlockResolver
 * @param handle
 * @param mapId
 * @param context
 * @tparam K
 * @tparam V
 */
private[spark] class HashShuffleWriter[K, V](
    shuffleBlockResolver: FileShuffleBlockResolver,
    handle: BaseShuffleHandle[K, V, _],
    mapId: Int,
    context: TaskContext)
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency //最终需要被reduce的信息内容
  private val numOutputSplits = dep.partitioner.numPartitions //一共多少个partition,此时partition是目标reduce的数量
  private val metrics = context.taskMetrics

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false

  private val writeMetrics = new ShuffleWriteMetrics()
  metrics.shuffleWriteMetrics = Some(writeMetrics)

  private val blockManager = SparkEnv.get.blockManager
  private val ser = Serializer.getSerializer(dep.serializer.getOrElse(null)) //如何序列化
  //mapId 表示原始RDD的第几个partition
  private val shuffle = shuffleBlockResolver.forMapTask(dep.shuffleId, mapId, numOutputSplits, ser,
    writeMetrics)

  /** Write a bunch of records to this task's output */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    val iter = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {//支持map端合并,相当于进行combiner操作,然后再进行分发到reduce中
        dep.aggregator.get.combineValuesByKey(records, context)
      } else {
        records
      }
    } else {
      require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
      records
    }

    for (elem <- iter) {//循环每一个元素
      val bucketId = dep.partitioner.getPartition(elem._1)//根据key获得应该写入到哪个桶里面
      shuffle.writers(bucketId).write(elem._1, elem._2)//找到该桶,写入key和value
    }
  }

  /** Close this writer, passing along whether the map completed
    * 参数表示是否成功完成
    **/
  override def stop(initiallySuccess: Boolean): Option[MapStatus] = {
    var success = initiallySuccess
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {//成功结束
        try {
          Some(commitWritesAndBuildStatus()) //返回文件最终大小
        } catch {
          case e: Exception =>
            success = false
            revertWrites()
            throw e
        }
      } else {//失败结束,因此要重新写
        revertWrites()
        None
      }
    } finally {
      // Release the writers back to the shuffle block manager.
      if (shuffle != null && shuffle.writers != null) {
        try {
          shuffle.releaseWriters(success)//释放该结果集,因为已经结束该map的shuffle了
        } catch {
          case e: Exception => logError("Failed to release shuffle writers", e)
        }
      }
    }
  }

  private def commitWritesAndBuildStatus(): MapStatus = {
    // Commit the writes. Get the size of each bucket block (total block size).
    //表示获取该map阶段产生的每一个reduce文件块对应的字节大小,并且对文件进行最后的提交
    val sizes: Array[Long] = shuffle.writers.map { writer: DiskBlockObjectWriter =>
      writer.commitAndClose()
      writer.fileSegment().length
    }
    MapStatus(blockManager.shuffleServerId, sizes) //计算该map最终产生多少输出,即所有reduce在该map中的输出字节总大小
  }

  //重新写
  private def revertWrites(): Unit = {
    if (shuffle != null && shuffle.writers != null) {
      for (writer <- shuffle.writers) {
        writer.revertPartialWritesAndClose()
      }
    }
  }
}
