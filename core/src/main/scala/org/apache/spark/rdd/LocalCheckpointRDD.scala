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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark.{Partition, SparkContext, SparkEnv, SparkException, TaskContext}
import org.apache.spark.storage.RDDBlockId

/**
 * 一个虚假的CheckpointRDD,在失败的时候提供错误信息
 * A dummy CheckpointRDD that exists to provide informative error messages during failures.
 *
 * 这是一个简单的占位符,因为原始的checkpointed的RDD预期是全部存储在磁盘上了,仅仅如果一个调度失败或者user没有存储该原始RDD的时候,spark尝试计算这个CheckpointRDD
 * 这个时候我们必须提供一个错误信息
 * This is simply a placeholder because the original checkpointed RDD is expected to be
 * fully cached. Only if an executor fails or if the user explicitly unpersists the original
 * RDD will Spark ever attempt to compute this CheckpointRDD. When this happens, however,
 * we must provide an informative error message.
 *
 * @param sc the active SparkContext
 * @param rddId the ID of the checkpointed RDD
 * @param numPartitions the number of partitions in the checkpointed RDD
 */
private[spark] class LocalCheckpointRDD[T: ClassTag](
    @transient sc: SparkContext,
    rddId: Int,
    numPartitions: Int)
  extends CheckpointRDD[T](sc) {

  def this(rdd: RDD[T]) {
    this(rdd.context, rdd.id, rdd.partitions.size)
  }

  //每一个partition都是对应一个CheckpointRDDPartition对象
  protected override def getPartitions: Array[Partition] = {
    (0 until numPartitions).toArray.map { i => new CheckpointRDDPartition(i) }
  }

  /**
   * Throw an exception indicating that the relevant block is not found.
   * 有一个异常指示关联的数据块没有被发现
   * This should only be called if the original RDD is explicitly unpersisted or if an
   * executor is lost. Under normal circumstances, however, the original RDD (our child)
   * is expected to be fully cached and so all partitions should already be computed and
   * available in the block storage.
   */
  override def compute(partition: Partition, context: TaskContext): Iterator[T] = {
    throw new SparkException(
      s"Checkpoint block ${RDDBlockId(rddId, partition.index)} not found! Either the executor " +
      s"that originally checkpointed this partition is no longer alive, or the original RDD is " +
      s"unpersisted. If this problem persists, you may consider using `rdd.checkpoint()` " +
      s"instead, which is slower than local checkpointing but more fault-tolerant.")
  }

}
