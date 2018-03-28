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

package org.apache.spark.sql.execution

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.DataType

private class ShuffledRowRDDPartition(val idx: Int) extends Partition {
  override val index: Int = idx
  override def hashCode(): Int = idx
}

/**
 * A dummy partitioner for use with records whose partition ids have been pre-computed (i.e. for
 * use on RDDs of (Int, Row) pairs where the Int is a partition id in the expected range).
 * 一个不吭声的partitioner,因为该数据的key就是已经计算好的分区，因此直接就返回key对应的分区即可
 */
private class PartitionIdPassthrough(override val numPartitions: Int) extends Partitioner {
  override def getPartition(key: Any): Int = key.asInstanceOf[Int] //key就是分区id
}

/**
 * This is a specialized version of [[org.apache.spark.rdd.ShuffledRDD]] that is optimized for
 * shuffling rows instead of Java key-value pairs. Note that something like this should eventually
 * be implemented in Spark core, but that is blocked by some more general refactorings to shuffle
 * interfaces / internals.
 *
 * @param prev the RDD being shuffled. Elements of this RDD are (partitionId, Row) pairs.
 *             Partition ids should be in the range [0, numPartitions - 1].
 * @param serializer the serializer used during the shuffle.
 * @param numPartitions the number of post-shuffle partitions.
 */
class ShuffledRowRDD(
    @transient var prev: RDD[Product2[Int, InternalRow]],//key就是已经知道了该一行数据属于哪个分区
    serializer: Serializer,
    numPartitions: Int)
  extends RDD[InternalRow](prev.context, Nil) {

  private val part: Partitioner = new PartitionIdPassthrough(numPartitions) //不依赖numPartitions数量

  override def getDependencies: Seq[Dependency[_]] = {
    List(new ShuffleDependency[Int, InternalRow, InternalRow](prev, part, Some(serializer))) //表示shuffle过程,key是分区id,value和合并后的结果依然是row对象
  }

  override val partitioner = Some(part)

  override def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](part.numPartitions)(i => new ShuffledRowRDDPartition(i))
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[Int, InternalRow, InternalRow]]
    SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context) //从远程加载该partition的数据
      .read()
      .asInstanceOf[Iterator[Product2[Int, InternalRow]]] //知道该分区对应的数据结构是分区id与一行数据
      .map(_._2) //返回一行数据
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}
