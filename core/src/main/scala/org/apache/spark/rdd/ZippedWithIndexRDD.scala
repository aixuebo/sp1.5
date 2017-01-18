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

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.util.Utils

private[spark]
class ZippedWithIndexRDDPartition(val prev: Partition, val startIndex: Long) //startIndex表示序号的开始位置
  extends Partition with Serializable {
  override val index: Int = prev.index //分区ID
}

/**
 * Represents a RDD zipped with its element indices. The ordering is first based on the partition
 * index and then the ordering of items within each partition. So the first item in the first
 * partition gets index 0, and the last item in the last partition receives the largest index.
 *
 * @param prev parent RDD
 * @tparam T parent RDD item type
 * 该方式不太好,因为要先扫描一下partition所有的数据,记录每一个partition有多少个数据,好编写序号
 * 比如3个partition,第一个partition有3个记录,第二个partition有5条记录,第三个partition有2条记录
 * 因此第一个partition的序号就是0 1 2 第二个partition序号就是3 4 5 6 7 第三个partition序号就是8 9
 */
private[spark]
class ZippedWithIndexRDD[T: ClassTag](@transient prev: RDD[T]) extends RDD[(T, Long)](prev) {

  /** The start index of each partition.
    * 初始化一个数组,数组记录每一个partition需要的开始位置
    **/
  @transient private val startIndices: Array[Long] = {
    val n = prev.partitions.length
    if (n == 0) {
      Array[Long]()
    } else if (n == 1) {
      Array(0L)
    } else {
      prev.context.runJob(
        prev,
        Utils.getIteratorSize _,//计算Iterator中元素个数
        0 until n - 1 // do not need to count the last partition 不需要计算最后一个partition,因为加入了一个0作为最终结果,因此要取消最后一个partition
      ).scanLeft(0L)(_ + _) //返回一个数组,比如partition3个,每一个元素数量分别是10,20,30,则返回的数组是[0,10,30]
    }
  }

  override def getPartitions: Array[Partition] = {
    firstParent[T].partitions.map(x => new ZippedWithIndexRDDPartition(x, startIndices(x.index))) //startIndices(x.index) 获取第index个partition的开始位置
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    firstParent[T].preferredLocations(split.asInstanceOf[ZippedWithIndexRDDPartition].prev)

  override def compute(splitIn: Partition, context: TaskContext): Iterator[(T, Long)] = {
    val split = splitIn.asInstanceOf[ZippedWithIndexRDDPartition]
    firstParent[T].iterator(split.prev, context).zipWithIndex.map { x =>
      (x._1, split.startIndex + x._2) //序号是开始序号位置+在该partition的序号,因此一定是序号唯一的
    }
  }
}
