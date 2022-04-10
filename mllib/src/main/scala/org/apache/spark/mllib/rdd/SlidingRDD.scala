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

package org.apache.spark.mllib.rdd

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.{TaskContext, Partition}
import org.apache.spark.rdd.RDD

private[mllib]
//新partition的序号,属于父rdd的partition的开始位置,tail表示下一个paritition的前window-1个元素
class SlidingRDDPartition[T](val idx: Int, val prev: Partition, val tail: Seq[T])
  extends Partition with Serializable {
  override val index: Int = idx
}

/**
  * 让父RDD的partition，按照顺序，window多个partition进行分组，组成一个新的partition
 * Represents a RDD from grouping items of its parent RDD in fixed size blocks by passing a sliding
 * window over them. The ordering is first based on the partition index and then the ordering of
 * items within each partition. This is similar to sliding in Scala collections, except that it
 * becomes an empty RDD if the window size is greater than the total number of items. It needs to
 * trigger a Spark job if the parent RDD has more than one partitions. To make this operation
 * efficient, the number of items per partition should be larger than the window size and the
 * window size should be small, e.g., 2.
  * 这个方法拆分是有效率影响的，应该让窗口window小一些，比如2，让父节点RDD的partition数量大一些，至少比window要大很多。
 *
 * @param parent     the parent RDD
 * @param windowSize the window size, must be greater than 1
 *
  *                  该方法会类似最终调用iterator的sliding方法，产生数据滑动效果。
  *                  因此有一个问题，在partition结束的时候，没有办法与下一个partition数据连接上，导致每一个partition最后部分是不能滑动的。
  *                  因此该RDD解决了这个问题，让每一个partition都把接下来的partition的window-1的数据拿过来。这样就可以做到滑动效果了。
 * @see [[org.apache.spark.mllib.rdd.RDDFunctions#sliding]]
 */
private[mllib]
class SlidingRDD[T: ClassTag](@transient val parent: RDD[T], val windowSize: Int)
  extends RDD[Array[T]](parent) {

  require(windowSize > 1, s"Window size must be greater than 1, but got $windowSize.")

  override def compute(split: Partition, context: TaskContext): Iterator[Array[T]] = {
    val part = split.asInstanceOf[SlidingRDDPartition[T]]
    (firstParent[T].iterator(part.prev, context) ++ part.tail) //让partition数据拼接上
      .sliding(windowSize) //数据可以滑动起来
      .withPartial(false)
      .map(_.toArray)
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    firstParent[T].preferredLocations(split.asInstanceOf[SlidingRDDPartition[T]].prev)

  //重新分区
  override def getPartitions: Array[Partition] = {
    val parentPartitions = parent.partitions
    val n = parentPartitions.size
    if (n == 0) {
      Array.empty
    } else if (n == 1) {
      //数组只有一个分区partition，序号是0，属于该分区的第一个父partition是谁?
      Array(new SlidingRDDPartition[T](0, parentPartitions(0), Seq.empty))
    } else {
      val n1 = n - 1
      val w1 = windowSize - 1
      // Get the first w1 items of each partition, starting from the second partition.
      //从第2个partition开始迭代，每一个分区获取前windows个数据
      val nextHeads =
        parent.context.runJob(parent, (iter: Iterator[T]) => iter.take(w1).toArray, 1 until n) //返回每一个partition中获取的元素数组
      val partitions = mutable.ArrayBuffer[SlidingRDDPartition[T]]()
      var i = 0
      var partitionIndex = 0
      while (i < n1) {//从0个分区开始不断迭代
        var j = i
        val tail = mutable.ListBuffer[T]()
        // Keep appending to the current tail until appended a head of size w1.
        //判断下一个partition的数据是不是小于window-1.如果是，则下一个partition的数据不够用，继续迭代到再下一个partition
        //其实这里面有bug,就是应该拿tail的size去比较，而不是拿nextHeads(j).size，不过考虑到partition的数据会很大，不会少于window-1，因此此处bug产生的可能性不大。
        while (j < n1 && nextHeads(j).size < w1) {
          tail ++= nextHeads(j)
          j += 1
        }
        if (j < n1) {
          tail ++= nextHeads(j)
          j += 1
        }
        partitions += new SlidingRDDPartition[T](partitionIndex, parentPartitions(i), tail) //产生一个新的分区,原始分区+下一个分区的window-1条数据
        partitionIndex += 1
        // Skip appended heads.
        i = j
      }
      // If the head of last partition has size w1, we also need to add this partition.
      if (nextHeads.last.size == w1) {
        partitions += new SlidingRDDPartition[T](partitionIndex, parentPartitions(n1), Seq.empty)
      }
      partitions.toArray
    }
  }

  // TODO: Override methods such as aggregate, which only requires one Spark job.
}
