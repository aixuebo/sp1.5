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

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.{Partition, Partitioner, TaskContext}

/**
 * An RDD that applies a user provided function to every partition of the parent RDD, and
 * additionally allows the user to prepare each partition before computing the parent partition.
 * 该map将T转换成U对象,再转换过程中涉及到M参数
 *
 * 该函数网络上没发现有使用的,因此可能涌出不是很大,我也没想到什么时候会使用该函数
 */
private[spark] class MapPartitionsWithPreparationRDD[U: ClassTag, T: ClassTag, M: ClassTag](
    prev: RDD[T],
    preparePartition: () => M,//获取M对象
    executePartition: (TaskContext, Int, M, Iterator[T]) => Iterator[U],//执行一个partition内容,第二个参数是第几个partition,第三个参数是M,第四个参数就是partition迭代器内容,返回值是迭代器U,即函数是将T转换成U的过程
    preservesPartitioning: Boolean = false)
  extends RDD[U](prev) {

  override val partitioner: Option[Partitioner] = {
    if (preservesPartitioning) firstParent[T].partitioner else None
  }

  //与父RDD是一对一的关系
  override def getPartitions: Array[Partition] = firstParent[T].partitions

  // In certain join operations, prepare can be called on the same partition multiple times.
  // In this case, we need to ensure that each call to compute gets a separate prepare argument.
  /**
在一些join操作中,在同一个partition中准备工作可能被调用多次,
在这个情况下,我们需要确保每一个调用计算获取一个单独的参数
   */
  private[this] val preparedArguments: ArrayBuffer[M] = new ArrayBuffer[M]

  /**
   * Prepare a partition for a single call to compute.
   */
  def prepare(): Unit = {
    preparedArguments += preparePartition() //preparePartition返回一个M对象,M对象添加到内部索引中
  }

  /**
   * Prepare a partition before computing it from its parent.
   */
  override def compute(partition: Partition, context: TaskContext): Iterator[U] = {
    val prepared =
      if (preparedArguments.isEmpty) {
        preparePartition()
      } else {
        preparedArguments.remove(0) //获取内部集合的第一个M对象
      }
    val parentIterator = firstParent[T].iterator(partition, context) //迭代一个partition内容
    executePartition(context, partition.index, prepared, parentIterator)
  }
}
