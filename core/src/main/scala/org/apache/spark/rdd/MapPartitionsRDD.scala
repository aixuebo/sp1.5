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

/**
 * An RDD that applies the provided function to every partition of the parent RDD.
 * 提供一个函数.partition中的每一个泛型元素T都会转换成U,组成新的RDD
 */
private[spark] class MapPartitionsRDD[U: ClassTag, T: ClassTag](
    prev: RDD[T],//父RDD[T]
    f: (TaskContext, Int, Iterator[T]) => Iterator[U],  // (TaskContext, partition index, iterator) 函数,三个参数,返回RDD[U]后,迭代每一个泛型U对象
    preservesPartitioning: Boolean = false) //true表示维持父类RDD[T]对应的Partitioner分隔类
  extends RDD[U](prev) { //继承RDD[U],该RDD依赖RDD[T]

  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None//如果为false,说明新的rdd是不需要partitioner的?挺奇怪,不知道为什么

  override def getPartitions: Array[Partition] = firstParent[T].partitions //默认partition数量与父RDD一样,即类似于Map方法,将每一个父partition对应一个子的partition

  //计算某一个partition,默认逻辑是调用父partition,父类的一行记录一行记录的输入流,流入f函数中,转成新的U对象
  override def compute(split: Partition, context: TaskContext): Iterator[U] =
    f(context, split.index, firstParent[T].iterator(split, context))
}
