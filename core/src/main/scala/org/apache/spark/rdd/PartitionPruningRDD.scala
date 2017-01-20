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

import org.apache.spark.{NarrowDependency, Partition, TaskContext}
import org.apache.spark.annotation.DeveloperApi

private[spark] class PartitionPruningRDDPartition(idx: Int, val parentSplit: Partition)//要保留的partition新的序号 和对应保留的partition对象
  extends Partition {
  override val index = idx //要保留的partition新的序号
}


/**
 * Represents a dependency between the PartitionPruningRDD and its parent. In this
 * case, the child RDD contains a subset of partitions of the parents'.
 * 对父RDD的partition进行精简筛选
 * 函数partitionFilterFunc表示 传入一个partition的id,返回该partition是否要被筛选掉,返回true的表示是最终要保留的partition
 */
private[spark] class PruneDependency[T](rdd: RDD[T], @transient partitionFilterFunc: Int => Boolean)
  extends NarrowDependency[T](rdd) { //新的依赖关系

  @transient
  val partitions: Array[Partition] = rdd.partitions
    .filter(s => partitionFilterFunc(s.index)).zipWithIndex //表示过滤一些partition
    .map { case(split, idx) => new PartitionPruningRDDPartition(idx, split) : Partition } //符合标准的partition进一步重新编写序号,1 2 3...生成新的partition是PartitionPruningRDDPartition,PartitionPruningRDDPartition属于Partition类型的,因此:Partition 表示返回值类型

  override def getParents(partitionId: Int): List[Int] = {
    List(partitions(partitionId).asInstanceOf[PartitionPruningRDDPartition].parentSplit.index) //说明该新的partition依赖原始父RDD的哪个partition
  }
}


/**
 * :: DeveloperApi ::
 * A RDD used to prune RDD partitions/partitions so we can avoid launching tasks on
 * all partitions. An example use case: If we know the RDD is partitioned by range,
 * and the execution DAG has a filter on the key, we can avoid launching tasks
 * on partitions that don't have the range covering the key.
 */
@DeveloperApi
class PartitionPruningRDD[T: ClassTag](
    @transient prev: RDD[T],
    @transient partitionFilterFunc: Int => Boolean)
  extends RDD[T](prev.context, List(new PruneDependency(prev, partitionFilterFunc))) {

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    firstParent[T].iterator(
      split.asInstanceOf[PartitionPruningRDDPartition].parentSplit, context)
  }

  //依赖哪些新的partition集合
  override protected def getPartitions: Array[Partition] =
    getDependencies.head.asInstanceOf[PruneDependency[T]].partitions
}


@DeveloperApi
object PartitionPruningRDD {

  /**
   * Create a PartitionPruningRDD. This function can be used to create the PartitionPruningRDD
   * when its type T is not known at compile time.
   * 创建一个精简类型的partition
   */
  def create[T](rdd: RDD[T], partitionFilterFunc: Int => Boolean): PartitionPruningRDD[T] = {
    new PartitionPruningRDD[T](rdd, partitionFilterFunc)(rdd.elementClassTag)
  }
}
