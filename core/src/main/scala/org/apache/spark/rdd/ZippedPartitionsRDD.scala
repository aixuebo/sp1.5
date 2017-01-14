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

import java.io.{IOException, ObjectOutputStream}

import scala.reflect.ClassTag

import org.apache.spark.{OneToOneDependency, Partition, SparkContext, TaskContext}
import org.apache.spark.util.Utils

private[spark] class ZippedPartitionsPartition(
    idx: Int,//表示zip操作后partition的序号
    @transient rdds: Seq[RDD[_]],//要进行zip操作的RDD集合
    @transient val preferredLocations: Seq[String])//建议在哪些节点上运行该partition任务,这个参数是host集合
  extends Partition {

  override val index: Int = idx
  var partitionValues = rdds.map(rdd => rdd.partitions(idx)) //每一个RDD对应的partition组成的集合
  def partitions: Seq[Partition] = partitionValues

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    partitionValues = rdds.map(rdd => rdd.partitions(idx))
    oos.defaultWriteObject()
  }
}

private[spark] abstract class ZippedPartitionsBaseRDD[V: ClassTag](
    sc: SparkContext,
    var rdds: Seq[RDD[_]],//要合并的一组partition集合
    preservesPartitioning: Boolean = false)
  extends RDD[V](sc, rdds.map(x => new OneToOneDependency(x))) {// rdds.map(x => new OneToOneDependency(x)) 表示新的RDD依赖所有的父RDD,每一个父RDD都是一对一的依赖,即ZippedPartitionsBaseRDD只是依赖每一个父RDD中的一个partition

  override val partitioner =
    if (preservesPartitioning) firstParent[Any].partitioner else None //因为所有的父RDD都是相同的partitioner,因此返回第一个partitioner就是ZippedPartitionsBaseRDD需要的partitioner

  //
  override def getPartitions: Array[Partition] = {
    val numParts = rdds.head.partitions.length//计算第一个RDD的partition数量
    if (!rdds.forall(rdd => rdd.partitions.length == numParts)) {//校验每一个RDD的partition数量是否相同
      throw new IllegalArgumentException("Can't zip RDDs with unequal numbers of partitions")
    }
    Array.tabulate[Partition](numParts) { i =>
      val prefs = rdds.map(rdd => rdd.preferredLocations(rdd.partitions(i))) //计算每一个RDD对应的partition所建议的host位置
      // Check whether there are any hosts that match all RDDs; otherwise return the union
    val exactMatchLocations = prefs.reduce((x, y) => x.intersect(y)) //获取所有host交集,注意:与第一个rdd的partition的交集
      val locs = if (!exactMatchLocations.isEmpty) exactMatchLocations else prefs.flatten.distinct //如果有交集,说明很多数据块都在该节点上,因此选择交集作为返回值,如果没有交集,则获取所有的节点集合作为返回值
      new ZippedPartitionsPartition(i, rdds, locs)
    }
  }

  //返回在哪个节点上运行该rdd
  override def getPreferredLocations(s: Partition): Seq[String] = {
    s.asInstanceOf[ZippedPartitionsPartition].preferredLocations
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdds = null
  }

  /**
   * Call the prepare method of every parent that has one.
   * This is needed for reserving execution memory in advance.
   */
  protected def tryPrepareParents(): Unit = {
    rdds.collect {
      case rdd: MapPartitionsWithPreparationRDD[_, _, _] => rdd.prepare()
    }
  }
}

//合并两个Partition
private[spark] class ZippedPartitionsRDD2[A: ClassTag, B: ClassTag, V: ClassTag](
    sc: SparkContext,
    var f: (Iterator[A], Iterator[B]) => Iterator[V],
    var rdd1: RDD[A],//第一个Partition
    var rdd2: RDD[B],//第二个partition
    preservesPartitioning: Boolean = false)
  extends ZippedPartitionsBaseRDD[V](sc, List(rdd1, rdd2), preservesPartitioning) {

  override def compute(s: Partition, context: TaskContext): Iterator[V] = {
    tryPrepareParents()
    val partitions = s.asInstanceOf[ZippedPartitionsPartition].partitions //获取对应的partition集合
    f(rdd1.iterator(partitions(0), context), rdd2.iterator(partitions(1), context)) //迭代第0个 和第1个partition进入f函数
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
    f = null
  }
}

private[spark] class ZippedPartitionsRDD3
  [A: ClassTag, B: ClassTag, C: ClassTag, V: ClassTag](
    sc: SparkContext,
    var f: (Iterator[A], Iterator[B], Iterator[C]) => Iterator[V],
    var rdd1: RDD[A],
    var rdd2: RDD[B],
    var rdd3: RDD[C],
    preservesPartitioning: Boolean = false)
  extends ZippedPartitionsBaseRDD[V](sc, List(rdd1, rdd2, rdd3), preservesPartitioning) {

  override def compute(s: Partition, context: TaskContext): Iterator[V] = {
    tryPrepareParents()
    val partitions = s.asInstanceOf[ZippedPartitionsPartition].partitions
    f(rdd1.iterator(partitions(0), context),
      rdd2.iterator(partitions(1), context),
      rdd3.iterator(partitions(2), context))
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
    rdd3 = null
    f = null
  }
}

private[spark] class ZippedPartitionsRDD4
  [A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, V: ClassTag](
    sc: SparkContext,
    var f: (Iterator[A], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V],
    var rdd1: RDD[A],
    var rdd2: RDD[B],
    var rdd3: RDD[C],
    var rdd4: RDD[D],
    preservesPartitioning: Boolean = false)
  extends ZippedPartitionsBaseRDD[V](sc, List(rdd1, rdd2, rdd3, rdd4), preservesPartitioning) {

  override def compute(s: Partition, context: TaskContext): Iterator[V] = {
    tryPrepareParents()
    val partitions = s.asInstanceOf[ZippedPartitionsPartition].partitions
    f(rdd1.iterator(partitions(0), context),
      rdd2.iterator(partitions(1), context),
      rdd3.iterator(partitions(2), context),
      rdd4.iterator(partitions(3), context))
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
    rdd3 = null
    rdd4 = null
    f = null
  }
}
