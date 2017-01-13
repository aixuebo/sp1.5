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

/**
 * Class representing partitions of PartitionerAwareUnionRDD, which maintains the list of
 * corresponding partitions of parent RDDs.
 */
private[spark]
class PartitionerAwareUnionRDDPartition(
    @transient val rdds: Seq[RDD[_]],//父RDD集合,因为union是需要依赖多个父RDD的
    val idx: Int //该RDD是第几个RDD,即依赖第几个父RDD
  ) extends Partition {

  var parents = rdds.map(_.partitions(idx)).toArray //该partition需要父RDD的partition集合

  override val index = idx
  override def hashCode(): Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent partition at the time of task serialization
    parents = rdds.map(_.partitions(index)).toArray
    oos.defaultWriteObject()
  }
}

/**
 * Class representing an RDD that can take multiple RDDs partitioned by the same partitioner and
 * unify them into a single RDD while preserving the partitioner. So m RDDs with p partitions each
 * will be unified to a single RDD with p partitions and the same partitioner. The preferred
 * location for each partition of the unified RDD will be the most common preferred location
 * of the corresponding partitions of the parent RDDs. For example, location of partition 0
 * of the unified RDD will be where most of partition 0 of the parent RDDs are located.
 * 多个RDD拥有相同的partitioner,将他们合并成一个RDD
 *
 */
private[spark]
class PartitionerAwareUnionRDD[T: ClassTag](
    sc: SparkContext,
    var rdds: Seq[RDD[T]]//要合并的RDD集合
  ) extends RDD[T](sc, rdds.map(x => new OneToOneDependency(x))) {//该RDD依赖多个父RDD,每一个RDD都是OneToOneDependency依赖的关系
  require(rdds.length > 0)//RDD集合必须存在
  require(rdds.forall(_.partitioner.isDefined)) //必须每一个RDD都有partitioner
  require(rdds.flatMap(_.partitioner).toSet.size == 1,
    "Parent RDDs have different partitioners: " + rdds.flatMap(_.partitioner))//必须所有的RDD使用同一个partitioner

  override val partitioner = rdds.head.partitioner //第一个partitioner就是新的RDD的partitioner

  //返回拆分后新的partition集合
  override def getPartitions: Array[Partition] = {
    val numPartitions = partitioner.get.numPartitions //一共多少个partition
    (0 until numPartitions).map(index => {
      new PartitionerAwareUnionRDDPartition(rdds, index)
    }).toArray
  }

  // Get the location where most of the partitions of parent RDDs are located
  //选择该partition要在哪个节点上运行
  override def getPreferredLocations(s: Partition): Seq[String] = {
    logDebug("Finding preferred location for " + this + ", partition " + s.index)
    val parentPartitions = s.asInstanceOf[PartitionerAwareUnionRDDPartition].parents //找到需要的父RDD的partition集合

    //返回多个RDD对应的所有host集合,同一个host可以存在多个
    val locations = rdds.zip(parentPartitions).flatMap {
      case (rdd, part) => {
        val parentLocations = currPrefLocs(rdd, part) //获取某一个rdd的某一个partition所在节点host集合
        logDebug("Location of " + rdd + " partition " + part.index + " = " + parentLocations)
        parentLocations
      }
    }
    val location = if (locations.isEmpty) {//说明没有节点推荐
      None
    } else {//按照host排序,获取host最多出现的节点上
      // Find the location that maximum number of parent partitions prefer
      Some(locations.groupBy(x => x).maxBy(_._2.length)._1)
    }
    logDebug("Selected location for " + this + ", partition " + s.index + " = " + location)
    location.toSeq
  }

  override def compute(s: Partition, context: TaskContext): Iterator[T] = {
    val parentPartitions = s.asInstanceOf[PartitionerAwareUnionRDDPartition].parents //找到需要的父RDD的partition集合
    rdds.zip(parentPartitions).iterator.flatMap {
      case (rdd, p) => rdd.iterator(p, context) //对每一个rdd的partition进行遍历计算
    }
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdds = null
  }

  // Get the *current* preferred locations from the DAGScheduler (as opposed to the static ones)
  //获取某一个rdd的某一个partition所在节点host集合
  private def currPrefLocs(rdd: RDD[_], part: Partition): Seq[String] = {
    rdd.context.getPreferredLocs(rdd, part.index).map(tl => tl.host)
  }
}
