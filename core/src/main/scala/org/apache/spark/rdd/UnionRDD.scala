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

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.{Dependency, Partition, RangeDependency, SparkContext, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.Utils

/**
 * Partition for UnionRDD.
 *
 * @param idx index of the partition
 * @param rdd the parent RDD this partition refers to
 * @param parentRddIndex index of the parent RDD this partition refers to
 * @param parentRddPartitionIndex index of the partition within the parent RDD
 *                                this partition refers to
 */
private[spark] class UnionPartition[T: ClassTag](
    idx: Int,//该UnionPartition的index序号
    @transient rdd: RDD[T],//这部分partition属于哪个RDD的partition
    val parentRddIndex: Int,//该partition属于第几个RDD
    @transient parentRddPartitionIndex: Int)//该partition属于那一个RDD的第几个partition
  extends Partition {

  var parentPartition: Partition = rdd.partitions(parentRddPartitionIndex) //获取对应的partition集合

  def preferredLocations(): Seq[String] = rdd.preferredLocations(parentPartition) //获取该partition所在最佳host节点

  override val index: Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    parentPartition = rdd.partitions(parentRddPartitionIndex)//获取RDD对应的的index个partition对象
    oos.defaultWriteObject()
  }
}

//表示将N个RDD进行合并,称为更大的RDD,但是新的RDD的partition数量是N个RDD的partition数量之和,即不会合并RDD处理
@DeveloperApi
class UnionRDD[T: ClassTag](
    sc: SparkContext,
    var rdds: Seq[RDD[T]])//要合并的一组RDD集合
  extends RDD[T](sc, Nil) {  // Nil since we implement getDependencies 这个RDD是根RDD,因为他的父RDD是nil

  //返回新的partition对象集合
  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](rdds.map(_.partitions.length).sum)//计算所有的RDD集合中一共多少个分区,组成数组
    var pos = 0//新的partition对象的序号
    for ((rdd, rddIndex) <- rdds.zipWithIndex; split <- rdd.partitions) {//双层for循环,先循环每一个RDD,然后在循环每一个rdd对应的partition集合
      array(pos) = new UnionPartition(pos, rdd, rddIndex, split.index) //组装成新的partition对象,每一个partition都是代表一个父RDD对应的一个partition
      pos += 1
    }
    array
  }


  override def getDependencies: Seq[Dependency[_]] = {
    val deps = new ArrayBuffer[Dependency[_]]
    var pos = 0
    for (rdd <- rdds) {//这个是每一个父RDD对应的依赖
      deps += new RangeDependency(rdd, 0, pos, rdd.partitions.length) // 虽然新RDD依赖父RDD也是一对一的,但是依赖的却有一个范围,该类是一对一的一种特别,计数要有一个基数,基数就是outStart
      pos += rdd.partitions.length
    }
    deps
  }

  //读取某一个父rdd的partition的内容
  override def compute(s: Partition, context: TaskContext): Iterator[T] = {
    val part = s.asInstanceOf[UnionPartition[T]]
    //parent[T](part.parentRddIndex)表示查找哪一个父RDD
    //iterator(part.parentPartition, context) 对该父RDD进行遍历
    parent[T](part.parentRddIndex).iterator(part.parentPartition, context)
  }

  override def getPreferredLocations(s: Partition): Seq[String] =
    s.asInstanceOf[UnionPartition[T]].preferredLocations()

  override def clearDependencies() {
    super.clearDependencies()
    rdds = null
  }
}
