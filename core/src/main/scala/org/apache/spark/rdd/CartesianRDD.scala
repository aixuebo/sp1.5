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

import org.apache.spark._
import org.apache.spark.util.Utils

//对两个RDD进行笛卡尔计算,返回RDD[(T, U)],这个函数对内存消耗较大,使用时候需要谨慎
private[spark]
class CartesianPartition(
    idx: Int,//最终的第几个分区
    @transient rdd1: RDD[_],//第一个RDD对象
    @transient rdd2: RDD[_],//第二个RDD对象
    s1Index: Int,//第一个rdd对象所在分区
    s2Index: Int//第二个rdd对象所在分区
  ) extends Partition {
  //分别获取两个RDD的partition内容
  var s1 = rdd1.partitions(s1Index)
  var s2 = rdd2.partitions(s2Index)
  override val index: Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    s1 = rdd1.partitions(s1Index)
    s2 = rdd2.partitions(s2Index)
    oos.defaultWriteObject()
  }
}

//对两个RDD进行笛卡尔计算,返回RDD[(T, U)],这个函数对内存消耗较大,使用时候需要谨慎
private[spark]
class CartesianRDD[T: ClassTag, U: ClassTag](
    sc: SparkContext,
    var rdd1 : RDD[T],//第一个RDD
    var rdd2 : RDD[U])//第二个RDD
  extends RDD[Pair[T, U]](sc, Nil)//产生新的RDD
  with Serializable {

  val numPartitionsInRdd2 = rdd2.partitions.length //rdd2的partition数量

  /**
   * 两个RDD的每一个partition两两相交,组成新的partition数量
   */
  override def getPartitions: Array[Partition] = {
    // create the cross product split
    val array = new Array[Partition](rdd1.partitions.length * rdd2.partitions.length) //最终partition数量是两者乘积
    for (s1 <- rdd1.partitions; s2 <- rdd2.partitions) {//两个for循环嵌套
      val idx = s1.index * numPartitionsInRdd2 + s2.index //获取最终序号,因为每个rdd1的partition都与rdd2所有的partition相交
      array(idx) = new CartesianPartition(idx, rdd1, rdd2, s1.index, s2.index)
    }
    array
  }

  //计算一个partition对应的建议host位置
  override def getPreferredLocations(split: Partition): Seq[String] = {
    val currSplit = split.asInstanceOf[CartesianPartition]
    (rdd1.preferredLocations(currSplit.s1) ++ rdd2.preferredLocations(currSplit.s2)).distinct //分别计算rdd1和rdd2中该partiton的位置,然后过滤重复,就是满足的所有host集合
  }

  //计算一个分区
  override def compute(split: Partition, context: TaskContext): Iterator[(T, U)] = {
    val currSplit = split.asInstanceOf[CartesianPartition]
    for (x <- rdd1.iterator(currSplit.s1, context);
         y <- rdd2.iterator(currSplit.s2, context)) yield (x, y) //拿到rdd1和rdd2中两个partition,元素两两相交
  }

  //该RDD依赖两个rdd,分别对每一个RDD都是一对一的依赖,即该rdd分别依赖rdd1和rdd2各一个partition
  override def getDependencies: Seq[Dependency[_]] = List(
    new NarrowDependency(rdd1) {
      def getParents(id: Int): Seq[Int] = List(id / numPartitionsInRdd2)
    },
    new NarrowDependency(rdd2) {
      def getParents(id: Int): Seq[Int] = List(id % numPartitionsInRdd2)
    }
  )

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
  }
}
