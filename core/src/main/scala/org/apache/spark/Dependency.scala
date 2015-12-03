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

package org.apache.spark

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ShuffleHandle

/**
 * :: DeveloperApi ::
 * Base class for dependencies.
 */
@DeveloperApi
abstract class Dependency[T] extends Serializable {
  //表示子类依赖的哪个父RDD,即rdd表示父类的RDD
  def rdd: RDD[T]
}


/**
 * :: DeveloperApi ::
 * Base class for dependencies where each partition of the child RDD depends on a small number
 * of partitions of the parent RDD. Narrow dependencies allow for pipelined execution.
 * 依赖的基本实现类
 * 子类RDD的每一个partition对应父类RDD的一组partition,即N:1,N指代的是父RDD多个partition可以与子RDD的某一个partition关联
 * 
 * 参数RDD是表示父RDD
 */
@DeveloperApi
abstract class NarrowDependency[T](_rdd: RDD[T]) extends Dependency[T] {
  /**
   * Get the parent partitions for a child partition.
   * @param partitionId a partition of the child RDD 参数是子RDD的partitionId
   * @return the partitions of the parent RDD that the child partition depends upon 返回值是与该子RDD的partition对应的N个父RDD的partition
   */
  def getParents(partitionId: Int): Seq[Int]

  //返回父RDD对象
  override def rdd: RDD[T] = _rdd
}


/**
 * :: DeveloperApi ::
 * Represents a dependency on the output of a shuffle stage. Note that in the case of shuffle,
 * the RDD is transient since we don't need it on the executor side.
 *
 * @param _rdd the parent RDD
 * @param partitioner partitioner used to partition the shuffle output
 * @param serializer [[org.apache.spark.serializer.Serializer Serializer]] to use. If set to None,
 *                   the default serializer, as specified by `spark.serializer` config option, will
 *                   be used.
 * @param keyOrdering key ordering for RDD's shuffles
 * @param aggregator map/reduce-side aggregator for RDD's shuffle
 * @param mapSideCombine whether to perform partial aggregation (also known as map-side combine)
 */
@DeveloperApi
class ShuffleDependency[K, V, C](
    @transient _rdd: RDD[_ <: Product2[K, V]],//k-v结构的父RDD
    val partitioner: Partitioner,//使用的partition对象
    val serializer: Option[Serializer] = None,//使用的序列化对象,shuffle不象map可以在local进行, 往往需要网络传输或存储, 所以需要serializerClass
    val keyOrdering: Option[Ordering[K]] = None,//k的排序对象
    val aggregator: Option[Aggregator[K, V, C]] = None,
    val mapSideCombine: Boolean = false)
  extends Dependency[Product2[K, V]] {

  //父RDD对象
  override def rdd: RDD[Product2[K, V]] = _rdd.asInstanceOf[RDD[Product2[K, V]]]

  //产生一个唯一ID
  val shuffleId: Int = _rdd.context.newShuffleId()

  //默认创建HashShuffleManager对象
  val shuffleHandle: ShuffleHandle = _rdd.context.env.shuffleManager.registerShuffle(
    shuffleId, _rdd.partitions.size, this)

  _rdd.sparkContext.cleaner.foreach(_.registerShuffleForCleanup(this))
}


/**
 * :: DeveloperApi ::
 * Represents a one-to-one dependency between partitions of the parent and child RDDs.
 * 代表仅仅依赖一个RDD,该类就是表示所依赖的RDD,因此参数也就是依赖的RDD
 */
@DeveloperApi
class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  //因为是一对一的关系,因此子类参数是partitionId,则对应父类的partitionId即可
  override def getParents(partitionId: Int): List[Int] = List(partitionId)
}


/**
 * :: DeveloperApi ::
 * Represents a one-to-one dependency between ranges of partitions in the parent and child RDDs.
 * @param rdd the parent RDD 父类RDD
 * @param inStart the start of the range in the parent RDD 从父RDD的哪个位置开始
 * @param outStart the start of the range in the child RDD 在子RDD的哪个位置开始
 * @param length the length of the range 长度
 * 
 * 翻译有误,请稍后查看修改
 * 例如 RDD1有3个partition RDD2有5个partition ,RDD1 union RDD2 之后就会产生新的RDD3,占用8个partition
 * 那么RDD1代表的RangeDependency类,分别rdd为RDD1,inStart=1,outStart=1,length=3
 * 那么RDD2代表的RangeDependency类,分别rdd为RDD2,inStart=1,outStart=4,length=2
 * 加入子类第5个partition作为参数传进来
 * 5 > 4 && 5<4+2,也就是说当前的partition是属于该父RDD2的,则返回5-4+1 = 2
 * 
 * 
虽然仍然是一一对应, 但是是parent RDD中的某个区间的partitions对应到child RDD中的某个区间的partitions 
典型的操作是union, 多个parent RDD合并到一个child RDD, 故每个parent RDD都对应到child RDD中的一个区间 
需要注意的是, 这里的union不会把多个partition合并成一个partition, 而是的简单的把多个RDD中的partitions放到一个RDD里面, partition不会发生变化, 可以参考Spark 源码分析 – RDD 中UnionRDD的实现

由于是range, 所以直接记录起点和length就可以了, 没有必要加入每个中间rdd, 所以RangeDependency优化了空间效率
 */
@DeveloperApi
class RangeDependency[T](rdd: RDD[T], inStart: Int, outStart: Int, length: Int)
  extends NarrowDependency[T](rdd) {

  override def getParents(partitionId: Int): List[Int] = {
    if (partitionId >= outStart && partitionId < outStart + length) {
      List(partitionId - outStart + inStart)
    } else {
      Nil
    }
  }
}
