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

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.mllib.random.RandomDataGenerator
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

import scala.reflect.ClassTag
import scala.util.Random

private[mllib] class RandomRDDPartition[T](
    override val index: Int,//第几个分区,即分区序号
    val size: Int,//产生多少条数据
    val generator: RandomDataGenerator[T],//如何产生数据
    val seed: Long //随机种子
 ) extends Partition {

  require(size >= 0, "Non-negative partition size required.")
}

// These two classes are necessary since Range objects in Scala cannot have size > Int.MaxValue
//如何产生随机数RDD
private[mllib] class RandomRDD[T: ClassTag](@transient sc: SparkContext,
    size: Long,
    numPartitions: Int,
    @transient rng: RandomDataGenerator[T],
    @transient seed: Long = Utils.random.nextLong) extends RDD[T](sc, Nil) {

  require(size > 0, "Positive RDD size required.")
  require(numPartitions > 0, "Positive number of partitions required")
  require(math.ceil(size.toDouble / numPartitions) <= Int.MaxValue,
    "Partition size cannot exceed Int.MaxValue")

  override def compute(splitIn: Partition, context: TaskContext): Iterator[T] = {
    val split = splitIn.asInstanceOf[RandomRDDPartition[T]]
    RandomRDD.getPointIterator[T](split)
  }

  override def getPartitions: Array[Partition] = {
    RandomRDD.getPartitions(size, numPartitions, rng, seed)
  }
}

//如何产生随机数向量RDD
private[mllib] class RandomVectorRDD(@transient sc: SparkContext,
    size: Long,
    vectorSize: Int,
    numPartitions: Int,
    @transient rng: RandomDataGenerator[Double],
    @transient seed: Long = Utils.random.nextLong) extends RDD[Vector](sc, Nil) {

  require(size > 0, "Positive RDD size required.")
  require(numPartitions > 0, "Positive number of partitions required")
  require(vectorSize > 0, "Positive vector size required.")
  require(math.ceil(size.toDouble / numPartitions) <= Int.MaxValue,
    "Partition size cannot exceed Int.MaxValue")

  override def compute(splitIn: Partition, context: TaskContext): Iterator[Vector] = {
    val split = splitIn.asInstanceOf[RandomRDDPartition[Double]]
    RandomRDD.getVectorIterator(split, vectorSize)
  }

  override protected def getPartitions: Array[Partition] = {
    RandomRDD.getPartitions(size, numPartitions, rng, seed)
  }
}

private[mllib] object RandomRDD {

  //产生partition集合
  def getPartitions[T](
      size: Long,//产生多少条数据,即输入源
      numPartitions: Int,//多少个分区
      rng: RandomDataGenerator[T],//如何产生数据
      seed: Long): Array[Partition] = { //随机种子

    val partitions = new Array[RandomRDDPartition[T]](numPartitions) //产生若干个随机partition
    var i = 0//第几个分区
    var start: Long = 0
    var end: Long = 0
    val random = new Random(seed)
    while (i < numPartitions) {//循环，产生若干个分组
      end = ((i + 1) * size) / numPartitions
      partitions(i) = new RandomRDDPartition(i, (end - start).toInt, rng, random.nextLong())
      start = end
      i += 1
    }
    partitions.asInstanceOf[Array[Partition]]
  }

  // The RNG has to be reset every time the iterator is requested to guarantee same data
  // every time the content of the RDD is examined.
  //随机产生若干条数据,充实该partition
  def getPointIterator[T: ClassTag](partition: RandomRDDPartition[T]): Iterator[T] = {
    val generator = partition.generator.copy()//拷贝 如何生成随机数对象
    generator.setSeed(partition.seed)//设置随机种子
    Iterator.fill(partition.size)(generator.nextValue())
  }

  // The RNG has to be reset every time the iterator is requested to guarantee same data
  // every time the content of the RDD is examined.
  //产生随机向量
  def getVectorIterator(
      partition: RandomRDDPartition[Double],
      vectorSize: Int): Iterator[Vector] = {
    val generator = partition.generator.copy() //拷贝 如何生成随机数对象
    generator.setSeed(partition.seed) //设置随机种子
    Iterator.fill(partition.size)(new DenseVector(Array.fill(vectorSize)(generator.nextValue())))
  }
}
