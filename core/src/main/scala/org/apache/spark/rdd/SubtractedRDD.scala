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

import java.util.{HashMap => JHashMap}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.Dependency
import org.apache.spark.OneToOneDependency
import org.apache.spark.Partition
import org.apache.spark.Partitioner
import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext
import org.apache.spark.serializer.Serializer

/**
 * An optimized version of cogroup for set difference/subtraction.
 *
 * It is possible to implement this operation with just `cogroup`, but
 * that is less efficient because all of the entries from `rdd2`, for
 * both matching and non-matching values in `rdd1`, are kept in the
 * JHashMap until the end.
 *
 * With this implementation, only the entries from `rdd1` are kept in-memory,
 * and the entries from `rdd2` are essentially streamed, as we only need to
 * touch each once to decide if the value needs to be removed.
 *
 * This is particularly helpful when `rdd1` is much smaller than `rdd2`, as
 * you can use `rdd1`'s partitioner/partition size and not worry about running
 * out of memory because of the size of `rdd2`.
 * 对两个rdd进行交互,获取RDD1存在,RDD2不存在数据,即差异的元素
 *
 * 如果partitioner不同,则要先进行shuffle,确保同一个key都在同一个partition中
 * 然后才能方便判断是否存在Key
 */
private[spark] class SubtractedRDD[K: ClassTag, V: ClassTag, W: ClassTag](
    @transient var rdd1: RDD[_ <: Product2[K, V]],//K-V键值对的RDD1
    @transient var rdd2: RDD[_ <: Product2[K, W]],//K-W键值对的RDD2
    part: Partitioner)
  extends RDD[(K, V)](rdd1.context, Nil) {

  private var serializer: Option[Serializer] = None

  /** Set a serializer for this RDD's shuffle, or null to use the default (spark.serializer) */
  def setSerializer(serializer: Serializer): SubtractedRDD[K, V, W] = {
    this.serializer = Option(serializer)
    this
  }

  /**
   * 因为两个RDD,因此每一个RDD都有自己的partitioner,而SubtractedRDD已经定义好了自己的partitioner了
   * 因此如果两个RDD分别看,如果与SubtractedRDD提供的partitioner相同,那么就是一对一的,
   * 如果RDD的partitioner与SubtractedRDD的不一样,则要进行一次shuffle处理
   */
  override def getDependencies: Seq[Dependency[_]] = {
    Seq(rdd1, rdd2).map { rdd =>
      if (rdd.partitioner == Some(part)) {
        logDebug("Adding one-to-one dependency with " + rdd)
        new OneToOneDependency(rdd)
      } else {
        logDebug("Adding shuffle dependency with " + rdd)
        new ShuffleDependency(rdd, part, serializer)
      }
    }
  }

  //每一个Partition是CoGroupPartition类型的
  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](part.numPartitions) //一共多少个分区
    for (i <- 0 until array.length) {//循环每一个分区
      // Each CoGroupPartition will depend on rdd1 and rdd2
      array(i) = new CoGroupPartition(i, Seq(rdd1, rdd2).zipWithIndex.map { case (rdd, j) =>
        dependencies(j) match {//获取该RDD对应的序号,即是RDD1还是RDD2,对应的是哪一个依赖
          case s: ShuffleDependency[_, _, _] =>
            None
          case _ =>
            Some(new NarrowCoGroupSplitDep(rdd, i, rdd.partitions(i)))
        }
      }.toArray)
    }
    array
  }

  override val partitioner = Some(part)

  override def compute(p: Partition, context: TaskContext): Iterator[(K, V)] = {
    val partition = p.asInstanceOf[CoGroupPartition]

    //该map可能当不太一样的数据很多的时候,会造成内存益处的可能,但是也不会,因为一个partition的内容都已经装进来了,而一个文件数据块又128M,因此也不会造成内存溢出,但是至少该方法一定是耗费内存的方法
    val map = new JHashMap[K, ArrayBuffer[V]] //最终存储rdd1中存在,rdd2中不存在的元素内容

    //返回key对应的一个数组集合
    def getSeq(k: K): ArrayBuffer[V] = {
      val seq = map.get(k)
      if (seq != null) {
        seq
      } else {
        val seq = new ArrayBuffer[V]()
        map.put(k, seq)
        seq
      }
    }

    //第一个参数可以知道是RDD1还是RDD2
    //第二个参数传入一个key-value元组,无返回值的函数
    def integrate(depNum: Int, op: Product2[K, V] => Unit) = {
      //depNum=0 表示RDD1 depNum=1 表示RDD2
      dependencies(depNum) match {
        case oneToOneDependency: OneToOneDependency[_] =>
          val dependencyPartition = partition.narrowDeps(depNum).get.split
          oneToOneDependency.rdd.iterator(dependencyPartition, context)
            .asInstanceOf[Iterator[Product2[K, V]]].foreach(op) //循环每一个元素,让其调用op函数

        case shuffleDependency: ShuffleDependency[_, _, _] =>
          val iter = SparkEnv.get.shuffleManager
            .getReader(
              shuffleDependency.shuffleHandle, partition.index, partition.index + 1, context)
            .read() //先做shuffle处理,然后找到对应的partition读取数据
          iter.foreach(op) //循环每一个元素,让其调用op函数
      }
    }

    // the first dep is rdd1; add all values to the map
    integrate(0, t => getSeq(t._1) += t._2) //添加value到给定的key对应的数组中
    // the second dep is rdd2; remove all of its keys
    integrate(1, t => map.remove(t._1)) //如果发现,从map中移除对应的key,说明没差异,因此从map中删除该key

    //其中t表示(K, ArrayBuffer[V])组成的元组
    //因此每一个k被循环后,value数组转换成(key,value)元组集合
    map.iterator.map { t => t._2.iterator.map { (t._1, _) } }.flatten
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
  }

}
