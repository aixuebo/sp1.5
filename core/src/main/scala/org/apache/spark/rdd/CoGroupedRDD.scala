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

import scala.language.existentials

import java.io.{IOException, ObjectOutputStream}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.collection.{ExternalAppendOnlyMap, AppendOnlyMap, CompactBuffer}
import org.apache.spark.util.Utils
import org.apache.spark.serializer.Serializer

/** The references to rdd and splitIndex are transient because redundant information is stored
  * in the CoGroupedRDD object.  Because CoGroupedRDD is serialized separately from
  * CoGroupPartition, if rdd and splitIndex aren't transient, they'll be included twice in the
  * task closure. 
  * 
  * CoGroupedRDD的每一个partition对应一个CoGroupPartition对象
  * 而CoGroupPartition对象中有一个Array数组NarrowCoGroupSplitDep
  * 
  * NarrowCoGroupSplitDep表示在CoGroupedRDD的一个partition中,从什么父RDD中获取数据
  **/
private[spark] case class NarrowCoGroupSplitDep(
    @transient rdd: RDD[_],//父RDD,也可能是None,此时说明是ShuffleDependency
    @transient splitIndex: Int,//第几个partition
    var split: Partition //该splitIndex对应的Partition对象
  ) extends Serializable {

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    split = rdd.partitions(splitIndex) //获取RDD的第splitIndex个partition对象
    oos.defaultWriteObject()
  }
}

/**
 * Stores information about the narrow dependencies used by a CoGroupedRdd.
 *
 * @param narrowDeps maps to the dependencies variable in the parent RDD: for each one to one
 *                   dependency in dependencies, narrowDeps has a NarrowCoGroupSplitDep (describing
 *                   the partition for that dependency) at the corresponding index. The size of
 *                   narrowDeps should always be equal to the number of parents.
 *  该类表示CoGroupedRDD的每一个partition对应一个该对象
 *  idx 表示该CoGroupPartition是CoGroupedRDD的第几个partition
 *  narrowDeps
 */
private[spark] class CoGroupPartition(
    idx: Int, val narrowDeps: Array[Option[NarrowCoGroupSplitDep]])
  extends Partition with Serializable {
  override val index: Int = idx //表示该CoGroupPartition是CoGroupedRDD的第几个partition
  override def hashCode(): Int = idx
}

/**
 * :: DeveloperApi ::
 * A RDD that cogroups its parents. For each key k in parent RDDs, the resulting RDD contains a
 * tuple with the list of values for that key.
 * 每一个父RDD的key,与每一个父RDD对应的value迭代器组合作为返回值
 * Note: This is an internal API. We recommend users use RDD.cogroup(...) instead of
 * instantiating this directly.

 * @param rdds parent RDDs.父RDD集合
 * @param part partitioner used to partition the shuffle output
 * eg: 传入4个RDD
 * val cg = new CoGroupedRDD[K](Seq(self, other1, other2, other3), partitioner)
 * 返回值是RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))],即key与三个RDD对应的迭代器组成的元组作为value返回
 */
@DeveloperApi
class CoGroupedRDD[K](@transient var rdds: Seq[RDD[_ <: Product2[K, _]]], part: Partitioner) //参数是一组RDD集合做join
  extends RDD[(K, Array[Iterable[_]])](rdds.head.context, Nil) {//返回值是key,value是Array数组,每一个元素是一个迭代器,每一个迭代器都是RDD与key关联后的返回值,该RDD最终也是根RDD,因为父RDD是nil

  // For example, `(k, a) cogroup (k, b)` produces k -> Array(ArrayBuffer as, ArrayBuffer bs).
  // Each ArrayBuffer is represented as a CoGroup, and the resulting Array as a CoGroupCombiner.
  // CoGroupValue is the intermediate state of each value before being merged in compute.
  private type CoGroup = CompactBuffer[Any] //类似于ArrayBuffer.但是内存使用更有效,该数组存储任务对象作为元素
  private type CoGroupValue = (Any, Int)  // Int is dependency number <元素内容,第几个rdd产生的元素>
  private type CoGroupCombiner = Array[CoGroup] //数组,每一个元素是CompactBuffer,即这个是一个二维数组

  private var serializer: Option[Serializer] = None

  /** Set a serializer for this RDD's shuffle, or null to use the default (spark.serializer) */
  def setSerializer(serializer: Serializer): CoGroupedRDD[K] = {
    this.serializer = Option(serializer)
    this
  }

  /**
   * 循环每一个RDD,返回每一个RDD与新的RDD之间的关系
   * 例如返回值是 seq[OneToOneDependency,OneToOneDependency,ShuffleDependency] 表示该CoGroupedRDD有3个父RDD,与三个RDD每一个对应依赖关系是OneToOneDependency,OneToOneDependency,ShuffleDependency
   * 该实现逻辑是 如果多个RDD的partitoner不同,则要先进行shuffle,确保同一个key都在同一个partition中,然后才能方便判断是否存在Key
   */
  override def getDependencies: Seq[Dependency[_]] = {
    rdds.map { rdd: RDD[_] =>
      if (rdd.partitioner == Some(part)) {//partitioner类型相同,并且分区数量相同,因此是一对一依赖
        logDebug("Adding one-to-one dependency with " + rdd)
        new OneToOneDependency(rdd)
      } else {
        logDebug("Adding shuffle dependency with " + rdd)
        new ShuffleDependency[K, Any, CoGroupCombiner](
          rdd.asInstanceOf[RDD[_ <: Product2[K, _]]], part, serializer)
      }
    }
  }

  //返回新的RDD提供多少个Partition,每一个Partition是CoGroupPartition,每一个CoGroupPartition对应该Partition要抓取每一个父RDD的哪些Partition
  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](part.numPartitions) //该CoGroupedRDD最后有多少个partition
    for (i <- 0 until array.length) {//为CoGroupedRDD的每一个partition分配数据,每一个partition是CoGroupPartition对象
      // Each CoGroupPartition will have a dependency per contributing RDD 循环每一个父RDD以及j代表父RDD对应是第几个父RDD
      array(i) = new CoGroupPartition(i, //第几个分区
        rdds.zipWithIndex.map { case (rdd, j) => //该分区执行每一个rdd与对应的rdd序号
        // Assume each RDD contributed a single dependency, and get it
        dependencies(j) match {
          case s: ShuffleDependency[_, _, _] =>
            None
          case _ =>
            Some(new NarrowCoGroupSplitDep(rdd, i, rdd.partitions(i)))
        }
      }.toArray)
    }
    array
  }

  //获取拆分器对象
  override val partitioner: Some[Partitioner] = Some(part)

  override def compute(s: Partition, context: TaskContext): Iterator[(K, Array[Iterable[_]])] = {
    val sparkConf = SparkEnv.get.conf
    val externalSorting = sparkConf.getBoolean("spark.shuffle.spill", true) //true表示内存不够的时候,进行外部排序存储
    val split = s.asInstanceOf[CoGroupPartition]
    val numRdds = dependencies.length //一共多少个RDD做join

    // A list of (rdd iterator, dependency number) pairs,数组的每一个元素组成,key是每一个父RDD的某个partition的输出流迭代器,value是该父RDD是第几个依赖的RDD
    val rddIterators = new ArrayBuffer[(Iterator[Product2[K, Any]], Int)]

    //循环所有依赖的父RDD集合,元组是依赖的父RDD,以及父RDD的序号
    for ((dep, depNum) <- dependencies.zipWithIndex) dep match {
      //如果依赖是一对一的,所以只要找到该父RDD对应的一个partiton即可
      case oneToOneDependency: OneToOneDependency[Product2[K, Any]] @unchecked =>
        val dependencyPartition = split.narrowDeps(depNum).get.split //为该父RDD对应的partition对象,该partition对象就是为子RDD分配到的partition
        // Read them from the parent,读取该父RDD的partition,返回K, Any类型的迭代器
        val it = oneToOneDependency.rdd.iterator(dependencyPartition, context) //读取某一个rdd分区的数据迭代器
        rddIterators += ((it, depNum)) //追加该迭代器,以及第几个父RDD序号

      case shuffleDependency: ShuffleDependency[_, _, _] =>
        // Read map outputs of shuffle 如果依赖是需要shuffer的,即父RDD的N个partition都要向N个新的RDD写入数据,因此要读取父RDD中数据,以shuffle方式读取
        val it = SparkEnv.get.shuffleManager
          .getReader(shuffleDependency.shuffleHandle, split.index, split.index + 1, context)
          .read()
        rddIterators += ((it, depNum))
    }

    if (!externalSorting) {//不用外部排序,就用内存排序


      //每一个key对应一个
      val map = new AppendOnlyMap[K, CoGroupCombiner] //类似hash table的实现,每一个key对应一个合并对象
      val update: (Boolean, CoGroupCombiner) => CoGroupCombiner = (hadVal, oldVal) => {
        if (hadVal) oldVal else Array.fill(numRdds)(new CoGroup) //相当于list
      }

      val getCombiner: K => CoGroupCombiner = key => {
        map.changeValue(key, update)
      }

      //因为k-v结构,没有对key和value进行排序.因此最终输出的结果也是没有排序的
      rddIterators.foreach { case (it, depNum) => //循环每一个rdd的内容,以及该rdd所在的序号
        while (it.hasNext) {
          val kv = it.next()
          getCombiner(kv._1)(depNum) += kv._2
        }
      }
      new InterruptibleIterator(context,
        map.iterator.asInstanceOf[Iterator[(K, Array[Iterable[_]])]])
    } else {//外部存储排序
      val map = createExternalMap(numRdds) //多少个数据源RDD
      for ((it, depNum) <- rddIterators) {
        map.insertAll(it.map(pair => (pair._1, new CoGroupValue(pair._2, depNum)))) // <元素内容,第几个rdd产生的元素>
      }
      context.taskMetrics().incMemoryBytesSpilled(map.memoryBytesSpilled)
      context.taskMetrics().incDiskBytesSpilled(map.diskBytesSpilled)
      context.internalMetricsToAccumulators(
        InternalAccumulator.PEAK_EXECUTION_MEMORY).add(map.peakMemoryUsedBytes)
      new InterruptibleIterator(context,
        map.iterator.asInstanceOf[Iterator[(K, Array[Iterable[_]])]])
    }
  }

  //创建外部的存储对象
  private def createExternalMap(numRdds: Int)
    : ExternalAppendOnlyMap[K, CoGroupValue, CoGroupCombiner] = {

    //<元素内容,第几个rdd产生的元素> --> Array[ArrayBuffer],即将数据元素添加到对应的流List中
    val createCombiner: (CoGroupValue => CoGroupCombiner) = value => {
      val newCombiner = Array.fill(numRdds)(new CoGroup)
      newCombiner(value._2) += value._1
      newCombiner
    }

    //即将数据元素添加到对应的流List中
    val mergeValue: (CoGroupCombiner, CoGroupValue) => CoGroupCombiner =
      (combiner, value) => {
      combiner(value._2) += value._1
      combiner
    }

    val mergeCombiners: (CoGroupCombiner, CoGroupCombiner) => CoGroupCombiner =
      (combiner1, combiner2) => {
        var depNum = 0
        while (depNum < numRdds) {
          combiner1(depNum) ++= combiner2(depNum)
          depNum += 1
        }
        combiner1
      }
    new ExternalAppendOnlyMap[K, CoGroupValue, CoGroupCombiner](
      createCombiner, mergeValue, mergeCombiners)
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdds = null
  }
}
