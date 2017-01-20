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

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.{ClassTag, classTag}
import scala.util.hashing.byteswap32

import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.{CollectionsUtils, Utils}
import org.apache.spark.util.random.{XORShiftRandom, SamplingUtils}

/**
 * An object that defines how the elements in a key-value pair RDD are partitioned by key.
 * Maps each key to a partition ID, from 0 to `numPartitions - 1`.
 * 如何分配key到第几个partition上
 * 如果是key-value键值对的RDD,则仅仅通过key去判断属于哪个partition
 */
abstract class Partitioner extends Serializable {
  def numPartitions: Int //一共多少个partition,此时partition是目标reduce的数量
  def getPartition(key: Any): Int //将key分配到第几个partition上
}

object Partitioner {
  /**
   * Choose a partitioner to use for a cogroup-like operation between a number of RDDs.
   *
   * If any of the RDDs already has a partitioner, choose that one.
   *
   * Otherwise, we use a default HashPartitioner. For the number of partitions, if
   * spark.default.parallelism is set, then we'll use the value from SparkContext
   * defaultParallelism, otherwise we'll use the max number of upstream partitions.
   *
   * Unless spark.default.parallelism is set, the number of partitions will be the
   * same as the number of partitions in the largest upstream RDD, as this should
   * be least likely to cause out-of-memory errors.
   *
   * We use two method parameters (rdd, others) to enforce callers passing at least 1 RDD.
   */
  def defaultPartitioner(rdd: RDD[_], others: RDD[_]*): Partitioner = {
    val bySize = (Seq(rdd) ++ others).sortBy(_.partitions.size).reverse //按照RDD中partition数量多少进行所有的RDD排序,即最终结果是第一个排序的是partitionSize最多的
    for (r <- bySize if r.partitioner.isDefined && r.partitioner.get.numPartitions > 0) {
      return r.partitioner.get //选择partitionSize最多的RDD对应的Partitioner对象
    }
    if (rdd.context.conf.contains("spark.default.parallelism")) {
      new HashPartitioner(rdd.context.defaultParallelism)
    } else {
      new HashPartitioner(bySize.head.partitions.size)
    }
  }
}

/**
 * A [[org.apache.spark.Partitioner]] that implements hash-based partitioning using
 * Java's `Object.hashCode`.
 *
 * Java arrays have hashCodes that are based on the arrays' identities rather than their contents,
 * so attempting to partition an RDD[Array[_]] or RDD[(Array[_], _)] using a HashPartitioner will
 * produce an unexpected or incorrect result.
 * 对key进行hash,然后取模即可
 */
class HashPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => Utils.nonNegativeMod(key.hashCode, numPartitions)
  }

  override def equals(other: Any): Boolean = other match {
    case h: HashPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}

/**
 * A [[org.apache.spark.Partitioner]] that partitions sortable records by range into roughly
 * equal ranges. The ranges are determined by sampling the content of the RDD passed in.
 *
 * Note that the actual number of partitions created by the RangePartitioner might not be the same
 * as the `partitions` parameter, in the case where the number of sampled records is less than
 * the value of `partitions`.
 * 针对K-v键值对,并且Key是可排序的
 * 
 * 1.每一个partition内部是无序的,但是partition和partition之间是有顺序的
 * 2.性能上有损耗,因此要先遍历一次RDD,进行抽样
 *
 *
 * 该RDD的partition,采用抽样的方式,根据抽样的key的排列顺序,划分了key的partition区间,从而保证了key会分配到对应的排序好的partition中,即最终的每一个partition都是有顺序的,但是每一个partition内部是不会按照key排序的,需要在shuffle中加入对key排序的方法,才能保证最终全部结果都是排序好的
 */
class RangePartitioner[K : Ordering : ClassTag, V](
    @transient partitions: Int,//最终要分成多少个partition
    @transient rdd: RDD[_ <: Product2[K, V]],//rdd输入源
    private var ascending: Boolean = true)//默认升序
  extends Partitioner {

  // We allow partitions = 0, which happens when sorting an empty RDD under the default settings.
  require(partitions >= 0, s"Number of partitions cannot be negative but found $partitions.")

  private var ordering = implicitly[Ordering[K]]

  // An array of upper bounds for the first (partitions - 1) partitions
  //抽样获取数组大小为N,每一个元素都是K,
  private var rangeBounds: Array[K] = {
    if (partitions <= 1) {
      Array.empty
    } else {
      // This is the sample size we need to have roughly balanced output partitions, capped at 1M.
      val sampleSize = math.min(20.0 * partitions, 1e6) //采样总数,1e6 = 1000000.0,即最多1000000,大概是1M,基本上是每一个partitions抽取20个做参考
      // Assume the input partitions are roughly balanced and over-sample a little bit.
      /**
       * 注意
       * 正常来说,每一个partition采样数量为sampleSize / rdd.partitions.size即可
       * 但是代码上父RDD每个分区需要采样的数据量是正常数的3倍。
       * 这是因为父RDD各分区中的数据量可能会出现倾斜的情况，乘于3的目的就是保证数据量小的分区能够采样到足够的数据，而对于数据量大的分区会进行第二次采样。
       */
      val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rdd.partitions.size).toInt //每一个partition获取多少个元素
      //rdd.map(_._1) 按照key映射成新的RDD,对新的RDD进行取样,每一个partition取sampleSizePerPartition个元素
      val (numItems, sketched) = RangePartitioner.sketch(rdd.map(_._1), sampleSizePerPartition) 
      if (numItems == 0L) {//一共RDD中有多少数据,则返回空数组
        Array.empty
      } else {//说明抽取出元素了
        // If a partition contains much more than the average number of items, we re-sample from it
        // to ensure that enough items are collected from that partition.
        //如果一个分区极端情况下包含的数据太多了,多于平均值,则重新对该分区进行抓取数据
        //如果某一个partition分区全部数据都在该分区,其他分区都是0个数据,因此fraction才会等于1,否则一定是小于1的
        val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0) //抓取因子,表示平均每一个partition分区最好抓取多少个百分比的数据
        val candidates = ArrayBuffer.empty[(K, Float)] //候选人,每一个元素是一个元组,由key候选人以及该key的权重组成,权重就是从N个集合中抽取M个数据,即抽取比例,比如从100个元素中抽取了10个,则抽取比例是10%
        val imbalancedPartitions = mutable.Set.empty[Int] //不平衡的partitionId集合
        sketched.foreach { case (idx, n, sample) =>
          if (fraction * n > sampleSizePerPartition) {//该partition平均抓取占比*该partition总条数,大于了sampleSizePerPartition,则说明该partition数据太多,要重新分配
            imbalancedPartitions += idx
          } else {//说明该partition分配比较平衡
            // The weight is 1 over the sampling probability.
            val weight = (n.toDouble / sample.size).toFloat //为该partition的所有key进行赋权重,权重就是从N个集合中抽取M个数据,即抽取比例,比如从100个元素中抽取了10个,则抽取比例是10%
            for (key <- sample) {
              candidates += ((key, weight))
            }
          }
        }
        
        //进行重新抽样
        if (imbalancedPartitions.nonEmpty) {
          // Re-sample imbalanced partitions with the desired sampling probability.
          val imbalanced = new PartitionPruningRDD(rdd.map(_._1), imbalancedPartitions.contains)
          val seed = byteswap32(-rdd.id - 1)
          val reSampled = imbalanced.sample(withReplacement = false, fraction, seed).collect()
          val weight = (1.0 / fraction).toFloat
          candidates ++= reSampled.map(x => (x, weight))
        }
        
        //从候选人集合中拆分成partitions个集合
        RangePartitioner.determineBounds(candidates, partitions)
      }
    }
  }

  def numPartitions: Int = rangeBounds.length + 1 //初始化

  private var binarySearch: ((Array[K], K) => Int) = CollectionsUtils.makeBinarySearch[K] //二分法查找K是否在数组K中存在,存在的话,返回下标位置

  //找到该key在哪个partition中,因为partition是按照key是有顺序的,因此一个key在哪个partition是可以确定的
  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[K]
    var partition = 0
    if (rangeBounds.length <= 128) {//如果抽样总数组长度小于128,则可以一个一个比较即可,找到key存储的partition位置
      // If we have less than 128 partitions naive search
      while (partition < rangeBounds.length && ordering.gt(k, rangeBounds(partition))) {
        partition += 1
      }
    } else {
      // Determine which binary search method to use only once.二分法查找key
      partition = binarySearch(rangeBounds, k)
      // binarySearch either returns the match location or -[insertion point]-1
      if (partition < 0) {
        partition = -partition-1
      }
      if (partition > rangeBounds.length) {
        partition = rangeBounds.length
      }
    }
    
    //根据升降序,返回key对应的partition位置
    if (ascending) {
      partition
    } else {
      rangeBounds.length - partition
    }
  }

  override def equals(other: Any): Boolean = other match {
    case r: RangePartitioner[_, _] =>
      r.rangeBounds.sameElements(rangeBounds) && r.ascending == ascending
    case _ =>
      false
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    var i = 0
    while (i < rangeBounds.length) {
      result = prime * result + rangeBounds(i).hashCode
      i += 1
    }
    result = prime * result + ascending.hashCode
    result
  }

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => out.defaultWriteObject()
      case _ =>
        out.writeBoolean(ascending)
        out.writeObject(ordering)
        out.writeObject(binarySearch)

        val ser = sfactory.newInstance()
        Utils.serializeViaNestedStream(out, ser) { stream =>
          stream.writeObject(scala.reflect.classTag[Array[K]])
          stream.writeObject(rangeBounds) //序列化初始化的数组
        }
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => in.defaultReadObject()
      case _ =>
        ascending = in.readBoolean()
        ordering = in.readObject().asInstanceOf[Ordering[K]]
        binarySearch = in.readObject().asInstanceOf[(Array[K], K) => Int]

        val ser = sfactory.newInstance()
        Utils.deserializeViaNestedStream(in, ser) { ds =>
          implicit val classTag = ds.readObject[ClassTag[Array[K]]]()
          rangeBounds = ds.readObject[Array[K]]()
        }
    }
  }
}

private[spark] object RangePartitioner {

  /**
   * Sketches the input RDD via reservoir sampling on each partition.
   *
   * @param rdd the input RDD to sketch 在RDD中抽取数据
   * @param sampleSizePerPartition max sample size per partition 每一个partition抽取多少数据
   * @return (total number of items, an array of (partitionId, number of items, sample))
   * 返回值1表示一共抽取了多少个数据集,2表示 Array[(Int, Int, Array[K])],第一个数组表示多少个partition,数组的每一个元素中包含3个元素,分别表示partitionId,该partitionId一共多少个元素,抽取元素组成的数组
   */
  def sketch[K : ClassTag](
      rdd: RDD[K],
      sampleSizePerPartition: Int): (Long, Array[(Int, Int, Array[K])]) = {
    val shift = rdd.id
    // val classTagK = classTag[K] // to avoid serializing the entire partitioner object
    val sketched = rdd.mapPartitionsWithIndex { (idx, iter) =>
      val seed = byteswap32(idx ^ (shift << 16)) //随机因子
      val (sample, n) = SamplingUtils.reservoirSampleAndCount(
        iter, sampleSizePerPartition, seed) //从idx对应的partition的数据源iter中,随机抽取sampleSizePerPartition个元素,返回值sample表示抽取完成的元素数组,n表示该partition中一共多少条数据
      Iterator((idx, n, sample)) //将三个返回值作为迭代器返回
    }.collect()
    val numItems = sketched.map(_._2.toLong).sum //表示该RDD一共多少条数据
    (numItems, sketched) //返回值1表示一共抽取了多少个数据集,2表示 Array[(Int, Int, Array[K])],第一个数组表示多少个partition,数组的每一个元素中包含3个元素,分别表示partitionId,该partitionId一共多少个元素,抽取元素组成的数组
  }

  /**
   * Determines the bounds for range partitioning from candidates with weights indicating how many
   * items each represents. Usually this is 1 over the probability used to sample this candidate.
   *
   * @param candidates unordered candidates with weights
   * @param partitions number of partitions
   * @return selected bounds
   * 从候选人集合中拆分成partitions个集合
   */
  def determineBounds[K : Ordering : ClassTag](
      candidates: ArrayBuffer[(K, Float)],//候选人集合
      partitions: Int): Array[K] = {
    val ordering = implicitly[Ordering[K]]
    val ordered = candidates.sortBy(_._1) //首先按照key进行排序
    val numCandidates = ordered.size //确定一共多少个候选人
    val sumWeights = ordered.map(_._2.toDouble).sum //候选人总权重值
    val step = sumWeights / partitions
    var cumWeight = 0.0
    var target = step
    val bounds = ArrayBuffer.empty[K]
    var i = 0
    var j = 0
    var previousBound = Option.empty[K]
    while ((i < numCandidates) && (j < partitions - 1)) {
      val (key, weight) = ordered(i)
      cumWeight += weight
      if (cumWeight > target) {
        // Skip duplicate values.
        if (previousBound.isEmpty || ordering.gt(key, previousBound.get)) {
          bounds += key
          target += step
          j += 1
          previousBound = Some(key)
        }
      }
      i += 1
    }
    bounds.toArray
  }
}
