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

import org.apache.spark.{Logging, Partitioner, RangePartitioner}
import org.apache.spark.annotation.DeveloperApi

/**
 * Extra functions available on RDDs of (key, value) pairs where the key is sortable through
 * an implicit conversion. They will work with any key type `K` that has an implicit `Ordering[K]`
 * in scope. Ordering objects already exist for all of the standard primitive types. Users can also
 * define their own orderings for custom types, or to override the default ordering. The implicit
 * ordering that is in the closest scope will be used.
 *
 * {{{
 *   import org.apache.spark.SparkContext._
 *
 *   val rdd: RDD[(String, Int)] = ...
 *   implicit val caseInsensitiveOrdering = new Ordering[String] {
 *     override def compare(a: String, b: String) = a.toLowerCase.compare(b.toLowerCase)
 *   }
 *
 *   // Sort by key, using the above case insensitive ordering.
 *   rdd.sortByKey()
 * }}}
 * 对RDD[K,V]形式的数据,其中K是支持排序的隐式转换,提供一些附加功能
 */
class OrderedRDDFunctions[K : Ordering : ClassTag,//可以排序的K
                          V: ClassTag,
                          P <: Product2[K, V] : ClassTag] @DeveloperApi() ( //输入源
    self: RDD[P])//包含K,V结果的RDD数据元源
  extends Logging with Serializable
{
  private val ordering = implicitly[Ordering[K]]

  /**
   * Sort the RDD by key, so that each partition contains a sorted range of the elements. Calling
   * `collect` or `save` on the resulting RDD will return or output an ordered list of records
   * (in the `save` case, they will be written to multiple `part-X` files in the filesystem, in
   * order of the keys).
   * 对RDD[K,V]结构的数据进行排序,按照K进行排序,返回排序后的RDD[K,V]
   */
  // TODO: this currently doesn't work on P other than Tuple2!
  def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.length)
      : RDD[(K, V)] = self.withScope
  {
    val part = new RangePartitioner(numPartitions, self, ascending) //该RDD的partition,采用抽样的方式,根据抽样的key的排列顺序,划分了key的partition区间,从而保证了key会分配到对应的排序好的partition中,即最终的每一个partition都是有顺序的,
    new ShuffledRDD[K, V, V](self, part)//将该RDD作为shuffle的数据源RDD
      .setKeyOrdering(if (ascending) ordering else ordering.reverse) //设置Key的排序方式,shuffle又保证了每一个partition内部又有顺序,因此最终就是按照key排号顺序的,不需要map-reduce中最终一个reduce进行排序方案了
  }

  /**
   * Repartition the RDD according to the given partitioner and, within each resulting partition,
   * sort records by their keys.
   *
   * This is more efficient than calling `repartition` and then sorting within each partition
   * because it can push the sorting down into the shuffle machinery.
   * 重新分配partition数量,并且保证每一个shuffle的结果是有顺序的,但是不保证多个partition文件中key是有顺序的
   */
  def repartitionAndSortWithinPartitions(partitioner: Partitioner): RDD[(K, V)] = self.withScope {
    new ShuffledRDD[K, V, V](self, partitioner).setKeyOrdering(ordering)
  }

  /**
   * Returns an RDD containing only the elements in the the inclusive range `lower` to `upper`.
   * If the RDD has been partitioned using a `RangePartitioner`, then this operation can be
   * performed efficiently by only scanning the partitions that might contain matching elements.
   * Otherwise, a standard `filter` is applied to all partitions.
   * 设置K的区间范围,即给定该区间的K的最小值和最大值
   * 查找全部RDD中的一个子集,子集的内容是key在[lower,upper]区间即可
   */
  def filterByRange(lower: K, upper: K): RDD[P] = self.withScope {

    //true表示参数K在[lower,upper]区间
    def inRange(k: K): Boolean = ordering.gteq(k, lower) && ordering.lteq(k, upper)

    //如何过滤RDD,返回的还是RDD本身,只是过滤了一层
    val rddToFilter: RDD[P] = self.partitioner match {
      case Some(rp: RangePartitioner[K, V]) => {//对RDD本身进行过滤
        val partitionIndicies = (rp.getPartition(lower), rp.getPartition(upper)) match {//因此可以根据key的过滤器,就能算出来只要扫描哪些partition即可,不需要扫描全部的partition
          case (l, u) => Math.min(l, u) to Math.max(l, u)
        }
        PartitionPruningRDD.create(self, partitionIndicies.contains) //过滤,只要partitionIndicies包含的partitionid
      }
      case _ =>
        self //如果不是RangePartitioner,则就返回RDD本身自己
    }
    rddToFilter.filter { case (k, v) => inRange(k) }//在给定rddToFilter方法的返回值下,继续filter过滤,只要K在[lower,upper]区间的内容
  }

}
