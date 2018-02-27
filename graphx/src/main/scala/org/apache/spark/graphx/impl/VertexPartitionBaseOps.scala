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

package org.apache.spark.graphx.impl

import scala.language.higherKinds
import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.Logging
import org.apache.spark.util.collection.BitSet

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap

/**
 * An class containing additional operations for subclasses of VertexPartitionBase that provide
 * implicit evidence of membership in the `VertexPartitionBaseOpsConstructor` typeclass (for
 * example, [[VertexPartition.VertexPartitionOpsConstructor]]).
 */
private[graphx] abstract class VertexPartitionBaseOps
    [VD: ClassTag, Self[X] <: VertexPartitionBase[X] : VertexPartitionBaseOpsConstructor]
    (self: Self[VD])
  extends Serializable with Logging {

  def withIndex(index: VertexIdToIndexMap): Self[VD] //只是替换顶点ID集合,不替换属性,因此返回值还是VD
  def withValues[VD2: ClassTag](values: Array[VD2]): Self[VD2] //因为要替换的是顶点属性,因此返回的是VD2
  def withMask(mask: BitSet): Self[VD] //只是替换顶点ID集合,不替换属性,因此返回值还是VD

  /**
   * Pass each vertex attribute along with the vertex id through a map
   * function and retain the original RDD's partitioning and index.
   *
   * @tparam VD2 the type returned by the map function 转换后的属性对象
   *
   * @param f the function applied to each vertex id and vertex
   * attribute in the RDD 转换函数
   *
   * @return a new VertexPartition with values obtained by applying `f` to
   * each of the entries in the original VertexRDD.  The resulting
   * VertexPartition retains the same index.
   * 将每一个顶点的属性,转换成新的属性VD2
   */
  def map[VD2: ClassTag](f: (VertexId, VD) => VD2): Self[VD2] = {
    // Construct a view of the map transformation
    val newValues = new Array[VD2](self.capacity) //新的属性集合
    var i = self.mask.nextSetBit(0) //一个一个顶点循环
    while (i >= 0) {
      newValues(i) = f(self.index.getValue(i), self.values(i)) //传入该顶点和属性对象---self.index.getValue(i)表示根据索引i序号获取的顶点信息
      i = self.mask.nextSetBit(i + 1) //循环下一个顶点
    }
    this.withValues(newValues) //重新设置新的属性值
  }

  /**
   * Restrict the vertex set to the set of vertices satisfying the given predicate.
   *
   * @param pred the user defined predicate 函数表示判断有效性,根据顶点ID以及顶点元素,返回true表示该顶点依然有效
   *
   * @note The vertex set preserves the original index structure which means that the returned
   *       RDD can be easily joined with the original vertex-set. Furthermore, the filter only
   *       modifies the bitmap index and so no new values are allocated.
   */
  def filter(pred: (VertexId, VD) => Boolean): Self[VD] = {
    // Allocate the array to store the results into
    val newMask = new BitSet(self.capacity)
    // Iterate over the active bits in the old mask and evaluate the predicate
    var i = self.mask.nextSetBit(0) //循环每一个元素
    while (i >= 0) {
      if (pred(self.index.getValue(i), self.values(i))) {//判断该元素是否有效
        newMask.set(i) //表示新的顶点映射集合
      }
      i = self.mask.nextSetBit(i + 1) //继续循环
    }
    this.withMask(newMask) //此时只是更新有效的顶点集合,因为value和key的顺序没有变化,因此不需要改变value和key,只是value和key已经有一些内容没意义了
  }

  /** Hides the VertexId's that are the same between `this` and `other`.
    * 在this中删除other包含的顶点
    **/
  def minus(other: Self[VD]): Self[VD] = {
    if (self.index != other.index) {//两个顶点集合的映射id是不同的
      logWarning("Minus operations on two VertexPartitions with different indexes is slow.")
      minus(createUsingIndex(other.iterator))
    } else {//说明两个顶点集合的id映射是相同的
      self.withMask(self.mask.andNot(other.mask))
    }
  }

  /** Hides the VertexId's that are the same between `this` and `other`. */
  def minus(other: Iterator[(VertexId, VD)]): Self[VD] = {
    minus(createUsingIndex(other))
  }

  /**
   * Hides vertices that are the same between this and other. For vertices that are different, keeps
   * the values from `other`. The indices of `this` and `other` must be the same.
   */
  def diff(other: Self[VD]): Self[VD] = {
    if (self.index != other.index) {
      logWarning("Diffing two VertexPartitions with different indexes is slow.")
      diff(createUsingIndex(other.iterator))
    } else {
      val newMask = self.mask & other.mask //获取join后,两个顶点集合都有的顶点ID集合
      var i = newMask.nextSetBit(0) //循环都有的顶点ID集合
      while (i >= 0) {
        if (self.values(i) == other.values(i)) {//如果两个顶点ID对应的属性值也相同,则删除该顶点
          newMask.unset(i)
        }
        i = newMask.nextSetBit(i + 1)
      }
      //因此最终找到的是相同顶点ID,但是属性值不同的顶点集合.并且返回的值是other的属性值
      this.withValues(other.values).withMask(newMask)
    }
  }

  /** Left outer join another VertexPartition.
    * 因为是left join,因此this的顶点ID和mask都不会有变化,因此该方法就是将两个顶点集合中,相同顶点ID的属性值进行merge操作,产生新的VD3属性值的过程
    **/
  def leftJoin[VD2: ClassTag, VD3: ClassTag]
      (other: Self[VD2])
      (f: (VertexId, VD, Option[VD2]) => VD3): Self[VD3] = { //f函数参数的意义this的顶点ID、this的顶点属性值、other的顶点属性值---产生新的属性值
    if (self.index != other.index) {
      logWarning("Joining two VertexPartitions with different indexes is slow.")
      leftJoin(createUsingIndex(other.iterator))(f)
    } else {
      val newValues = new Array[VD3](self.capacity) //确保顶点的序号是相同的

      var i = self.mask.nextSetBit(0) //循环this所有的顶点
      while (i >= 0) {
        val otherV: Option[VD2] = if (other.mask.get(i)) Some(other.values(i)) else None //返回该顶点在other集合中的属性值
        newValues(i) = f(self.index.getValue(i), self.values(i), otherV) //参数的意义:this的顶点ID、this的顶点属性值、other的顶点属性值---产生新的属性值
        i = self.mask.nextSetBit(i + 1)
      }
      this.withValues(newValues) //设置新的值,因为是left join,因此this的顶点ID和mask都不会有变化
    }
  }

  /** Left outer join another iterator of messages. */
  def leftJoin[VD2: ClassTag, VD3: ClassTag]
      (other: Iterator[(VertexId, VD2)])
      (f: (VertexId, VD, Option[VD2]) => VD3): Self[VD3] = {
    leftJoin(createUsingIndex(other))(f)
  }

  /** Inner join another VertexPartition.
    * 对两个顶点集合取交集,同时将相同顶点ID的属性进行运算,返回新的属性VD2
    **/
  def innerJoin[U: ClassTag, VD2: ClassTag]
      (other: Self[U])
      (f: (VertexId, VD, U) => VD2): Self[VD2] = {
    if (self.index != other.index) {
      logWarning("Joining two VertexPartitions with different indexes is slow.")
      innerJoin(createUsingIndex(other.iterator))(f)
    } else {
      val newMask = self.mask & other.mask //获取顶点集合相同的
      val newValues = new Array[VD2](self.capacity)//确保顶点的序号是相同的
      var i = newMask.nextSetBit(0)
      while (i >= 0) {
        newValues(i) = f(self.index.getValue(i), self.values(i), other.values(i)) //参数是this的顶点ID,this的顶点属性,other的顶点属性
        i = newMask.nextSetBit(i + 1)
      }
      this.withValues(newValues).withMask(newMask)//此时新的属性值有一些位置是无用的,是空的
    }
  }

  /**
   * Inner join an iterator of messages.
   */
  def innerJoin[U: ClassTag, VD2: ClassTag]
      (iter: Iterator[Product2[VertexId, U]])
      (f: (VertexId, VD, U) => VD2): Self[VD2] = {
    innerJoin(createUsingIndex(iter))(f)
  }

  /**
   * Similar effect as aggregateUsingIndex((a, b) => a)
   * 让参数迭代器的属性ID映射与this的映射是相同的
   */
  def createUsingIndex[VD2: ClassTag](iter: Iterator[Product2[VertexId, VD2]])
    : Self[VD2] = {
    val newMask = new BitSet(self.capacity)
    val newValues = new Array[VD2](self.capacity)
    iter.foreach { pair =>
      val pos = self.index.getPos(pair._1) //获取this中该顶点的序号
      if (pos >= 0) {//说明this中存在该顶点
        newMask.set(pos)
        newValues(pos) = pair._2
      }
    }
    this.withValues(newValues).withMask(newMask) //因为key是与this相同的映射,因此可以不用设置key集合
  }

  /**
   * Similar to innerJoin, but vertices from the left side that don't appear in iter will remain in
   * the partition, hidden by the bitmask.
   * 类似与innerJoin,区别:
   * 1.不需要merge,即顶点ID对应的值以iter为准。
   * 2.虽然mask是最新的,但是newValues是包含所有数据的,即保持了this表的所有数据,只是join上的数据的value使用iter最新的数据
   */
  def innerJoinKeepLeft(iter: Iterator[Product2[VertexId, VD]]): Self[VD] = {
    val newMask = new BitSet(self.capacity)
    val newValues = new Array[VD](self.capacity)
    System.arraycopy(self.values, 0, newValues, 0, newValues.length) //将value值进行复制
    iter.foreach { pair =>
      val pos = self.index.getPos(pair._1) //找到key对应的位置,即key必须存在this的顶点ID中
      if (pos >= 0) {
        newMask.set(pos) //新的key位置
        newValues(pos) = pair._2 //新的value值
      }
    }
    this.withValues(newValues).withMask(newMask)
  }

  //只查询iter中顶点在this顶点集合中的数据,将数据进行merge合并
  def aggregateUsingIndex[VD2: ClassTag](
      iter: Iterator[Product2[VertexId, VD2]],
      reduceFunc: (VD2, VD2) => VD2): Self[VD2] = {//当iter中同一个顶点多次出现的时候,如何合并该顶点的属性,即两个属性值 合并成新的属性值的过程
    val newMask = new BitSet(self.capacity)
    val newValues = new Array[VD2](self.capacity)
    iter.foreach { product =>
      val vid = product._1
      val vdata = product._2
      val pos = self.index.getPos(vid)
      if (pos >= 0) {//说明this中有该顶点
        if (newMask.get(pos)) {//说明iter中不是第一次出现该顶点
          newValues(pos) = reduceFunc(newValues(pos), vdata) //合并值
        } else { // otherwise just store the new value //说明iter中第一次出现该顶点
          newMask.set(pos)
          newValues(pos) = vdata //设置新值
        }
      }
    }
    this.withValues(newValues).withMask(newMask)
  }

  /**
   * Construct a new VertexPartition whose index contains only the vertices in the mask.
   * 针对当前的mask,将value和key中无用的数据删除掉
   */
  def reindex(): Self[VD] = {
    val hashMap = new GraphXPrimitiveKeyOpenHashMap[VertexId, VD]
    val arbitraryMerge = (a: VD, b: VD) => a //如果顶点重复出现的时候,如何合并两个顶点属性---我觉得不会出现这种可能性?
    for ((k, v) <- self.iterator) { //循环每一个顶点以及顶点属性
      hashMap.setMerge(k, v, arbitraryMerge)
    }
    this.withIndex(hashMap.keySet).withValues(hashMap._values).withMask(hashMap.keySet.getBitSet)
  }

  /**
   * Converts a vertex partition (in particular, one of type `Self`) into a
   * `VertexPartitionBaseOps`. Within this class, this allows chaining the methods defined above,
   * because these methods return a `Self` and this implicit conversion re-wraps that in a
   * `VertexPartitionBaseOps`. This relies on the context bound on `Self`.
   */
  private implicit def toOps[VD2: ClassTag](partition: Self[VD2])
    : VertexPartitionBaseOps[VD2, Self] = {
    implicitly[VertexPartitionBaseOpsConstructor[Self]].toOps(partition)
  }
}
