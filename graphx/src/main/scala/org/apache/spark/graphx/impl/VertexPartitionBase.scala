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
import scala.reflect.ClassTag

import org.apache.spark.util.collection.BitSet

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap

private[graphx] object VertexPartitionBase {
  /**
   * Construct the constituents of a VertexPartitionBase from the given vertices, merging duplicate
   * entries arbitrarily.
   */
  def initFrom[VD: ClassTag](iter: Iterator[(VertexId, VD)]) //参数是顶点集合的迭代器
    : (VertexIdToIndexMap, Array[VD], BitSet) = { //返回值顶点ID集合、每一个顶点对应的属性值数组集合、BitSet可以获取key是否存在
    val map = new GraphXPrimitiveKeyOpenHashMap[VertexId, VD]
    iter.foreach { pair => //让数据转换成map数据,key是顶点ID,value是顶点的属性对象
      map(pair._1) = pair._2
    }
    (map.keySet, map._values, map.keySet.getBitSet)
  }

  /**
   * Construct the constituents of a VertexPartitionBase from the given vertices, merging duplicate
   * entries using `mergeFunc`.
   */
  def initFrom[VD: ClassTag](iter: Iterator[(VertexId, VD)], mergeFunc: (VD, VD) => VD)
    : (VertexIdToIndexMap, Array[VD], BitSet) = {
    val map = new GraphXPrimitiveKeyOpenHashMap[VertexId, VD]
    iter.foreach { pair =>
      map.setMerge(pair._1, pair._2, mergeFunc) //对于key相同出现的,要进行merge合并,即两个重复出现的属性进行运算,产生一个新的属性
    }
    (map.keySet, map._values, map.keySet.getBitSet)
  }
}

/**
 * An abstract map from vertex id to vertex attribute. [[VertexPartition]] is the corresponding
 * concrete implementation. [[VertexPartitionBaseOps]] provides a variety of operations for
 * VertexPartitionBase and subclasses that provide implicit evidence of membership in the
 * `VertexPartitionBaseOpsConstructor` typeclass (for example,
 * [[VertexPartition.VertexPartitionOpsConstructor]]).
 */
private[graphx] abstract class VertexPartitionBase[@specialized(Long, Int, Double) VD: ClassTag]
  extends Serializable {

  def index: VertexIdToIndexMap //顶点与属性值位置的映射
  def values: Array[VD] //顶点的属性值集合
  def mask: BitSet

  val capacity: Int = index.capacity

  def size: Int = mask.cardinality()

  /** Return the vertex attribute for the given vertex ID.
    * 获取一个顶点的属性对象
    **/
  def apply(vid: VertexId): VD = values(index.getPos(vid)) //通过index.getPos(vid)找到该顶点对应的数组序号,从而找到该顶点对应的属性值

  //表示是否包含该顶点
  def isDefined(vid: VertexId): Boolean = {
    val pos = index.getPos(vid) //获取该顶点对应的序号
    pos >= 0 && mask.get(pos) //true说明该序号是有效的
  }

  //迭代每一个顶点以及顶点属性对象
  /**
   * 不断循环每一个有效的顶点ID
   * 找到该id对应的顶点具体内容,以及该顶点的属性值
   */
  def iterator: Iterator[(VertexId, VD)] =
    mask.iterator.map(ind => (index.getValue(ind), values(ind)))
}

/**
 * A typeclass for subclasses of `VertexPartitionBase` representing the ability to wrap them in a
 * `VertexPartitionBaseOps`.
 */
private[graphx] trait VertexPartitionBaseOpsConstructor[T[X] <: VertexPartitionBase[X]] {
  def toOps[VD: ClassTag](partition: T[VD]): VertexPartitionBaseOps[VD, T]
}
