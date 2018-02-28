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

import scala.reflect.ClassTag

import org.apache.spark.util.collection.{BitSet, PrimitiveVector}

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap

/** Stores vertex attributes to ship to an edge partition.
  * 参数存储的是顶点ID和属性集合,分别在两个数组中存储
  **/
private[graphx]
class VertexAttributeBlock[VD: ClassTag](val vids: Array[VertexId], val attrs: Array[VD])
  extends Serializable {
  //该方法是从两个数组中提取出一个元组,表示顶点ID和顶点属性
  def iterator: Iterator[(VertexId, VD)] =
    (0 until vids.size).iterator.map { i => (vids(i), attrs(i)) }
}

//表示顶点RDD的某一个分区
//可以将该RDD分区内的所有顶点按照边需要的顶点进行分组
private[graphx]
object ShippableVertexPartition {
  /** Construct a `ShippableVertexPartition` from the given vertices without any routing table. */
  def apply[VD: ClassTag](iter: Iterator[(VertexId, VD)]): ShippableVertexPartition[VD] =
    apply(iter, RoutingTablePartition.empty, null.asInstanceOf[VD], (a, b) => a)

  /**
   * Construct a `ShippableVertexPartition` from the given vertices with the specified routing
   * table, filling in missing vertices mentioned in the routing table using `defaultVal`.
   */
  def apply[VD: ClassTag](
      iter: Iterator[(VertexId, VD)], routingTable: RoutingTablePartition, defaultVal: VD)
    : ShippableVertexPartition[VD] =
    apply(iter, routingTable, defaultVal, (a, b) => a)

  /**
   * Construct a `ShippableVertexPartition` from the given vertices with the specified routing
   * table, filling in missing vertices mentioned in the routing table using `defaultVal`,
   * and merging duplicate vertex atrribute with mergeFunc.
   */
  def apply[VD: ClassTag](
      iter: Iterator[(VertexId, VD)],//顶点集合
      routingTable: RoutingTablePartition,//顶点的路由对象
      defaultVal: VD,//顶点属性的默认值
      mergeFunc: (VD, VD) => VD) //相同顶点ID的属性merge函数
     : ShippableVertexPartition[VD] = {
    val map = new GraphXPrimitiveKeyOpenHashMap[VertexId, VD]
    // Merge the given vertices using mergeFunc
    //循环所有的顶点信息,对相同顶点ID的属性值进行merge,产生新的属性值
    iter.foreach { pair =>
      map.setMerge(pair._1, pair._2, mergeFunc)
    }
    // Fill in missing vertices mentioned in the routing table 追加iter中缺失的顶点,但是在routingTable路由表中存在的顶点
    routingTable.iterator.foreach { vid =>
      map.changeValue(vid, defaultVal, identity)
    }

    new ShippableVertexPartition(map.keySet, map._values, map.keySet.getBitSet, routingTable)
  }

  import scala.language.implicitConversions

  /**
   * Implicit conversion to allow invoking `VertexPartitionBase` operations directly on a
   * `ShippableVertexPartition`.
   */
  implicit def shippablePartitionToOps[VD: ClassTag](partition: ShippableVertexPartition[VD])
    : ShippableVertexPartitionOps[VD] = new ShippableVertexPartitionOps(partition)

  /**
   * Implicit evidence that `ShippableVertexPartition` is a member of the
   * `VertexPartitionBaseOpsConstructor` typeclass. This enables invoking `VertexPartitionBase`
   * operations on a `ShippableVertexPartition` via an evidence parameter, as in
   * [[VertexPartitionBaseOps]].
   */
  implicit object ShippableVertexPartitionOpsConstructor
    extends VertexPartitionBaseOpsConstructor[ShippableVertexPartition] {
    def toOps[VD: ClassTag](partition: ShippableVertexPartition[VD])
      : VertexPartitionBaseOps[VD, ShippableVertexPartition] = shippablePartitionToOps(partition)
  }
}

/**
 * A map from vertex id to vertex attribute that additionally stores edge partition join sites for
 * each vertex attribute, enabling joining with an [[org.apache.spark.graphx.EdgeRDD]].
 */
private[graphx]
class ShippableVertexPartition[VD: ClassTag](
    val index: VertexIdToIndexMap,
    val values: Array[VD],
    val mask: BitSet,
    val routingTable: RoutingTablePartition)
  extends VertexPartitionBase[VD] {

  /** Return a new ShippableVertexPartition with the specified routing table.
    * 重新设置路由表
    **/
  def withRoutingTable(routingTable_ : RoutingTablePartition): ShippableVertexPartition[VD] = {
    new ShippableVertexPartition(index, values, mask, routingTable_)
  }

  /**
   * Generate a `VertexAttributeBlock` for each edge partition keyed on the edge partition ID. The
   * `VertexAttributeBlock` contains the vertex attributes from the current partition that are
   * referenced in the specified positions in the edge partition.
   * 获取每一个边分区,有哪些顶点ID以及属性集合
   */
  def shipVertexAttributes(
      shipSrc: Boolean, shipDst: Boolean): Iterator[(PartitionID, VertexAttributeBlock[VD])] = { //每一个分区对应一组顶点集合
    //先产生边分区个size的迭代器,然后循环每一个迭代器,将自己分区的顶点放在对应的边分区中
    Iterator.tabulate(routingTable.numEdgePartitions) { pid => //循环每一个分区
      val initialSize = if (shipSrc && shipDst) routingTable.partitionSize(pid) else 64 //初始化该分区有多少个顶点
      val vids = new PrimitiveVector[VertexId](initialSize)
      val attrs = new PrimitiveVector[VD](initialSize)
      var i = 0
      //循环该分区的所有顶点
      routingTable.foreachWithinEdgePartition(pid, shipSrc, shipDst) { vid =>
        if (isDefined(vid)) {
          vids += vid
          attrs += this(vid) //调用VertexPartitionBase的apply方法,获取该顶点的属性值
        }
        i += 1
      }
      (pid, new VertexAttributeBlock(vids.trim().array, attrs.trim().array))
    }
  }

  /**
   * Generate a `VertexId` array for each edge partition keyed on the edge partition ID. The array
   * contains the visible vertex ids from the current partition that are referenced in the edge
   * partition.
   * 获取每一个边分区,有哪些顶点ID
   */
  def shipVertexIds(): Iterator[(PartitionID, Array[VertexId])] = {
    //先产生边分区个size的迭代器,然后循环每一个迭代器,将自己分区的顶点放在对应的边分区中
    Iterator.tabulate(routingTable.numEdgePartitions) { pid => //循环每一个分区
      val vids = new PrimitiveVector[VertexId](routingTable.partitionSize(pid)) //分区内所有顶点集合
      var i = 0
      routingTable.foreachWithinEdgePartition(pid, true, true) { vid => //循环所有顶点
        if (isDefined(vid)) {
          vids += vid
        }
        i += 1
      }
      (pid, vids.trim().array)
    }
  }
}

private[graphx] class ShippableVertexPartitionOps[VD: ClassTag](self: ShippableVertexPartition[VD])
  extends VertexPartitionBaseOps[VD, ShippableVertexPartition](self) {

  def withIndex(index: VertexIdToIndexMap): ShippableVertexPartition[VD] = {
    new ShippableVertexPartition(index, self.values, self.mask, self.routingTable)
  }

  def withValues[VD2: ClassTag](values: Array[VD2]): ShippableVertexPartition[VD2] = {
    new ShippableVertexPartition(self.index, values, self.mask, self.routingTable)
  }

  def withMask(mask: BitSet): ShippableVertexPartition[VD] = {
    new ShippableVertexPartition(self.index, self.values, mask, self.routingTable)
  }
}
