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

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.util.collection.{BitSet, PrimitiveVector}

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap

import org.apache.spark.graphx.impl.RoutingTablePartition.RoutingTableMessage

private[graphx]
object RoutingTablePartition {
  /**
   * A message from an edge partition to a vertex specifying the position in which the edge
   * partition references the vertex (src, dst, or both). The edge partition is encoded in the lower
   * 30 bytes of the Int, and the position is encoded in the upper 2 bytes of the Int.
   * 用于表示每一个顶点在哪个分区
   */
  type RoutingTableMessage = (VertexId, Int) //即每一个顶点ID,pisition | 分区ID 组成的int

  //position参数的含义 第一个bit是1,表示该顶点是边的src;第二个bit是1,表示该顶点是边的dst
  private def toMessage(vid: VertexId, pid: PartitionID, position: Byte): RoutingTableMessage = {
    val positionUpper2 = position << 30
    val pidLower30 = pid & 0x3FFFFFFF //30个1
    (vid, positionUpper2 | pidLower30) //即每一个顶点ID,pisition | 分区ID 组成的int
  }

  //信息还原
  private def vidFromMessage(msg: RoutingTableMessage): VertexId = msg._1
  private def pidFromMessage(msg: RoutingTableMessage): PartitionID = msg._2 & 0x3FFFFFFF
  private def positionFromMessage(msg: RoutingTableMessage): Byte = (msg._2 >> 30).toByte

  val empty: RoutingTablePartition = new RoutingTablePartition(Array.empty)

  /** Generate a `RoutingTableMessage` for each vertex referenced in `edgePartition`.
    * 循环一个分区的所有的边
    **/
  def edgePartitionToMsgs(pid: PartitionID, edgePartition: EdgePartition[_, _])
    : Iterator[RoutingTableMessage] = {
    // Determine which positions each vertex id appears in using a map where the low 2 bits
    // represent src and dst
    val map = new GraphXPrimitiveKeyOpenHashMap[VertexId, Byte] //将该分区的所有顶点,标识成该顶点是src还是dst还是都有参与
    edgePartition.iterator.foreach { e =>
      map.changeValue(e.srcId, 0x1, (b: Byte) => (b | 0x1).toByte)
      map.changeValue(e.dstId, 0x2, (b: Byte) => (b | 0x2).toByte)
    }
    map.iterator.map { vidAndPosition =>
      val vid = vidAndPosition._1
      val position = vidAndPosition._2
      toMessage(vid, pid, position) //产生最终的信息,即每一个顶点对应一个RoutingTableMessage对象
    }
  }

  /** Build a `RoutingTablePartition` from `RoutingTableMessage`s.
    * numEdgePartitions表示分区数量
    * 该参数iter 表示某一个分区,所有的分区内能保证一个顶点ID只能在某一个分区内出现
    * 比如有两个分区情况,具体参见VertexRDD代码备注
    * 1.第一个分区内有a和b两个顶点
    * <a,1,有src存在>
    * <b,1,有src存在>
    * <a,2,有src存在>
    * 2.第二个分区内有c和e两个顶点
    * <c,1,有src存在>
    * <c,2,有src存在>
    * <e,2,有src存在>
    * 最终结果
    * pid2vid
    * [
    *   [a,b],
    *   [a]
    * ]
    *
    **/
  def fromMsgs(numEdgePartitions: Int, iter: Iterator[RoutingTableMessage])
    : RoutingTablePartition = {
    val pid2vid = Array.fill(numEdgePartitions)(new PrimitiveVector[VertexId]) //有多少个分区,就有多少个数组,每一个分区对应一个PrimitiveVector集合,存储该分区的顶点集合
    val srcFlags = Array.fill(numEdgePartitions)(new PrimitiveVector[Boolean]) //每一个顶点是否是src顶点
    val dstFlags = Array.fill(numEdgePartitions)(new PrimitiveVector[Boolean]) //每一个顶点是否是dst顶点
    for (msg <- iter) {
      val vid = vidFromMessage(msg) //顶点ID
      val pid = pidFromMessage(msg) //分区ID
      val position = positionFromMessage(msg)
      pid2vid(pid) += vid //存储该分区的顶点ID
      srcFlags(pid) += (position & 0x1) != 0 //第一个bit是1,表示该顶点是边的src
      dstFlags(pid) += (position & 0x2) != 0 //第二个bit是1,表示该顶点是边的dst
    }
    new RoutingTablePartition(pid2vid.zipWithIndex.map {
      case (vids, pid) => (vids.trim().array, toBitSet(srcFlags(pid)), toBitSet(dstFlags(pid)))
    })
  }

  /** Compact the given vector of Booleans into a BitSet.
    * 将boolean向量转换成BitSet对象,属于数据压缩范畴
    **/
  private def toBitSet(flags: PrimitiveVector[Boolean]): BitSet = {
    val bitset = new BitSet(flags.size)
    var i = 0
    while (i < flags.size) {
      if (flags(i)) {
        bitset.set(i)
      }
      i += 1
    }
    bitset
  }
}

/**
 * Stores the locations of edge-partition join sites for each vertex attribute in a particular
 * vertex partition. This provides routing information for shipping vertex attributes to edge
 * partitions.
 * 表示顶点的某一个分区
 */
private[graphx]
class RoutingTablePartition(
   //参数数组Array,有多少个分区,就有多少个元素.---表示的是该顶点分区内的顶点在每一个边分区的分布情况
   //每一个元素表示一个分区内的内容,包含该分区包含的顶点集合,以及每一个顶点是否是src和dst的bitset对象
    private val routingTable: Array[(Array[VertexId], BitSet, BitSet)]) extends Serializable {
  /** The maximum number of edge partitions this `RoutingTablePartition` is built to join with. */
  val numEdgePartitions: Int = routingTable.size //表示有多少个分区

  /** Returns the number of vertices that will be sent to the specified edge partition. */
  def partitionSize(pid: PartitionID): Int = routingTable(pid)._1.size //分区顶点集合中,在边的pid分区内有多少顶点存在

  /** Returns an iterator over all vertex ids stored in this `RoutingTablePartition`. */
  def iterator: Iterator[VertexId] = routingTable.iterator.flatMap(_._1.iterator) //循环每一个分区,然后在循环该分区下每一个顶点,即这样的迭代器如果一个顶点在多个分区内,就会产生多次

  /** Returns a new RoutingTablePartition reflecting a reversal of all edge directions. */
  def reverse: RoutingTablePartition = {
    new RoutingTablePartition(routingTable.map { //将dst和src映射进行反转
      case (vids, srcVids, dstVids) => (vids, dstVids, srcVids)
    })
  }

  /**
   * Runs `f` on each vertex id to be sent to the specified edge partition. Vertex ids can be
   * filtered by the position they have in the edge partition.
   */
  def foreachWithinEdgePartition
      (pid: PartitionID, includeSrc: Boolean, includeDst: Boolean) //对某一个分区进行迭代,找到符合条件的顶点进行迭代,并且计算函数f
      (f: VertexId => Unit) { //对找到的顶点进行操作,参数是顶点ID,返回值是void
    val (vidsCandidate, srcVids, dstVids) = routingTable(pid) //找到该边分区的需要的顶点数据集合
    val size = vidsCandidate.length //该分区有多少个顶点
    if (includeSrc && includeDst) {//循环所有顶点
      // Avoid checks for performance
      vidsCandidate.iterator.foreach(f)
    } else if (!includeSrc && !includeDst) {
      // Do nothing
    } else { //只循环src或者dst的顶点
      val relevantVids = if (includeSrc) srcVids else dstVids
      relevantVids.iterator.foreach { i => f(vidsCandidate(i)) }
    }
  }
}
