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

import scala.reflect.{classTag, ClassTag}

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.apache.spark.graphx._

/**
 * Manages shipping vertex attributes to the edge partitions of an
 * [[org.apache.spark.graphx.EdgeRDD]]. Vertex attributes may be partially shipped to construct a
 * triplet view with vertex attributes on only one side, and they may be updated. An active vertex
 * set may additionally be shipped to the edge partitions. Be careful not to store a reference to
 * `edges`, since it may be modified when the attribute shipping level is upgraded.
 * 对边RDD进行包租航
 */
private[impl]
class ReplicatedVertexView[VD: ClassTag, ED: ClassTag](
    var edges: EdgeRDDImpl[ED, VD],
    var hasSrcId: Boolean = false,//true表示边RDD的src顶点元素已经被装载过了
    var hasDstId: Boolean = false) {

  /**
   * Return a new `ReplicatedVertexView` with the specified `EdgeRDD`, which must have the same
   * shipping level.
   */
  def withEdges[VD2: ClassTag, ED2: ClassTag](
      edges_ : EdgeRDDImpl[ED2, VD2]): ReplicatedVertexView[VD2, ED2] = {
    new ReplicatedVertexView(edges_, hasSrcId, hasDstId)
  }

  /**
   * Return a new `ReplicatedVertexView` where edges are reversed and shipping levels are swapped to
   * match.
   */
  def reverse(): ReplicatedVertexView[VD, ED] = {
    val newEdges = edges.mapEdgePartitions((pid, part) => part.reverse)
    new ReplicatedVertexView(newEdges, hasDstId, hasSrcId)
  }

  /**
   * Upgrade the shipping level in-place to the specified levels by shipping vertex attributes from
   * `vertices`. This operation modifies the `ReplicatedVertexView`, and callers can access `edges`
   * afterwards to obtain the upgraded view.
   * 要求对边RDD中的边顶点装载一些顶点属性
   * VertexRDD包含了所有的顶点属性集合,那么边装载的是src还是dst呢?取决于后面两个参数
   */
  def upgrade(vertices: VertexRDD[VD], includeSrc: Boolean, includeDst: Boolean) {
    val shipSrc = includeSrc && !hasSrcId //includeSrc 说明要装载src,hasSrcId=false,说明src没有装载过,则需要装载
    val shipDst = includeDst && !hasDstId
    if (shipSrc || shipDst) {//说明有任意一个没有被装载过,都要去装载
      val shippedVerts: RDD[(Int, VertexAttributeBlock[VD])] =
        vertices.shipVertexAttributes(shipSrc, shipDst) //先将顶点RDD按照边进行分组,即同一个顶点可能分布在不同的边里面,因此返回RDD[(PartitionID, VertexAttributeBlock[VD])]
          .setName("ReplicatedVertexView.upgrade(%s, %s) - shippedVerts %s %s (broadcast)".format(
            includeSrc, includeDst, shipSrc, shipDst))
          .partitionBy(edges.partitioner.get) //按照边的partition进行shuffle,保证每一个边需要的顶点都在边的分区中存在
      val newEdges = edges.withPartitionsRDD(edges.partitionsRDD.zipPartitions(shippedVerts) {//这样就可以装载顶点数据了
        (ePartIter, shippedVertsIter) => ePartIter.map { //参数ePartIter表示边的rdd分区,shippedVertsIter表示顶点的rdd分区
          case (pid, edgePartition) =>
            (pid, edgePartition.updateVertices(shippedVertsIter.flatMap(_._2.iterator))) //shippedVertsIters是RDD[(PartitionID, VertexAttributeBlock[VD])]结构
        }
      })
      edges = newEdges
      hasSrcId = includeSrc
      hasDstId = includeDst
    }
  }

  /**
   * Return a new `ReplicatedVertexView` where the `activeSet` in each edge partition contains only
   * vertex ids present in `actives`. This ships a vertex id to all edge partitions where it is
   * referenced, ignoring the attribute shipping level.
   */
  def withActiveSet(actives: VertexRDD[_]): ReplicatedVertexView[VD, ED] = {
    val shippedActives = actives.shipVertexIds()
      .setName("ReplicatedVertexView.withActiveSet - shippedActives (broadcast)")
      .partitionBy(edges.partitioner.get)

    val newEdges = edges.withPartitionsRDD(edges.partitionsRDD.zipPartitions(shippedActives) {
      (ePartIter, shippedActivesIter) => ePartIter.map {
        case (pid, edgePartition) =>
          (pid, edgePartition.withActiveSet(shippedActivesIter.flatMap(_._2.iterator)))
      }
    })
    new ReplicatedVertexView(newEdges, hasSrcId, hasDstId)
  }

  /**
   * Return a new `ReplicatedVertexView` where vertex attributes in edge partition are updated using
   * `updates`. This ships a vertex attribute only to the edge partitions where it is in the
   * position(s) specified by the attribute shipping level.
   */
  def updateVertices(updates: VertexRDD[VD]): ReplicatedVertexView[VD, ED] = {
    val shippedVerts = updates.shipVertexAttributes(hasSrcId, hasDstId)
      .setName("ReplicatedVertexView.updateVertices - shippedVerts %s %s (broadcast)".format(
        hasSrcId, hasDstId))
      .partitionBy(edges.partitioner.get)

    val newEdges = edges.withPartitionsRDD(edges.partitionsRDD.zipPartitions(shippedVerts) {
      (ePartIter, shippedVertsIter) => ePartIter.map {
        case (pid, edgePartition) =>
          (pid, edgePartition.updateVertices(shippedVertsIter.flatMap(_._2.iterator)))
      }
    })
    new ReplicatedVertexView(newEdges, hasSrcId, hasDstId)
  }
}
