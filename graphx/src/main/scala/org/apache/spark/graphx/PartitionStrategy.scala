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

package org.apache.spark.graphx

/**
 * Represents the way edges are assigned to edge partitions based on their source and destination
 * vertex IDs.
 */
trait PartitionStrategy extends Serializable {
  /** Returns the partition number for a given edge.
    * 返回给定边对应的分区,即该边属于哪个分区
    **/
  def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID
}

/**
 * Collection of built-in [[PartitionStrategy]] implementations.
 */
object PartitionStrategy {
  /**
   * Assigns edges to partitions using a 2D partitioning of the sparse edge adjacency matrix,
   * guaranteeing a `2 * sqrt(numParts)` bound on vertex replication.
   *
   * Suppose we have a graph with 12 vertices that we want to partition
   * over 9 machines.  We can use the following sparse matrix representation:
   * 假设我们有12个顶点的图,我们想拆分到9个节点上计算
   * <pre>
   *       __________________________________
   *  v0   | P0 *     | P1       | P2    *  |
   *  v1   |  ****    |  *       |          |
   *  v2   |  ******* |      **  |  ****    |
   *  v3   |  *****   |  *  *    |       *  |
   *       ----------------------------------
   *  v4   | P3 *     | P4 ***   | P5 **  * |
   *  v5   |  *  *    |  *       |          |
   *  v6   |       *  |      **  |  ****    |
   *  v7   |  * * *   |  *  *    |       *  |
   *       ----------------------------------
   *  v8   | P6   *   | P7    *  | P8  *   *|
   *  v9   |     *    |  *    *  |          |
   *  v10  |       *  |      **  |  *  *    |
   *  v11  | * <-E    |  ***     |       ** |
   *       ----------------------------------
   * </pre>
   *
   * The edge denoted by `E` connects `v11` with `v1` and is assigned to processor `P6`.
   * 我们通过E代表一个边,比如v11与v1连接,他就被分配给第6个分区里面
   * To get the processor number we divide the matrix into `sqrt(numParts)` by `sqrt(numParts)` blocks.
   * 这样就让我们将顶点矩阵给拆分,按照分区数量拆分,比如9个分区,则变成3*3数据块,
   * Notice that edges adjacent to `v11` can only be in the first column of blocks `(P0, P3,P6)` or the last
   * row of blocks `(P6, P7, P8)`.  As a consequence we can guarantee that `v11` will need to be
   * replicated to at most `2 * sqrt(numParts)` machines.
   * 注意:相邻的边
   *
   * Notice that `P0` has many edges and as a consequence this partitioning would lead to poor work
   * balance.  To improve balance we first multiply each vertex id by a large prime to shuffle the
   * vertex locations.
   *
   * When the number of partitions requested is not a perfect square we use a slightly different
   * method where the last column can have a different number of rows than the others while still
   * maintaining the same size per block.
   */
  case object EdgePartition2D extends PartitionStrategy {
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      val ceilSqrtNumParts: PartitionID = math.ceil(math.sqrt(numParts)).toInt //分区数量开方
      val mixingPrime: VertexId = 1125899906842597L
      if (numParts == ceilSqrtNumParts * ceilSqrtNumParts) {//真实的分区数量正好是可以开方的
        // Use old method for perfect squared to ensure we get same results
        //计算src和dst是分区后的第几行,第几列分区
        val col: PartitionID = (math.abs(src * mixingPrime) % ceilSqrtNumParts).toInt
        val row: PartitionID = (math.abs(dst * mixingPrime) % ceilSqrtNumParts).toInt
        //后来观察了一下,因为有mixingPrime.所以还是需要%numParts的
        (col * ceilSqrtNumParts + row) % numParts //理论上一定不需要%numParts----这个结果就说明该节点存储在几行*一行的列数+第几列,就是最终的分区数
      } else {
        // Otherwise use new method
        val cols = ceilSqrtNumParts //一行的列数先规定好
        val rows = (numParts + cols - 1) / cols //一共numParts个分区能产生多少行---即 (numParts + (cols - 1)) / cols
        val lastColRows = numParts - rows * (cols - 1) //最后一行有多少个列是有用的
        val col = (math.abs(src * mixingPrime) % numParts / rows).toInt
        val row = (math.abs(dst * mixingPrime) % (if (col < cols - 1) rows else lastColRows)).toInt
        col * rows + row
      }
    }
  }

  /**
   * Assigns edges to partitions using only the source vertex ID, colocating edges with the same
   * source.
   * 按照src顶点进行分区,让同一个顶点作为src的都在一个分区里面
   */
  case object EdgePartition1D extends PartitionStrategy {
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      val mixingPrime: VertexId = 1125899906842597L
      (math.abs(src * mixingPrime) % numParts).toInt
    }
  }


  /**
   * Assigns edges to partitions by hashing the source and destination vertex IDs, resulting in a
   * random vertex cut that colocates all same-direction edges between two vertices.
   * 随机策略很容易理解,就是对元组进行hash一下,然后随机分配
   */
  case object RandomVertexCut extends PartitionStrategy {
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      math.abs((src, dst).hashCode()) % numParts
    }
  }


  /**
   * Assigns edges to partitions by hashing the source and destination vertex IDs in a canonical
   * direction, resulting in a random vertex cut that colocates all edges between two vertices,
   * regardless of direction.
   * 将src和dst先排序,按照<小,大>的顺序排序好后,在hash,然后再随机排序
   * 这样可以保证src和dst相同的两个有向图可以在一个分区里面
   */
  case object CanonicalRandomVertexCut extends PartitionStrategy {
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      if (src < dst) {
        math.abs((src, dst).hashCode()) % numParts
      } else {
        math.abs((dst, src).hashCode()) % numParts
      }
    }
  }

  /** Returns the PartitionStrategy with the specified name.
    * 各种策略的对应关系
    **/
  def fromString(s: String): PartitionStrategy = s match {
    case "RandomVertexCut" => RandomVertexCut
    case "EdgePartition1D" => EdgePartition1D
    case "EdgePartition2D" => EdgePartition2D
    case "CanonicalRandomVertexCut" => CanonicalRandomVertexCut
    case _ => throw new IllegalArgumentException("Invalid PartitionStrategy: " + s)
  }
}
