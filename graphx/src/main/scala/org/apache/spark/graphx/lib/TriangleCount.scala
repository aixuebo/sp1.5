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

package org.apache.spark.graphx.lib

import scala.reflect.ClassTag

import org.apache.spark.graphx._

/**
 * Compute the number of triangles passing through each vertex.
 * 计算三角形数量
 * The algorithm is relatively straightforward and can be computed in three steps:
 *
 * <ul>
 * <li>Compute the set of neighbors for each vertex
 * <li>For each edge compute the intersection of the sets and send the count to both vertices.
 * <li> Compute the sum at each vertex and divide by two since each triangle is counted twice.
 * </ul>
 *
 * Note that the input graph should have its edges in canonical direction
 * (i.e. the `sourceId` less than `destId`). Also the graph must have been partitioned
 * using [[org.apache.spark.graphx.Graph#partitionBy]].
 */
object TriangleCount {

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[Int, ED] = {
    // Remove redundant edges移除平行边
    val g = graph.groupEdges((a, b) => a).cache()

    // Construct set representations of the neighborhoods
    // g.collectNeighborIds(EdgeDirection.Either)收集每一个顶点的邻居顶点集合--对边两个顶点都要计算邻居
    val nbrSets: VertexRDD[VertexSet] =
      g.collectNeighborIds(EdgeDirection.Either).mapValues { (vid, nbrs) => //循环每一个顶点以及邻居顶点,修改顶点的属性作为返回值OpenHashSet[VertexId]
        val set = new VertexSet(4) //返回邻居节点集合
        var i = 0
        while (i < nbrs.size) {//循环每一个邻居顶点
          // prevent self cycle
          if (nbrs(i) != vid) {//防止自循环图,过滤掉自己
            set.add(nbrs(i))
          }
          i += 1
        }
        set
      }
    // join the sets with the graph 修改顶点属性值--修改后表示每一个顶点的属性值是邻居顶点集合
    val setGraph: Graph[VertexSet, ED] = g.outerJoinVertices(nbrSets) {
      (vid, _, optSet) => optSet.getOrElse(null) //对相同顶点的属性值进行join,merge函数只要other节点属性值了
    }
    // Edge function computes intersection of smaller vertex with larger vertex
    //循环每一条边
    def edgeFunc(ctx: EdgeContext[VertexSet, ED, Int]) {//顶点的属性最终值是int
      assert(ctx.srcAttr != null)
      assert(ctx.dstAttr != null)
      val (smallSet, largeSet) = if (ctx.srcAttr.size < ctx.dstAttr.size) {//看边的两个顶点谁的相邻顶点少
        (ctx.srcAttr, ctx.dstAttr)
      } else {
        (ctx.dstAttr, ctx.srcAttr)
      }
      val iter = smallSet.iterator
      var counter: Int = 0 //三角形数量
      while (iter.hasNext) {//循环相邻的顶点
        val vid = iter.next()
        //该相邻的顶点不是边的两个顶点,并且在大的顶点相邻顶点集合中存在,说明该顶点vid与边可以成为三角形
        if (vid != ctx.srcId && vid != ctx.dstId && largeSet.contains(vid)) {
          counter += 1
        }
      }
      //将数量都发送给两个顶点
      ctx.sendToSrc(counter)
      ctx.sendToDst(counter)
    }
    // compute the intersection along edges
    val counters: VertexRDD[Int] = setGraph.aggregateMessages(edgeFunc, _ + _) //统计每一个顶点,返回值是int数量,表示每一个顶点有多少个三角形
    // Merge counters with the graph and divide by two since each triangle is counted twice
    g.outerJoinVertices(counters) {//再次做join,修改顶点属性值为顶点三角形数量
      (vid, _, optCounter: Option[Int]) =>
        val dblCount = optCounter.getOrElse(0)
        // double count should be even (divisible by two)
        assert((dblCount & 1) == 0) //断言每一个顶点的三角形数量一定是2的倍数
        //此时/2的目的是 a b c 是一个三角形,a b是一个边,在计算过程中 a三角形数量+1,b也+1
        //a c也是一条边,a+1 c+1
        //b c也是一条边,b+1 c+1
        //因此a=b=c=2  但是他们只是表示一个三角形
        dblCount / 2
    }
  } // end of TriangleCount
}
