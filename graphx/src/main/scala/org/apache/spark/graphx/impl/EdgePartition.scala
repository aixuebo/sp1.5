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

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap
import org.apache.spark.util.collection.BitSet

/**
 * A collection of edges, along with referenced vertex attributes and an optional active vertex set
 * for filtering computation on the edges.
 * 表示边的集合--追加上顶点的属性值,用于过滤操作
 *
 * The edges are stored in columnar format in `localSrcIds`, `localDstIds`, and `data`. All
 * referenced global vertex ids are mapped to a compact set of local vertex ids according to the
 * `global2local` map. Each local vertex id is a valid index into `vertexAttrs`, which stores the
 * corresponding vertex attribute, and `local2global`, which stores the reverse mapping to global
 * vertex id. The global vertex ids that are active are optionally stored in `activeSet`.
 *
 * The edges are clustered by source vertex id, and the mapping from global vertex id to the index
 * of the corresponding edge cluster is stored in `index`.
 *
 * @tparam ED the edge attribute type 边属性类型
 * @tparam VD the vertex attribute type 顶点属性类型
 *
 * @param localSrcIds the local source vertex id of each edge as an index into `local2global` and
 *   `vertexAttrs` 每一个边在本地的source顶点ID
 * @param localDstIds the local destination vertex id of each edge as an index into `local2global`
 *   and `vertexAttrs`
 * @param data the attribute associated with each edge
 * @param index a clustered index on source vertex id as a map from each global source vertex id to
 *   the offset in the edge arrays where the cluster for that vertex id begins
 * @param global2local a map from referenced vertex ids to local ids which index into vertexAttrs
 * @param local2global an array of global vertex ids where the offsets are local vertex ids
 * @param vertexAttrs an array of vertex attributes where the offsets are local vertex ids
 * @param activeSet an optional active vertex set for filtering computation on the edges
 * 该对象表示一个分区内的所有边信息
 */
private[graphx]
class EdgePartition[
    @specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED: ClassTag, VD: ClassTag](
    localSrcIds: Array[Int],//存储每一个边的src顶点ID的本地序号
    localDstIds: Array[Int],//存储每一个边的dst顶点ID的本地序号
    data: Array[ED],//每一个边的属性集合
    index: GraphXPrimitiveKeyOpenHashMap[VertexId, Int],//每一个顶点从第几个位置开始切换的
    global2local: GraphXPrimitiveKeyOpenHashMap[VertexId, Int],//顶点与本地序号的映射
    local2global: Array[VertexId],//本地序号与顶点的映射
    vertexAttrs: Array[VD],//顶点的属性集合,序号是本地ID
    activeSet: Option[VertexSet])
  extends Serializable {

  /** No-arg constructor for serialization. */
  private def this() = this(null, null, null, null, null, null, null, null)

  /** Return a new `EdgePartition` with the specified edge data. */
  def withData[ED2: ClassTag](data: Array[ED2]): EdgePartition[ED2, VD] = {
    new EdgePartition(
      localSrcIds, localDstIds, data, index, global2local, local2global, vertexAttrs, activeSet)
  }

  /** Return a new `EdgePartition` with the specified active set, provided as an iterator.
    * 设置需要的顶点集合
    **/
  def withActiveSet(iter: Iterator[VertexId]): EdgePartition[ED, VD] = {
    val activeSet = new VertexSet
    while (iter.hasNext) { activeSet.add(iter.next()) }
    new EdgePartition(
      localSrcIds, localDstIds, data, index, global2local, local2global, vertexAttrs,
      Some(activeSet))
  }

  /** Return a new `EdgePartition` with updates to vertex attributes specified in `iter`.
    * 更新顶点的属性
    * 对参数的顶点进行更新,不再参数中的顶点的属性值保持原样
    **/
  def updateVertices(iter: Iterator[(VertexId, VD)]): EdgePartition[ED, VD] = {
    val newVertexAttrs = new Array[VD](vertexAttrs.length)
    System.arraycopy(vertexAttrs, 0, newVertexAttrs, 0, vertexAttrs.length)
    while (iter.hasNext) {
      val kv = iter.next()
      newVertexAttrs(global2local(kv._1)) = kv._2 //global2local(kv._1)表示通过顶点ID转换成本地ID的过程
    }
    new EdgePartition(
      localSrcIds, localDstIds, data, index, global2local, local2global, newVertexAttrs,
      activeSet)
  }

  /** Return a new `EdgePartition` without any locally cached vertex attributes.
    * 清空顶点属性信息
    **/
  def withoutVertexAttributes[VD2: ClassTag](): EdgePartition[ED, VD2] = {
    val newVertexAttrs = new Array[VD2](vertexAttrs.length)
    new EdgePartition(
      localSrcIds, localDstIds, data, index, global2local, local2global, newVertexAttrs,
      activeSet)
  }

  //获取第pos个边对应的src顶点是什么
  //localSrcIds(pos) 获取该边的src的本地序号 ;local2global获取本地序号对应的真实顶点ID
  @inline private def srcIds(pos: Int): VertexId = local2global(localSrcIds(pos))

  //获取第pos个边对应的dst顶点是什么
  @inline private def dstIds(pos: Int): VertexId = local2global(localDstIds(pos))

  //获取第pos个边对应的边属性
  @inline private def attrs(pos: Int): ED = data(pos)

  /** Look up vid in activeSet, throwing an exception if it is None. */
  def isActive(vid: VertexId): Boolean = {
    activeSet.get.contains(vid)
  }

  /** The number of active vertices, if any exist.
    * 获取activeSet.size值
    **/
  def numActives: Option[Int] = activeSet.map(_.size)

  /**
   * Reverse all the edges in this partition.
   *
   * @return a new edge partition with all edges reversed.
   * 反转,即由a->b转换成b->a的过程
   */
  def reverse: EdgePartition[ED, VD] = {
    val builder = new ExistingEdgePartitionBuilder[ED, VD](
      global2local, local2global, vertexAttrs, activeSet, size)
    var i = 0
    while (i < size) {//循环每一个边
      //获取边的本地序号
      val localSrcId = localSrcIds(i)
      val localDstId = localDstIds(i)
      //获取真实的顶点ID
      val srcId = local2global(localSrcId)
      val dstId = local2global(localDstId)
      val attr = data(i)
      //反转一下
      builder.add(dstId, srcId, localDstId, localSrcId, attr)
      i += 1
    }
    builder.toEdgePartition
  }

  /**
   * Construct a new edge partition by applying the function f to all
   * edges in this partition.
   *
   * Be careful not to keep references to the objects passed to `f`.
   * To improve GC performance the same object is re-used for each call.
   *
   * @param f a function from an edge to a new attribute
   * @tparam ED2 the type of the new attribute
   * @return a new edge partition with the result of the function `f`
   *         applied to each edge
   * 更新边的属性
   */
  def map[ED2: ClassTag](f: Edge[ED] => ED2) //函数是给定每一个边,将其转换新的边,此时能改的就只有边的属性
     : EdgePartition[ED2, VD] = {
    val newData = new Array[ED2](data.size)
    val edge = new Edge[ED]()
    val size = data.size
    var i = 0
    while (i < size) {//循环每一个边
      edge.srcId = srcIds(i)
      edge.dstId = dstIds(i)
      edge.attr = data(i)
      newData(i) = f(edge)
      i += 1
    }
    this.withData(newData)
  }

  /**
   * Construct a new edge partition by using the edge attributes
   * contained in the iterator.
   *
   * @note The input iterator should return edge attributes in the
   * order of the edges returned by `EdgePartition.iterator` and
   * should return attributes equal to the number of edges.
   *
   * @param iter an iterator for the new attribute values 参数是新边的属性集合
   * @tparam ED2 the type of the new attribute
   * @return a new edge partition with the attribute values replaced
   * 注意:要求参数的size数量与原始边的size是相同的,因此可以一对一的进行覆盖
   */
  def map[ED2: ClassTag](iter: Iterator[ED2]): EdgePartition[ED2, VD] = {
    // Faster than iter.toArray, because the expected size is known.
    val newData = new Array[ED2](data.size)
    var i = 0
    while (iter.hasNext) {
      newData(i) = iter.next()
      i += 1
    }
    assert(newData.size == i)
    this.withData(newData)
  }

  /**
   * Construct a new edge partition containing only the edges matching `epred` and where both
   * vertices match `vpred`.
   */
  def filter(
      epred: EdgeTriplet[VD, ED] => Boolean,//如何对边进行过滤
      vpred: (VertexId, VD) => Boolean) //src的顶点ID以及顶点属性进行过滤、dst的顶点ID以及顶点属性进行过滤
      : EdgePartition[ED, VD] = {
    val builder = new ExistingEdgePartitionBuilder[ED, VD](
      global2local, local2global, vertexAttrs, activeSet)
    var i = 0
    while (i < size) {
      // The user sees the EdgeTriplet, so we can't reuse it and must create one per edge.
      //获取每一个边的顶点的本地序号
      val localSrcId = localSrcIds(i)
      val localDstId = localDstIds(i)
      val et = new EdgeTriplet[VD, ED]
      //通过本地序号还原顶点ID
      et.srcId = local2global(localSrcId)
      et.dstId = local2global(localDstId)
      //提取顶点属性
      et.srcAttr = vertexAttrs(localSrcId)
      et.dstAttr = vertexAttrs(localDstId)
      //提取边属性
      et.attr = data(i)
      if (vpred(et.srcId, et.srcAttr) && vpred(et.dstId, et.dstAttr) && epred(et)) { //两者都返回true才允许通过
        builder.add(et.srcId, et.dstId, localSrcId, localDstId, et.attr)
      }
      i += 1
    }
    builder.toEdgePartition
  }

  /**
   * Apply the function f to all edges in this partition.
   *
   * @param f an external state mutating user defined function.
   * 处理每一个边,没有返回值
   */
  def foreach(f: Edge[ED] => Unit) {
    iterator.foreach(f)
  }

  /**
   * Merge all the edges with the same src and dest id into a single
   * edge using the `merge` function
   *
   * @param merge a commutative associative merge operation
   * @return a new edge partition without duplicate edges
   * 因为边的集合已经排序好了,因此group by的过程是很容易的,就是一行一行查找即可
   *
   * 对两个相同的顶点连接的平行边进行处理
   *
   * 对边集合操作,将顶点相同的平行边进行合并操作。产生新的边属性,即可能是更改边的权重值
   */
  def groupEdges(merge: (ED, ED) => ED) //参数函数表示对两个平行边merge出一个新的边
    : EdgePartition[ED, VD] = {
    val builder = new ExistingEdgePartitionBuilder[ED, VD](
      global2local, local2global, vertexAttrs, activeSet)
    var currSrcId: VertexId = null.asInstanceOf[VertexId]//当前边对应的两个顶点ID
    var currDstId: VertexId = null.asInstanceOf[VertexId]
    var currLocalSrcId = -1 ////当前边对应的两个顶点的本地序号
    var currLocalDstId = -1
    var currAttr: ED = null.asInstanceOf[ED] //当前处理的边属性
    // Iterate through the edges, accumulating runs of identical edges using the curr* variables and
    // releasing them to the builder when we see the beginning of the next run
    var i = 0
    while (i < size) {
      if (i > 0 && currSrcId == srcIds(i) && currDstId == dstIds(i)) {//说明当前边与前面的边的两个顶点是相同的,需要merge
        // This edge should be accumulated into the existing run
        currAttr = merge(currAttr, data(i))
      } else {//说明切换了一个新边了
        // This edge starts a new run of edges
        if (i > 0) {
          // First release the existing run to the builder
          builder.add(currSrcId, currDstId, currLocalSrcId, currLocalDstId, currAttr)
        }
        // Then start accumulating for a new run
        //设置当前两个顶点,以及两个顶点对应的本地序号,以及边属性
        currSrcId = srcIds(i)
        currDstId = dstIds(i)
        currLocalSrcId = localSrcIds(i)
        currLocalDstId = localDstIds(i)
        currAttr = data(i)
      }
      i += 1
    }
    // Finally, release the last accumulated run
    if (size > 0) {
      builder.add(currSrcId, currDstId, currLocalSrcId, currLocalDstId, currAttr)
    }
    builder.toEdgePartition
  }

  /**
   * Apply `f` to all edges present in both `this` and `other` and return a new `EdgePartition`
   * containing the resulting edges.
   *
   * If there are multiple edges with the same src and dst in `this`, `f` will be invoked once for
   * each edge, but each time it may be invoked on any corresponding edge in `other`.
   *
   * If there are multiple edges with the same src and dst in `other`, `f` will only be invoked
   * once.
   * 两个分区进行join操作
   */
  def innerJoin[ED2: ClassTag, ED3: ClassTag]
      (other: EdgePartition[ED2, _]) //另外一个分区
      (f: (VertexId, VertexId, ED, ED2) => ED3) //如何merge,参数分别表示两个顶点,以及两个顶点的边属性,相当于对属性进行merge操作
     : EdgePartition[ED3, VD] = {
    val builder = new ExistingEdgePartitionBuilder[ED3, VD](
      global2local, local2global, vertexAttrs, activeSet)
    var i = 0 //表示this的边序号
    var j = 0 //表示other的边序号
    // For i = index of each edge in `this`...
    while (i < size && j < other.size) { //因为是排序好的,所以非常好遍历
      //找到此时this的一个边
      val srcId = this.srcIds(i)
      val dstId = this.dstIds(i)
      // ... forward j to the index of the corresponding edge in `other`, and...
      while (j < other.size && other.srcIds(j) < srcId) { j += 1 }
      if (j < other.size && other.srcIds(j) == srcId) {//说明两个集合中src顶点相同的相遇了
        while (j < other.size && other.srcIds(j) == srcId && other.dstIds(j) < dstId) { j += 1 }
        if (j < other.size && other.srcIds(j) == srcId && other.dstIds(j) == dstId) {//说明dst顶点相同的也相遇了
          // ... run `f` on the matching edge
          builder.add(srcId, dstId, localSrcIds(i), localDstIds(i),
            f(srcId, dstId, this.data(i), other.attrs(j))) //进行merge操作
        }
      }
      i += 1
    }
    builder.toEdgePartition
  }

  /**
   * The number of edges in this partition
   *
   * @return size of the partition
   * 表示该分区内有多少条边
   */
  val size: Int = localSrcIds.size

  /** The number of unique source vertices in the partition.
    * 该分区内有多少条不同的src顶点--即有多少条边存在
    **/
  def indexSize: Int = index.size

  /**
   * Get an iterator over the edges in this partition.
   *
   * Be careful not to keep references to the objects from this iterator.
   * To improve GC performance the same object is re-used in `next()`.
   *
   * @return an iterator over edges in the partition
   * 遍历一个分区内所有的边
   */
  def iterator: Iterator[Edge[ED]] = new Iterator[Edge[ED]] {
    private[this] val edge = new Edge[ED]
    private[this] var pos = 0 //当前遍历了多少条边了

    override def hasNext: Boolean = pos < EdgePartition.this.size //说明该有边存在

    override def next(): Edge[ED] = {
      edge.srcId = srcIds(pos)
      edge.dstId = dstIds(pos)
      edge.attr = data(pos)
      pos += 1
      edge
    }
  }

  /**
   * Get an iterator over the edge triplets in this partition.
   *
   * It is safe to keep references to the objects from this iterator.
   * 提取每一个边的全部信息进行遍历
   */
  def tripletIterator(
      includeSrc: Boolean = true, includeDst: Boolean = true) //参数表示是否要提取顶点的属性
      : Iterator[EdgeTriplet[VD, ED]] = new Iterator[EdgeTriplet[VD, ED]] {
    private[this] var pos = 0 //遍历当前第几条边了

    override def hasNext: Boolean = pos < EdgePartition.this.size //是否还有边尚未被遍历到

    override def next(): EdgeTriplet[VD, ED] = {
      val triplet = new EdgeTriplet[VD, ED]
      //获取第几条边的src和dst的本地序号
      val localSrcId = localSrcIds(pos)
      val localDstId = localDstIds(pos)
      //获取真实的顶点ID
      triplet.srcId = local2global(localSrcId)
      triplet.dstId = local2global(localDstId)
      //设置src和dst顶点的属性
      if (includeSrc) {
        triplet.srcAttr = vertexAttrs(localSrcId)
      }
      if (includeDst) {
        triplet.dstAttr = vertexAttrs(localDstId)
      }
      triplet.attr = data(pos) //获取边属性
      pos += 1
      triplet
    }
  }

  /**
   * Send messages along edges and aggregate them at the receiving vertices. Implemented by scanning
   * all edges sequentially.
   *
   * @param sendMsg generates messages to neighboring vertices of an edge
   * @param mergeMsg the combiner applied to messages destined to the same vertex
   * @param tripletFields which triplet fields `sendMsg` uses
   * @param activeness criteria for filtering edges based on activeness
   *
   * @return iterator aggregated messages keyed by the receiving vertex id
   * 对一个分区内的数据进行聚合
   */
  def aggregateMessagesEdgeScan[A: ClassTag](
      sendMsg: EdgeContext[VD, ED, A] => Unit,//泛型表示顶点元素类型、边元素类型、发送信息的类型
      mergeMsg: (A, A) => A,//聚合函数
      tripletFields: TripletFields,//是否要获取顶点元素
      activeness: EdgeActiveness) //数据发送方向--边满足什么情况下需要被发送统计值
      : Iterator[(VertexId, A)] = {//每一轮返回一个统计值
    val aggregates = new Array[A](vertexAttrs.length)
    val bitset = new BitSet(vertexAttrs.length)

    var ctx = new AggregatingEdgeContext[VD, ED, A](mergeMsg, aggregates, bitset)
    var i = 0
    while (i < size) {//循环每一个边
      //获取该边的顶点的本地序号以及真实的顶点ID
      val localSrcId = localSrcIds(i)
      val srcId = local2global(localSrcId)
      val localDstId = localDstIds(i)
      val dstId = local2global(localDstId)
      val edgeIsActive =
        if (activeness == EdgeActiveness.Neither) true
        else if (activeness == EdgeActiveness.SrcOnly) isActive(srcId)
        else if (activeness == EdgeActiveness.DstOnly) isActive(dstId)
        else if (activeness == EdgeActiveness.Both) isActive(srcId) && isActive(dstId)
        else if (activeness == EdgeActiveness.Either) isActive(srcId) || isActive(dstId) //有一个活跃即可
        else throw new Exception("unreachable")
      if (edgeIsActive) {
        //获取顶点的属性值
        val srcAttr = if (tripletFields.useSrc) vertexAttrs(localSrcId) else null.asInstanceOf[VD]
        val dstAttr = if (tripletFields.useDst) vertexAttrs(localDstId) else null.asInstanceOf[VD]
        //设置边的上下文信息
        ctx.set(srcId, dstId, localSrcId, localDstId, srcAttr, dstAttr, data(i))
        sendMsg(ctx) //本地节点进行汇总
      }
      i += 1
    }

    bitset.iterator.map { localId => (local2global(localId), aggregates(localId)) } //返回每一个顶点以及对应的统计值
  }

  /**
   * Send messages along edges and aggregate them at the receiving vertices. Implemented by
   * filtering the source vertex index, then scanning each edge cluster.
   *
   * @param sendMsg generates messages to neighboring vertices of an edge
   * @param mergeMsg the combiner applied to messages destined to the same vertex
   * @param tripletFields which triplet fields `sendMsg` uses
   * @param activeness criteria for filtering edges based on activeness
   *
   * @return iterator aggregated messages keyed by the receiving vertex id
   * 先扫描src,如果src都不通过,就没有必要扫描src相同的所有边了,就可以直接跳跃到下一个src开始的边
   */
  def aggregateMessagesIndexScan[A: ClassTag](
      sendMsg: EdgeContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields,
      activeness: EdgeActiveness): Iterator[(VertexId, A)] = {
    val aggregates = new Array[A](vertexAttrs.length)
    val bitset = new BitSet(vertexAttrs.length)

    var ctx = new AggregatingEdgeContext[VD, ED, A](mergeMsg, aggregates, bitset)
    index.iterator.foreach { cluster => //循环每一个src开始的边
      val clusterSrcId = cluster._1 //src顶点
      val clusterPos = cluster._2 //第几个位置开始切换的
      val clusterLocalSrcId = localSrcIds(clusterPos) //该位置对应的本地序号,即src顶点对应的本地序号

      //先扫描src,但是前提是必须要求src为活跃的,如果不是,则都要结果为true,因为没有办法通过索引去优化
      val scanCluster =
        if (activeness == EdgeActiveness.Neither) true //不需要考虑活跃情况
        else if (activeness == EdgeActiveness.SrcOnly) isActive(clusterSrcId)
        else if (activeness == EdgeActiveness.DstOnly) true
        else if (activeness == EdgeActiveness.Both) isActive(clusterSrcId)
        else if (activeness == EdgeActiveness.Either) true
        else throw new Exception("unreachable")

      if (scanCluster) {
        var pos = clusterPos
        val srcAttr =
          if (tripletFields.useSrc) vertexAttrs(clusterLocalSrcId) else null.asInstanceOf[VD]
        ctx.setSrcOnly(clusterSrcId, clusterLocalSrcId, srcAttr)
        while (pos < size && localSrcIds(pos) == clusterLocalSrcId) {//说明这些边都是以src开始的
          val localDstId = localDstIds(pos) //dst顶点的本地序号
          val dstId = local2global(localDstId) //真实的dst顶点
          val edgeIsActive =
            if (activeness == EdgeActiveness.Neither) true
            else if (activeness == EdgeActiveness.SrcOnly) true
            else if (activeness == EdgeActiveness.DstOnly) isActive(dstId)
            else if (activeness == EdgeActiveness.Both) isActive(dstId)
            else if (activeness == EdgeActiveness.Either) isActive(clusterSrcId) || isActive(dstId)
            else throw new Exception("unreachable")
          if (edgeIsActive) {
            val dstAttr =
              if (tripletFields.useDst) vertexAttrs(localDstId) else null.asInstanceOf[VD]
            ctx.setRest(dstId, localDstId, dstAttr, data(pos))
            sendMsg(ctx)
          }
          pos += 1
        }
      }
    }

    bitset.iterator.map { localId => (local2global(localId), aggregates(localId)) }
  }
}

private class AggregatingEdgeContext[VD, ED, A](
    mergeMsg: (A, A) => A,//merge两个结果,产生一个新的结果
    aggregates: Array[A],//存储结果---每一个本地顶点序号对应一个数组,即通过本地序号就能找到该顶点对应的结果
    bitset: BitSet) //true的位表示有聚合值
  extends EdgeContext[VD, ED, A] {

  private[this] var _srcId: VertexId = _
  private[this] var _dstId: VertexId = _
  private[this] var _localSrcId: Int = _
  private[this] var _localDstId: Int = _
  private[this] var _srcAttr: VD = _
  private[this] var _dstAttr: VD = _
  private[this] var _attr: ED = _

  def set(
      srcId: VertexId, dstId: VertexId,
      localSrcId: Int, localDstId: Int,
      srcAttr: VD, dstAttr: VD,
      attr: ED) {
    _srcId = srcId
    _dstId = dstId
    _localSrcId = localSrcId
    _localDstId = localDstId
    _srcAttr = srcAttr
    _dstAttr = dstAttr
    _attr = attr
  }

  def setSrcOnly(srcId: VertexId, localSrcId: Int, srcAttr: VD) {
    _srcId = srcId
    _localSrcId = localSrcId
    _srcAttr = srcAttr
  }

  def setRest(dstId: VertexId, localDstId: Int, dstAttr: VD, attr: ED) {
    _dstId = dstId //目的地的顶点
    _localDstId = localDstId //目的地的本地序号
    _dstAttr = dstAttr //目的地顶点属性
    _attr = attr //边属性
  }

  override def srcId: VertexId = _srcId
  override def dstId: VertexId = _dstId
  override def srcAttr: VD = _srcAttr
  override def dstAttr: VD = _dstAttr
  override def attr: ED = _attr

  //向src节点发送一个值
  override def sendToSrc(msg: A) {
    send(_localSrcId, msg)
  }
  override def sendToDst(msg: A) {
    send(_localDstId, msg)
  }

  //说明某个节点产生了一个值
  @inline private def send(localId: Int, msg: A) {
    if (bitset.get(localId)) {
      aggregates(localId) = mergeMsg(aggregates(localId), msg)
    } else {
      aggregates(localId) = msg
      bitset.set(localId)
    }
  }
}
