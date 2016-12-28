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

package org.apache.spark.scheduler

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import org.roaringbitmap.RoaringBitmap

import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.Utils

/**
 * Result returned by a ShuffleMapTask to a scheduler. Includes the block manager address that the
 * task ran on as well as the sizes of outputs for each reducer, for passing on to the reduce tasks.
 * 表示ShuffleMapTask去调度后返回的一个结果,包含数据块地址和任务运行输出的大小,将其传递给每一个reduce任务
 */
private[spark] sealed trait MapStatus {
  /** Location where this task was run. 这个task运行的位置,即在哪个节点运行的该数据块*/
  def location: BlockManagerId

  /**
   * Estimated size for the reduce block, in bytes.
   *
   * If a block is non-empty, then this method MUST return a non-zero size.  This invariant is
   * necessary for correctness, since block fetchers are allowed to skip zero-size blocks.
   * 估算某一个reduce的数据块所占大小
   */
  def getSizeForBlock(reduceId: Int): Long
}


private[spark] object MapStatus {

  /**
   *
   * @param loc
   * @param uncompressedSizes 数据块字节数组,数组索引代表的是每一个reduce对应的文件大小预估值
   * @return
   */
  def apply(loc: BlockManagerId, uncompressedSizes: Array[Long]): MapStatus = {
    if (uncompressedSizes.length > 2000) {//超过2000个partition,要如何处理
      HighlyCompressedMapStatus(loc, uncompressedSizes)
    } else {
      new CompressedMapStatus(loc, uncompressedSizes)
    }
  }

  private[this] val LOG_BASE = 1.1

  /**
   * Compress a size in bytes to 8 bits for efficient reporting of map output sizes.
   * We do this by encoding the log base 1.1 of the size as an integer, which can support
   * sizes up to 35 GB with at most 10% error.
   */
  def compressSize(size: Long): Byte = {
    if (size == 0) {
      0
    } else if (size <= 1L) {
      1
    } else {
      math.min(255, math.ceil(math.log(size) / math.log(LOG_BASE)).toInt).toByte
    }
  }

  /**
   * Decompress an 8-bit encoded block size, using the reverse operation of compressSize.
   */
  def decompressSize(compressedSize: Byte): Long = {
    if (compressedSize == 0) {
      0
    } else {
      math.pow(LOG_BASE, compressedSize & 0xFF).toLong
    }
  }
}


/**
 * A [[MapStatus]] implementation that tracks the size of each block. Size for each block is
 * represented using a single byte.
 *
 * @param loc location where the task is being executed.
 * @param compressedSizes size of the blocks, indexed by reduce partition id.
 */
private[spark] class CompressedMapStatus(
    private[this] var loc: BlockManagerId,
    private[this] var compressedSizes: Array[Byte])//数据块字节数组,数组索引代表的是partition的ID,每一个元素表示该partition对应的文件大小预估值
  extends MapStatus with Externalizable {

  protected def this() = this(null, null.asInstanceOf[Array[Byte]])  // For deserialization only

  def this(loc: BlockManagerId, uncompressedSizes: Array[Long]) {
    this(loc, uncompressedSizes.map(MapStatus.compressSize))
  }

  override def location: BlockManagerId = loc

  //获取第reduceId个partition解压后的大小
  override def getSizeForBlock(reduceId: Int): Long = {
    MapStatus.decompressSize(compressedSizes(reduceId))
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    loc.writeExternal(out)
    out.writeInt(compressedSizes.length)//多少个partition
    out.write(compressedSizes)//每一个partition大小
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    loc = BlockManagerId(in)
    val len = in.readInt()
    compressedSizes = new Array[Byte](len)
    in.readFully(compressedSizes)
  }
}

/**
 * A [[MapStatus]] implementation that only stores the average size of non-empty blocks,
 * plus a bitmap for tracking which blocks are empty.  During serialization, this bitmap
 * is compressed.
 *
 * @param loc location where the task is being executed
 * @param numNonEmptyBlocks the number of non-empty blocks
 * @param emptyBlocks a bitmap tracking which blocks are empty
 * @param avgSize average size of the non-empty blocks
 */
private[spark] class HighlyCompressedMapStatus private (
    private[this] var loc: BlockManagerId,//数据存储路径位置
    private[this] var numNonEmptyBlocks: Int,//不是空的数据块的数量
    private[this] var emptyBlocks: RoaringBitmap,//空的数据块集合,即可以知道哪些数据块是空的
    private[this] var avgSize: Long)//平均每一个数据块大小
  extends MapStatus with Externalizable {

  // loc could be null when the default constructor is called during deserialization
  require(loc == null || avgSize > 0 || numNonEmptyBlocks == 0,
    "Average size can only be zero for map stages that produced no output")

  protected def this() = this(null, -1, null, -1)  // For deserialization only

  override def location: BlockManagerId = loc

  override def getSizeForBlock(reduceId: Int): Long = {
    if (emptyBlocks.contains(reduceId)) {//说明该数据块是空,则返回0
      0
    } else {
      avgSize
    }
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    loc.writeExternal(out)
    emptyBlocks.writeExternal(out)//写入空的数据块集合
    out.writeLong(avgSize)//平均每一个数据块多少字节
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    loc = BlockManagerId(in)
    emptyBlocks = new RoaringBitmap()
    emptyBlocks.readExternal(in)
    avgSize = in.readLong()
  }
}

//partition过多的时候,超过2000个,则使用这个压缩技术
private[spark] object HighlyCompressedMapStatus {
  /**
   *
   * @param loc
   * @param uncompressedSizes 数据块字节数组,数组索引代表的是partition的ID,每一个元素表示该partition对应的文件大小预估值
   * @return
   */
  def apply(loc: BlockManagerId, uncompressedSizes: Array[Long]): HighlyCompressedMapStatus = {
    // We must keep track of which blocks are empty so that we don't report a zero-sized
    // block as being non-empty (or vice-versa) when using the average block size.
    var i = 0 //当前第几个数据块了
    var numNonEmptyBlocks: Int = 0//不是空的数据块个数
    var totalSize: Long = 0 //总数据块所占用大小之和
    // From a compression standpoint, it shouldn't matter whether we track empty or non-empty
    // blocks. From a performance standpoint, we benefit from tracking empty blocks because
    // we expect that there will be far fewer of them, so we will perform fewer bitmap insertions.
    val emptyBlocks = new RoaringBitmap()//空的数据块都添加到这个集合里面
    val totalNumBlocks = uncompressedSizes.length //总partition数据块数量
    while (i < totalNumBlocks) {
      var size = uncompressedSizes(i)//获取该数据块大小
      if (size > 0) {//数据块有内容
        numNonEmptyBlocks += 1
        totalSize += size
      } else {
        emptyBlocks.add(i)//添加空的数据块
      }
      i += 1
    }

    //平均每一个数据块占用多少字节
    val avgSize = if (numNonEmptyBlocks > 0) {
      totalSize / numNonEmptyBlocks
    } else {
      0
    }
    new HighlyCompressedMapStatus(loc, numNonEmptyBlocks, emptyBlocks, avgSize)
  }
}
