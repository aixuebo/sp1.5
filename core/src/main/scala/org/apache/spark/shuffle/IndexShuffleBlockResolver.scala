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

package org.apache.spark.shuffle

import java.io._

import com.google.common.io.ByteStreams

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.storage._
import org.apache.spark.util.Utils

import IndexShuffleBlockResolver.NOOP_REDUCE_ID

/**
 * Create and maintain the shuffle blocks' mapping between logic block and physical file location.
 * Data of shuffle blocks from the same map task are stored in a single consolidated data file.
 * The offsets of the data blocks in the data file are stored in a separate index file.
 *
 * We use the name of the shuffle data's shuffleBlockId with reduce ID set to 0 and add ".data"
 * as the filename postfix for data file, and ".index" as the filename postfix for index file.、
 * 可以读取一个数据块的真实内容
 */
// Note: Changes to the format in this file should be kept in sync with
// org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getSortBasedShuffleBlockData().
private[spark] class IndexShuffleBlockResolver(conf: SparkConf) extends ShuffleBlockResolver {

  private lazy val blockManager = SparkEnv.get.blockManager

  private val transportConf = SparkTransportConf.fromSparkConf(conf)

  /**
   * 获取某一个shuffleId下某一个map的数据文件,即获取第0个reduce的结果
   */
  def getDataFile(shuffleId: Int, mapId: Int): File = {
    blockManager.diskBlockManager.getFile(ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
  }

  /**
   * 获取某一个shuffleId下某一个map的索引文件
   */
  private def getIndexFile(shuffleId: Int, mapId: Int): File = {
    blockManager.diskBlockManager.getFile(ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
  }

  /**
   * Remove data file and index file that contain the output data from one map.
   * 删除某一个shuffle下某一个map上所有的数据,包括索引文件
   * */
  def removeDataByMap(shuffleId: Int, mapId: Int): Unit = {
    var file = getDataFile(shuffleId, mapId)
    if (file.exists()) {
      file.delete()
    }

    file = getIndexFile(shuffleId, mapId)
    if (file.exists()) {
      file.delete()
    }
  }

  /**
   * Write an index file with the offsets of each block, plus a final offset at the end for the
   * end of the output file. This will be used by getBlockData to figure out where each block
   * begins and ends.
   * 写入索引文件,将每一个数据块的开始位置写入到索引文件中
   *
   * 索引文件的内容是每一个reduce的offset
   * */
  def writeIndexFile(shuffleId: Int, mapId: Int, lengths: Array[Long]): Unit = {
    val indexFile = getIndexFile(shuffleId, mapId)
    val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexFile)))
    Utils.tryWithSafeFinally {
      // We take in lengths of each block, need to convert it to offsets.
      var offset = 0L
      out.writeLong(offset)
      for (length <- lengths) {
        offset += length
        out.writeLong(offset)
      }
    } {
      out.close()
    }
  }

  //读取某一个数据块的信息字节流
  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
    // The block is actually going to be a range of a single map output file for this map, so
    // find out the consolidated file, then the offset within that from our index
    //获取该shuffle文件的map对应的索引文件
    val indexFile = getIndexFile(blockId.shuffleId, blockId.mapId)

    //读取索引文件
    val in = new DataInputStream(new FileInputStream(indexFile))
    try {
      ByteStreams.skipFully(in, blockId.reduceId * 8) //跳过reduce对应的数据块的开始位置
      val offset = in.readLong() //获取该数据块所在数据块位置
      val nextOffset = in.readLong() //读取数据块长度
      
      //返回值就是可以读取所属的数据信息
      new FileSegmentManagedBuffer(
        transportConf,
        getDataFile(blockId.shuffleId, blockId.mapId),
        offset,
        nextOffset - offset)
    } finally {
      in.close()
    }
  }

  override def stop(): Unit = {}
}

private[spark] object IndexShuffleBlockResolver {
  // No-op reduce ID used in interactions with disk store and DiskBlockObjectWriter.
  // The disk store currently expects puts to relate to a (map, reduce) pair, but in the sort
  // shuffle outputs for several reduces are glommed into a single file.
  // TODO: Avoid this entirely by having the DiskBlockObjectWriter not require a BlockId.
  val NOOP_REDUCE_ID = 0
}
