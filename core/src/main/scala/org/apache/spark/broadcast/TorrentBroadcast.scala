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

package org.apache.spark.broadcast

import java.io._
import java.nio.ByteBuffer

import scala.collection.JavaConversions.asJavaEnumeration
import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark.{Logging, SparkConf, SparkEnv, SparkException}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{BroadcastBlockId, StorageLevel}
import org.apache.spark.util.{ByteBufferInputStream, Utils}
import org.apache.spark.util.io.ByteArrayChunkOutputStream

/**
 * A BitTorrent-like implementation of [[org.apache.spark.broadcast.Broadcast]].
 *
 * The mechanism is as follows:
 *
 * The driver divides the serialized object into small chunks and
 * stores those chunks in the BlockManager of the driver.
 *
 * On each executor, the executor first attempts to fetch the object from its BlockManager. If
 * it does not exist, it then uses remote fetches to fetch the small chunks from the driver and/or
 * other executors if available. Once it gets the chunks, it puts the chunks in its own
 * BlockManager, ready for other executors to fetch from.
 * 每一个executor第一次尝试从本地抓去该广播的数据块内容
 * 如果内润不存在,则去driver或者其他executor节点获取内容,如果过去到,则存储在本地节点,为以后其他executor节点获取做准备
 *
 * This prevents the driver from being the bottleneck in sending out multiple copies of the
 * broadcast data (one per executor) as done by the [[org.apache.spark.broadcast.HttpBroadcast]].
 *
 * When initialized, TorrentBroadcast objects read SparkEnv.get.conf.
 *
 * @param obj object to broadcast
 * @param id A unique identifier for the broadcast variable.
 *
 *
 * 主要功能:
 * 1.广播数据分片存储成多个数据块
 * 2.懒加载的方式真正读取数据内容
 * 3.本地保存完每一个分片后,通知driver,因此实现了BT功能.即数据分布在多个节点,同时完整的数据被打散成多个数据块
 * 4.序列化的时候,不需要序列化value整个字节,只需要序列化id和多少个分片即可,因为是BT模式,因此拿到这两个参数就可以恢复整个数据
 */
private[spark] class TorrentBroadcast[T: ClassTag](obj: T, id: Long)
  extends Broadcast[T](id) with Logging with Serializable {

  /**
   * Value of the broadcast object on executors. This is reconstructed by [[readBroadcastBlock]],
   * which builds this value by reading blocks from the driver and/or other executors.
   *
   * On the driver, if the value is required, it is read lazily from the block manager.
   */
  @transient private lazy val _value: T = readBroadcastBlock() //懒加载模式获取数据块内容

  /** The compression codec to use, or None if compression is disabled */
  @transient private var compressionCodec: Option[CompressionCodec] = _
  /** Size of each block. Default value is 4MB.  This value is only read by the broadcaster. */
  @transient private var blockSize: Int = _

  private def setConf(conf: SparkConf) {
    compressionCodec = if (conf.getBoolean("spark.broadcast.compress", true)) {
      Some(CompressionCodec.createCodec(conf))
    } else {
      None
    }
    // Note: use getSizeAsKb (not bytes) to maintain compatiblity if no units are provided
    blockSize = conf.getSizeAsKb("spark.broadcast.blockSize", "4m").toInt * 1024
  }
  setConf(SparkEnv.get.conf)

  private val broadcastId = BroadcastBlockId(id)

  /** Total number of blocks this broadcast variable contains.
    * 将数据块分片,以及存储到本地,然后返回数据块分片数量---最终需要被序列化的
    **/
  private val numBlocks: Int = writeBlocks(obj)

  override protected def getValue() = {
    _value
  }

  /**
   * Divide the object into multiple blocks and put those blocks in the block manager.
   * 将该对象拆分成多个数据块分片,存储在本地的数据块管理器中
   * @param value the object to divide
   * @return number of blocks this broadcast variable is divided into 返回拆分了多少个数据块
   */
  private def writeBlocks(value: T): Int = {
    // Store a copy of the broadcast variable in the driver so that tasks run on the driver
    // do not create a duplicate copy of the broadcast variable's value.
    SparkEnv.get.blockManager.putSingle(broadcastId, value, StorageLevel.MEMORY_AND_DISK,
      tellMaster = false) //先将数据块内容写入本地
    val blocks =
      TorrentBroadcast.blockifyObject(value, blockSize, SparkEnv.get.serializer, compressionCodec)//对数据块内容进行分片处理,返回每一个分片的字节数组
    blocks.zipWithIndex.foreach { case (block, i) => //获取(分片字节数组,第几个分片)
      SparkEnv.get.blockManager.putBytes(
        BroadcastBlockId(id, "piece" + i),
        block,
        StorageLevel.MEMORY_AND_DISK_SER,
        tellMaster = true) //单独存储每一个分片
    }
    blocks.length //返回多少个分片
  }

  /** Fetch torrent blocks from the driver and/or other executors.
    * 从driver或者其他executor节点去抓去该广播对应的内容
    **/
  private def readBlocks(): Array[ByteBuffer] = {
    // Fetch chunks of data. Note that all these chunks are stored in the BlockManager and reported
    // to the driver, so other executors can pull these chunks from this executor as well.
    val blocks = new Array[ByteBuffer](numBlocks) //获取多少个分片
    val bm = SparkEnv.get.blockManager

    for (pid <- Random.shuffle(Seq.range(0, numBlocks))) { //打乱抓去分片的顺序,不需要按照顺序抓去
      val pieceId = BroadcastBlockId(id, "piece" + pid)
      logDebug(s"Reading piece $pieceId of $broadcastId")
      // First try getLocalBytes because there is a chance that previous attempts to fetch the
      // broadcast blocks have already fetched some of the blocks. In that case, some blocks
      // would be available locally (on this executor).
      def getLocal: Option[ByteBuffer] = bm.getLocalBytes(pieceId) //本地获取该分片
      def getRemote: Option[ByteBuffer] = bm.getRemoteBytes(pieceId).map { block =>
        // If we found the block from remote executors/driver's BlockManager, put the block
        // in this executor's BlockManager.
        SparkEnv.get.blockManager.putBytes(
          pieceId,
          block,
          StorageLevel.MEMORY_AND_DISK_SER,
          tellMaster = true) //通知master,因此实现了BT模式
        block
      } //远程获取该分片,然后存储在本地
      val block: ByteBuffer = getLocal.orElse(getRemote).getOrElse(
        throw new SparkException(s"Failed to get $pieceId of $broadcastId")) //获取该分片内容
      blocks(pid) = block //存储起来
    }
    blocks
  }

  /**
   * Remove all persisted state associated with this Torrent broadcast on the executors.
   */
  override protected def doUnpersist(blocking: Boolean) {
    TorrentBroadcast.unpersist(id, removeFromDriver = false, blocking)
  }

  /**
   * Remove all persisted state associated with this Torrent broadcast on the executors
   * and driver.
   */
  override protected def doDestroy(blocking: Boolean) {
    TorrentBroadcast.unpersist(id, removeFromDriver = true, blocking)
  }

  /** Used by the JVM when serializing this object. */
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    assertValid()
    out.defaultWriteObject() //序列化id以及分片数量
  }

  private def readBroadcastBlock(): T = Utils.tryOrIOException {
    TorrentBroadcast.synchronized {
      setConf(SparkEnv.get.conf)
      SparkEnv.get.blockManager.getLocal(broadcastId).map(_.data.next()) match {//优先读取本地的数据块
        case Some(x) =>
          x.asInstanceOf[T] //说明本笃就有该数据块数据

        case None => //说明本地没有该广播信息
          logInfo("Started reading broadcast variable " + id)
          val startTimeMs = System.currentTimeMillis()
          val blocks = readBlocks() //远程节点读取该广播的数据块分片集合
          logInfo("Reading broadcast variable " + id + " took" + Utils.getUsedTimeMs(startTimeMs))

          //对抓去回来的所有分片进行反序列化成对象
          val obj = TorrentBroadcast.unBlockifyObject[T](
            blocks, SparkEnv.get.serializer, compressionCodec)
          // Store the merged copy in BlockManager so other tasks on this executor don't
          // need to re-fetch it.
          SparkEnv.get.blockManager.putSingle(
            broadcastId, obj, StorageLevel.MEMORY_AND_DISK, tellMaster = false)//将该广播数据库存储在本地
          obj
      }
    }
  }

}


private object TorrentBroadcast extends Logging {

  //将数据进行分片
  def blockifyObject[T: ClassTag](
      obj: T,//原始广播的数据
      blockSize: Int,//每一个数据块分片大小
      serializer: Serializer,
      compressionCodec: Option[CompressionCodec]): Array[ByteBuffer] = {
    val bos = new ByteArrayChunkOutputStream(blockSize) //输出流
    val out: OutputStream = compressionCodec.map(c => c.compressedOutputStream(bos)).getOrElse(bos) //对输出流进行压缩处理
    val ser = serializer.newInstance()
    val serOut = ser.serializeStream(out) //创建序列化的输出流
    serOut.writeObject[T](obj).close() //对value进行序列化输出到流中
    bos.toArrays.map(ByteBuffer.wrap)
  }

  //顺序读取每一个数据块分片内容,将其结果反序列化成T对象
  def unBlockifyObject[T: ClassTag](
      blocks: Array[ByteBuffer],
      serializer: Serializer,
      compressionCodec: Option[CompressionCodec]): T = {
    require(blocks.nonEmpty, "Cannot unblockify an empty array of blocks")
    val is = new SequenceInputStream(
      asJavaEnumeration(blocks.iterator.map(block => new ByteBufferInputStream(block)))) //读取每一个数据分片
    val in: InputStream = compressionCodec.map(c => c.compressedInputStream(is)).getOrElse(is)
    val ser = serializer.newInstance()
    val serIn = ser.deserializeStream(in)
    val obj = serIn.readObject[T]()
    serIn.close()
    obj
  }

  /**
   * Remove all persisted blocks associated with this torrent broadcast on the executors.
   * If removeFromDriver is true, also remove these persisted blocks on the driver.
   * 通知master移除该数据块
   */
  def unpersist(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit = {
    logDebug(s"Unpersisting TorrentBroadcast $id")
    SparkEnv.get.blockManager.master.removeBroadcast(id, removeFromDriver, blocking)
  }
}
