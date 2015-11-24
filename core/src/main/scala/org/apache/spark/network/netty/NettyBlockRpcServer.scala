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

package org.apache.spark.network.netty

import java.nio.ByteBuffer

import scala.collection.JavaConversions._

import org.apache.spark.Logging
import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.network.server.{OneForOneStreamManager, RpcHandler, StreamManager}
import org.apache.spark.network.shuffle.protocol.{BlockTransferMessage, OpenBlocks, StreamHandle, UploadBlock}
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{BlockId, StorageLevel}

/**
 * Serves requests to open blocks by simply registering one chunk per block requested.
 * Handles opening and uploading arbitrary BlockManager blocks.
 *
 * Opened blocks are registered with the "one-for-one" strategy, meaning each Transport-layer Chunk
 * is equivalent to one Spark-level shuffle block.
 * PRC请求,针对客户端要下载该服务器的哪些数据块，以及该服务器下载别的节点的数据块到本地服务
 */
class NettyBlockRpcServer(
    serializer: Serializer,//序列化的对象
    blockManager: BlockDataManager)//本地磁盘关于数据块的管理对象
  extends RpcHandler with Logging {

  //服务器这边注册一个流ID,该流ID包含了该服务器的数据块集合要发送给哪些客户端
  private val streamManager = new OneForOneStreamManager()

  override def receive(
      client: TransportClient,
      messageBytes: Array[Byte],
      responseContext: RpcResponseCallback): Unit = {
    val message = BlockTransferMessage.Decoder.fromByteArray(messageBytes)
    logTrace(s"Received request: $message")

    message match {
      case openBlocks: OpenBlocks =>
        /**
         * 1.openBlocks.blockIds.map(BlockId.apply) 表示获取BlockId集合
         * 2..map(blockManager.getBlockData) 表示获取该BlockId集合中每一个BlockId对应的Block内容ManagedBuffer集合
         */
        val blocks: Seq[ManagedBuffer] =
          openBlocks.blockIds.map(BlockId.apply).map(blockManager.getBlockData)
          
        //注册一个流ID,使该流ID对应的数据块集合要发送给某一个客户端
        val streamId = streamManager.registerStream(blocks.iterator)
        logTrace(s"Registered streamId $streamId with ${blocks.size} buffers")
        
        //返回成功注册了流ID,以及该流对应多少个数据块准备传输
        responseContext.onSuccess(new StreamHandle(streamId, blocks.size).toByteArray)

      case uploadBlock: UploadBlock =>
        // StorageLevel is serialized as bytes using our JavaSerializer.
        //该服务器接收到了一个数据块
        val level: StorageLevel =
          serializer.newInstance().deserialize(ByteBuffer.wrap(uploadBlock.metadata))
        val data = new NioManagedBuffer(ByteBuffer.wrap(uploadBlock.blockData))
        blockManager.putBlockData(BlockId(uploadBlock.blockId), data, level)
        responseContext.onSuccess(new Array[Byte](0))
    }
  }

  override def getStreamManager(): StreamManager = streamManager
}
