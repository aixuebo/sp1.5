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

package org.apache.spark.network

import java.io.Closeable
import java.nio.ByteBuffer

import scala.concurrent.{Promise, Await, Future}
import scala.concurrent.duration.Duration

import org.apache.spark.Logging
import org.apache.spark.network.buffer.{NioManagedBuffer, ManagedBuffer}
import org.apache.spark.network.shuffle.{ShuffleClient, BlockFetchingListener}
import org.apache.spark.storage.{BlockManagerId, BlockId, StorageLevel}

private[spark]
abstract class BlockTransferService extends ShuffleClient with Closeable with Logging {

  /**
   * Initialize the transfer service by giving it the BlockDataManager that can be used to fetch
   * local blocks or put local blocks.
   * 初始化传输数据块服务,参数BlockDataManager可以帮助我们抓取本地数据块以及存储数据块到本地
   */
  def init(blockDataManager: BlockDataManager)

  /**
   * Tear down the transfer service.
   */
  def close(): Unit

  /**
   * Port number the service is listening on, available only after [[init]] is invoked.
   */
  def port: Int

  /**
   * Host name the service is listening on, available only after [[init]] is invoked.
   */
  def hostName: String

  /**
   * Fetch a sequence of blocks from a remote node asynchronously,
   * available only after [[init]] is invoked.
   *
   * Note that this API takes a sequence so the implementation can batch requests, and does not
   * return a future so the underlying implementation can invoke onBlockFetchSuccess as soon as
   * the data of a block is fetched, rather than waiting for all blocks to be fetched.
   * 抓取host:port上的数据块信息
   */
  override def fetchBlocks(
      host: String,
      port: Int,
      execId: String,
      blockIds: Array[String],
      listener: BlockFetchingListener): Unit

  /**
   * Upload a single block to a remote node, available only after [[init]] is invoked.
   * 将参数数据块BlockId与内容blockData 上传到host:port上
   */
  def uploadBlock(
      hostname: String,
      port: Int,
      execId: String,
      blockId: BlockId,
      blockData: ManagedBuffer,
      level: StorageLevel): Future[Unit]

  /**
   * A special case of [[fetchBlocks]], as it fetches only one block and is blocking.
   * 阻塞的方式,仅仅抓取一个数据块
   * It is also only available after [[init]] is invoked.
   * 去host:port上抓取数据块blockId信息
   * 返回数据块内容
   */
  def fetchBlockSync(host: String, port: Int, execId: String, blockId: String): ManagedBuffer = {
    // A monitor for the thread to wait on.
    val result = Promise[ManagedBuffer]()
    fetchBlocks(host, port, execId, Array(blockId),
      new BlockFetchingListener {
        override def onBlockFetchFailure(blockId: String, exception: Throwable): Unit = {
          result.failure(exception)
        }
        override def onBlockFetchSuccess(blockId: String, data: ManagedBuffer): Unit = {
          val ret = ByteBuffer.allocate(data.size.toInt)//由数据块大小,创建内存缓冲区
          ret.put(data.nioByteBuffer())//将数据块内容存储到缓冲区内
          ret.flip()
          result.success(new NioManagedBuffer(ret))//将数据块缓冲区内容设置到结果集中
        }
      })

    Await.result(result.future, Duration.Inf)
  }

  /**
   * Upload a single block to a remote node, available only after [[init]] is invoked.
   *
   * This method is similar to [[uploadBlock]], except this one blocks the thread
   * until the upload finishes.
   * 同步上传一个数据块到指定host:port上
   */
  def uploadBlockSync(
      hostname: String,//上传的目的地
      port: Int,//上传的目的地
      execId: String,//上传的目的地
      blockId: BlockId,//要上传的数据块ID
      blockData: ManagedBuffer,//要上传的数据块内容
      level: StorageLevel): Unit = {//要上传的存储信息
    Await.result(uploadBlock(hostname, port, execId, blockId, blockData, level), Duration.Inf)
  }
}
