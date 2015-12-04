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

package org.apache.spark.storage

import scala.collection.mutable

import org.apache.spark.scheduler._

//用于在UI上展示该数据块的详细信息
private[spark] case class BlockUIData(
    blockId: BlockId,
    location: String,
    storageLevel: StorageLevel,
    memSize: Long,
    diskSize: Long,
    externalBlockStoreSize: Long)

/**
 * The aggregated status of stream blocks in an executor
 * 获取所有的数据块状态信息,即该数据块管理者属于在哪个host:port上的执行者
 */
private[spark] case class ExecutorStreamBlockStatus(
    executorId: String,//哪个执行者
    location: String,//host:port
    blocks: Seq[BlockUIData]) {//该执行者管理的数据块集合

  def totalMemSize: Long = blocks.map(_.memSize).sum

  def totalDiskSize: Long = blocks.map(_.diskSize).sum

  def totalExternalBlockStoreSize: Long = blocks.map(_.externalBlockStoreSize).sum

  def numStreamBlocks: Int = blocks.size //管理的数据块数量

}

private[spark] class BlockStatusListener extends SparkListener {

  //存储每一个BlockManagerId对应的数据块集合HashMap[BlockId, BlockUIData]
  private val blockManagers =
    new mutable.HashMap[BlockManagerId, mutable.HashMap[BlockId, BlockUIData]]

  //更新一个数据块信息
  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = {
    val blockId = blockUpdated.blockUpdatedInfo.blockId
    if (!blockId.isInstanceOf[StreamBlockId]) {
      // Now we only monitor StreamBlocks
      return
    }
    val blockManagerId = blockUpdated.blockUpdatedInfo.blockManagerId
    val storageLevel = blockUpdated.blockUpdatedInfo.storageLevel
    val memSize = blockUpdated.blockUpdatedInfo.memSize
    val diskSize = blockUpdated.blockUpdatedInfo.diskSize
    val externalBlockStoreSize = blockUpdated.blockUpdatedInfo.externalBlockStoreSize

    synchronized {
      // Drop the update info if the block manager is not registered
      blockManagers.get(blockManagerId).foreach { blocksInBlockManager =>
        if (storageLevel.isValid) {
          blocksInBlockManager.put(blockId,
            BlockUIData(
              blockId,
              blockManagerId.hostPort,
              storageLevel,
              memSize,
              diskSize,
              externalBlockStoreSize)
          )
        } else {
          // If isValid is not true, it means we should drop the block.
          blocksInBlockManager -= blockId
        }
      }
    }
  }

  //增加一个数据块manager
  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {
    synchronized {
      blockManagers.put(blockManagerAdded.blockManagerId, mutable.HashMap())
    }
  }

  //减少一个数据块manager
  override def onBlockManagerRemoved(
      blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = synchronized {
    blockManagers -= blockManagerRemoved.blockManagerId
  }

  //获取所有的数据块状态信息,即该数据块管理者属于在哪个host:port上的执行者
  def allExecutorStreamBlockStatus: Seq[ExecutorStreamBlockStatus] = synchronized {
    blockManagers.map { case (blockManagerId, blocks) =>
      ExecutorStreamBlockStatus(
        blockManagerId.executorId, blockManagerId.hostPort, blocks.values.toSeq)
    }.toSeq
  }
}
