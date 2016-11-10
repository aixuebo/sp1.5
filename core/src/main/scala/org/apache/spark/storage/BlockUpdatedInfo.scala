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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.storage.BlockManagerMessages.UpdateBlockInfo

/**
 * :: DeveloperApi ::
 * Stores information about a block status in a block manager.
 * 存储一个数据块在一个数据块管理着(block manager)里面的状态信息,用于org.apache.spark.scheduler.SparkListener里面监听SparkListenerBlockUpdated事件,用于更新数据块
 *
 * 更新一个数据块信息--包含该数据块所在节点、数据块ID、该数据块存储级别、该数据块所占内存、磁盘大小
 */
@DeveloperApi
case class BlockUpdatedInfo(
    blockManagerId: BlockManagerId,//数据块所在管理器ID
    blockId: BlockId,//数据块ID
    storageLevel: StorageLevel, //数据块的详细信息
    memSize: Long,
    diskSize: Long,
    externalBlockStoreSize: Long)

private[spark] object BlockUpdatedInfo {

  private[spark] def apply(updateBlockInfo: UpdateBlockInfo): BlockUpdatedInfo = {
    BlockUpdatedInfo(
      updateBlockInfo.blockManagerId,
      updateBlockInfo.blockId,
      updateBlockInfo.storageLevel,
      updateBlockInfo.memSize,
      updateBlockInfo.diskSize,
      updateBlockInfo.externalBlockStoreSize)
  }
}
