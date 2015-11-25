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

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.Utils

//定义数据块管理过程中的节点间传递的信息
private[spark] object BlockManagerMessages {
  //////////////////////////////////////////////////////////////////////////////////
  // Messages from the master to slaves.信息从master传递到slave
  //////////////////////////////////////////////////////////////////////////////////
  sealed trait ToBlockManagerSlave

  // Remove a block from the slaves that have it. This can only be used to remove
  // blocks that the master knows about.
  //从slaves节点移除一个数据块,如果该数据块存在的话,这个操作仅仅被使用与master已经知道要移动该数据块的数据块
  case class RemoveBlock(blockId: BlockId) extends ToBlockManagerSlave

  // Remove all blocks belonging to a specific RDD.移除该rdd下的所有的数据块
  case class RemoveRdd(rddId: Int) extends ToBlockManagerSlave

  // Remove all blocks belonging to a specific shuffle.移除该shuffle下所有的数据块
  case class RemoveShuffle(shuffleId: Int) extends ToBlockManagerSlave

  // Remove all blocks belonging to a specific broadcast.移除该广播下所有的数据块
  case class RemoveBroadcast(broadcastId: Long, removeFromDriver: Boolean = true)
    extends ToBlockManagerSlave

  //////////////////////////////////////////////////////////////////////////////////
  // Messages from slaves to the master. 从slaves节点发送数据到master节点
  //////////////////////////////////////////////////////////////////////////////////
  sealed trait ToBlockManagerMaster

  case class RegisterBlockManager(
      blockManagerId: BlockManagerId,
      maxMemSize: Long,
      sender: RpcEndpointRef)
    extends ToBlockManagerMaster

  case class UpdateBlockInfo(
      var blockManagerId: BlockManagerId,
      var blockId: BlockId,
      var storageLevel: StorageLevel,
      var memSize: Long,
      var diskSize: Long,
      var externalBlockStoreSize: Long)
    extends ToBlockManagerMaster
    with Externalizable {

    def this() = this(null, null, null, 0, 0, 0)  // For deserialization only

    override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
      blockManagerId.writeExternal(out)
      out.writeUTF(blockId.name)
      storageLevel.writeExternal(out)
      out.writeLong(memSize)
      out.writeLong(diskSize)
      out.writeLong(externalBlockStoreSize)
    }

    override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
      blockManagerId = BlockManagerId(in)
      blockId = BlockId(in.readUTF())
      storageLevel = StorageLevel(in)
      memSize = in.readLong()
      diskSize = in.readLong()
      externalBlockStoreSize = in.readLong()
    }
  }

  case class GetLocations(blockId: BlockId) extends ToBlockManagerMaster

  case class GetLocationsMultipleBlockIds(blockIds: Array[BlockId]) extends ToBlockManagerMaster

  case class GetPeers(blockManagerId: BlockManagerId) extends ToBlockManagerMaster

  case class GetRpcHostPortForExecutor(executorId: String) extends ToBlockManagerMaster

  case class RemoveExecutor(execId: String) extends ToBlockManagerMaster

  case object StopBlockManagerMaster extends ToBlockManagerMaster

  case object GetMemoryStatus extends ToBlockManagerMaster

  case object GetStorageStatus extends ToBlockManagerMaster

  case class GetBlockStatus(blockId: BlockId, askSlaves: Boolean = true)
    extends ToBlockManagerMaster

  case class GetMatchingBlockIds(filter: BlockId => Boolean, askSlaves: Boolean = true)
    extends ToBlockManagerMaster

  case class BlockManagerHeartbeat(blockManagerId: BlockManagerId) extends ToBlockManagerMaster

  case class HasCachedBlocks(executorId: String) extends ToBlockManagerMaster
}
