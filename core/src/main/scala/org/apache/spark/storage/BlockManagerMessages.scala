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
  //driver通知executor节点删除该数据块
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

  //在executor节点注册了一个BlockManagerId,最多允许使用maxMemSize内存
  case class RegisterBlockManager(
      blockManagerId: BlockManagerId,
      maxMemSize: Long,
      sender: RpcEndpointRef) //该executor节点通信对象
    extends ToBlockManagerMaster

  //更新一个数据块信息--包含该数据块所在节点、数据块ID、该数据块存储级别、该数据块所占内存、磁盘大小
  //即让driver知道存在一个该数据块在该节点上
  //相当于add方法
  case class UpdateBlockInfo(
      var blockManagerId: BlockManagerId,//该数据块所在节点
      var blockId: BlockId,//数据块ID
      var storageLevel: StorageLevel,//该数据块存储级别
      var memSize: Long,//该数据块所占内存大小
      var diskSize: Long,//该数据块所占磁盘大小
      var externalBlockStoreSize: Long)//数据块占用外部存储大小
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

  //相当于select方法
  //获取该数据块对应存储在哪些节点上
  case class GetLocations(blockId: BlockId) extends ToBlockManagerMaster

  //相当于select方法
  //获取多个数据块的位置,每一个数据块都在多个节点上,因此返回的是IndexedSeq[Seq[BlockManagerId]]
  case class GetLocationsMultipleBlockIds(blockIds: Array[BlockId]) extends ToBlockManagerMaster

  //返回除了driver和自己之外的  BlockManagerId集合 Seq[BlockManagerId]
  case class GetPeers(blockManagerId: BlockManagerId) extends ToBlockManagerMaster

  //返回一个执行者的rcp对应的host和port Option[(String, Int)]
  case class GetRpcHostPortForExecutor(executorId: String) extends ToBlockManagerMaster

  //删除一个executor节点
  case class RemoveExecutor(execId: String) extends ToBlockManagerMaster

  //停止该driver节点
  case object StopBlockManagerMaster extends ToBlockManagerMaster

  //返回所有的BlockManagerId,与之该BlockManagerId相关的最大内存和剩余内存,即Map[BlockManagerId, (Long, Long)]
  case object GetMemoryStatus extends ToBlockManagerMaster

  //每一个BlockManagerId对应一个StorageStatus被返回,因此返回的是 Array[StorageStatus]数组
  case object GetStorageStatus extends ToBlockManagerMaster

  //返回该数据块在每一个BlockManagerId节点上的状态集合,格式即Map[BlockManagerId, Future[Option[BlockStatus]]]
  //参数askSlaves 为true,则表示这些状态要去executor节点去现查
  case class GetBlockStatus(blockId: BlockId, askSlaves: Boolean = true)
    extends ToBlockManagerMaster

  //查询所有的数据块,找到跟参数filter函数匹配的数据块集合
  case class GetMatchingBlockIds(filter: BlockId => Boolean, askSlaves: Boolean = true)
    extends ToBlockManagerMaster

  //定期executor节点要心跳给driver节点,让driver节点知道该executor节点还活着
  case class BlockManagerHeartbeat(blockManagerId: BlockManagerId) extends ToBlockManagerMaster
  //判断该executor节点上是否在driver节点上有缓存数据块,返回boolean值
  case class HasCachedBlocks(executorId: String) extends ToBlockManagerMaster
}
