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

import java.util.{HashMap => JHashMap}

import scala.collection.immutable.HashSet
import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}

import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv, RpcCallContext, ThreadSafeRpcEndpoint}
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * BlockManagerMasterEndpoint is an [[ThreadSafeRpcEndpoint]] on the master node to track statuses
 * of all slaves' block managers.
 * 该类在driver上执行,是一个线程安全的终端,用于跟踪所有slave上的数据块管理器的状态
 */
private[spark]
class BlockManagerMasterEndpoint(
    override val rpcEnv: RpcEnv,//driver所在的host和port组成对对象
    val isLocal: Boolean,//true表示使用local方式启动的sparkContext
    conf: SparkConf,
    listenerBus: LiveListenerBus)
  extends ThreadSafeRpcEndpoint with Logging {

  // Mapping from executor ID to block manager ID.知道执行者ID就可以获取对应的BlockManagerId对象
  private val blockManagerIdByExecutor = new mutable.HashMap[String, BlockManagerId]

  // Mapping from block manager id to the block manager's information.知道BlockManagerId对象,就可以获取对应的BlockManagerInfo对象
  private val blockManagerInfo = new mutable.HashMap[BlockManagerId, BlockManagerInfo]
  
  // Mapping from block id to the set of block managers that have the block.
  //可以知道一个数据块,在哪些solve节点管理着
  private val blockLocations = new JHashMap[BlockId, mutable.HashSet[BlockManagerId]]

  private val askThreadPool = ThreadUtils.newDaemonCachedThreadPool("block-manager-ask-thread-pool")
  private implicit val askExecutionContext = ExecutionContext.fromExecutorService(askThreadPool)

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

    //从slaves节点发送数据到master节点 beigin
    case RegisterBlockManager(blockManagerId, maxMemSize, slaveEndpoint) =>
      register(blockManagerId, maxMemSize, slaveEndpoint) //在slave节点注册了一个BlockManagerId,最多允许使用maxMemSize内存
      context.reply(true)

      /**
       * 更新一个数据块信息--包含该数据块所在节点、数据块ID、该数据块存储级别、该数据块所占内存、磁盘大小
       */
    case _updateBlockInfo @ UpdateBlockInfo(
      blockManagerId, blockId, storageLevel, deserializedSize, size, externalBlockStoreSize) =>
      context.reply(updateBlockInfo(
        blockManagerId, blockId, storageLevel, deserializedSize, size, externalBlockStoreSize))

      //发送事件
      listenerBus.post(SparkListenerBlockUpdated(BlockUpdatedInfo(_updateBlockInfo))) 

    case GetLocations(blockId) =>
      context.reply(getLocations(blockId)) //获取该数据块对应存储在哪些节点上

  /**
   * 获取一组数据块的集合
   * 参数 blockIds是等待获取数据块的集合数组
   * 返回值 是IndexedSeq[Seq[BlockManagerId]],每一个元素是一个数据块对应的BlockManagerId集合,相当于一个二维数组
   */
    case GetLocationsMultipleBlockIds(blockIds) =>
      context.reply(getLocationsMultipleBlockIds(blockIds))

      /**
       * 返回除了driver和自己之外的  BlockManagerId集合
       */
    case GetPeers(blockManagerId) =>
      context.reply(getPeers(blockManagerId))

      /**
       * 返回一个执行者的rcp对应的host和port Option[(String, Int)]
       */
    case GetRpcHostPortForExecutor(executorId) =>
      context.reply(getRpcHostPortForExecutor(executorId))

    //删除一个执行节点
    case RemoveExecutor(execId) =>
      removeExecutor(execId)
      context.reply(true)

      //停止该driver节点
    case StopBlockManagerMaster =>
      context.reply(true)
      stop()

      /**
       * 返回所有的BlockManagerId,与之该BlockManagerId相关的最大内存和剩余内存,即Map[BlockManagerId, (Long, Long)]
       */
    case GetMemoryStatus =>
      context.reply(memoryStatus)

    //每一个BlockManagerId对应一个StorageStatus被返回,因此返回的是 Array[StorageStatus]数组
    case GetStorageStatus =>
      context.reply(storageStatus)

    //返回该数据块在每一个BlockManagerId节点上的状态集合,格式即Map[BlockManagerId, Future[Option[BlockStatus]]]
    //参数askSlaves 为true,则表示这些状态要去executor节点去现查
    case GetBlockStatus(blockId, askSlaves) =>
      context.reply(blockStatus(blockId, askSlaves))

    //查询所有的数据块,找到跟参数filter函数匹配的数据块集合
    case GetMatchingBlockIds(filter, askSlaves) =>
      context.reply(getMatchingBlockIds(filter, askSlaves))

    //定期executor节点要心跳给driver节点,让driver节点知道该executor节点还活着
    case BlockManagerHeartbeat(blockManagerId) =>
      context.reply(heartbeatReceived(blockManagerId))

    //判断该executor节点上是否在driver节点上有缓存数据块,返回boolean值
    case HasCachedBlocks(executorId) =>
      blockManagerIdByExecutor.get(executorId) match {//找到该executorId对应的BlockManagerId对象
        case Some(bm) =>
          if (blockManagerInfo.contains(bm)) {//driver中是否有该节点对应的内存映射对象BlockManagerInfo
            val bmInfo = blockManagerInfo(bm)
            context.reply(bmInfo.cachedBlocks.nonEmpty)//判断此时缓存的数据块是否存在
          } else {
            context.reply(false)
          }
        case None => context.reply(false)
      }
    //从slaves节点发送数据到master节点 end

    //从master节点发送数据到slaves节点 beigin
    case RemoveBlock(blockId) =>
      removeBlockFromWorkers(blockId) //将该数据块从work节点移除
      context.reply(true)

    case RemoveRdd(rddId) =>
      context.reply(removeRdd(rddId)) //删除该RDD

    case RemoveShuffle(shuffleId) =>
      context.reply(removeShuffle(shuffleId)) //删除一个shffle

    case RemoveBroadcast(broadcastId, removeFromDriver) =>
      context.reply(removeBroadcast(broadcastId, removeFromDriver)) //删除一个广播

    //从master节点发送数据到slaves节点 end

  }

  //删除一个RDD
  private def removeRdd(rddId: Int): Future[Seq[Int]] = {
    // First remove the metadata for the given RDD, and then asynchronously remove the blocks
    // from the slaves.

    // Find all blocks for the given RDD, remove the block from both blockLocations and
    // the blockManagerInfo that is tracking the blocks.
    //过滤所有数据该rddid的数据块集合,找到要删除的RDD集合
    val blocks = blockLocations.keys.flatMap(_.asRDDId).filter(_.rddId == rddId)
    /**
     * 1.循环每一个rddid对应的数据块集合
     * 2.定位该数据块在哪些BlockManagerId节点上
     * 3.循环BlockManagerId节点,从内存上移除这些数据块所占用映射
     * 4.删除该数据块所有映射
     */
    blocks.foreach { blockId =>
      val bms: mutable.HashSet[BlockManagerId] = blockLocations.get(blockId)  //一个数据块在多个节点上
      bms.foreach(bm => blockManagerInfo.get(bm).foreach(_.removeBlock(blockId))) //删除该节点在driver上的内存映射
      blockLocations.remove(blockId) //删除该数据块的内存映射
    }

    // Ask the slaves to remove the RDD, and put the result in a sequence of Futures.
    // The dispatcher is used as an implicit argument into the Future sequence construction.
    //去每一个真正的节点删除该数据块的信息
    //其实没必要让所有的节点都去删除该RDD,因为不是所有节点都有该rdd,不过不知道作者怎么想的
    val removeMsg = RemoveRdd(rddId)
    Future.sequence(
      blockManagerInfo.values.map { bm =>
        bm.slaveEndpoint.ask[Int](removeMsg)
      }.toSeq //去每一个节点真正去删除该RDD数据块
    )
  }

  //删除所有节点上shuffer信息
  private def removeShuffle(shuffleId: Int): Future[Seq[Boolean]] = {
    // Nothing to do in the BlockManagerMasterEndpoint data structures 在driver的缓存中什么也不做,因为driver不接受shuffle文件信息
    val removeMsg = RemoveShuffle(shuffleId)
    Future.sequence(
      blockManagerInfo.values.map { bm =>
        bm.slaveEndpoint.ask[Boolean](removeMsg) //直接发送所有的节点,让他们删除shuffle文件
      }.toSeq
    )
  }

  /**
   * Delegate RemoveBroadcast messages to each BlockManager because the master may not notified
   * of all broadcast blocks. If removeFromDriver is false, broadcast blocks are only removed
   * from the executors, but not from the driver.
   */
  private def removeBroadcast(broadcastId: Long, removeFromDriver: Boolean): Future[Seq[Int]] = {
    val removeMsg = RemoveBroadcast(broadcastId, removeFromDriver)
    val requiredBlockManagers = blockManagerInfo.values.filter { info =>
      removeFromDriver || !info.blockManagerId.isDriver
    }
    Future.sequence(
      requiredBlockManagers.map { bm =>
        bm.slaveEndpoint.ask[Int](removeMsg) //通知所有的节点,删除他们的广播文件
      }.toSeq
    )
  }

  //删除一个executor节点
  private def removeBlockManager(blockManagerId: BlockManagerId) {
    val info = blockManagerInfo(blockManagerId)//返回对应的内存对象BlockManagerInfo

    // Remove the block manager from blockManagerIdByExecutor.
    blockManagerIdByExecutor -= blockManagerId.executorId //删除内存映射

    // Remove it from blockManagerInfo and remove all the blocks.
    blockManagerInfo.remove(blockManagerId) //删除内存映射
    val iterator = info.blocks.keySet.iterator //循环该节点上所有数据块
    while (iterator.hasNext) {
      val blockId = iterator.next
      val locations = blockLocations.get(blockId) //每一个要删除的数据块对应的其他节点集合
      locations -= blockManagerId //从节点集合中删除这次要删除的节点
      if (locations.size == 0) {
        blockLocations.remove(blockId) //说明该数据块没有节点存储
      }
    }

    //发送事件
    listenerBus.post(SparkListenerBlockManagerRemoved(System.currentTimeMillis(), blockManagerId))
    logInfo(s"Removing block manager $blockManagerId")
  }

  //删除一个executor节点
  private def removeExecutor(execId: String) {
    logInfo("Trying to remove executor " + execId + " from BlockManagerMaster.")
    blockManagerIdByExecutor.get(execId).foreach(removeBlockManager)
  }

  /**
   * Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   * 定期executor节点要心跳给driver节点,让driver节点知道该executor节点还活着
   */
  private def heartbeatReceived(blockManagerId: BlockManagerId): Boolean = {
    if (!blockManagerInfo.contains(blockManagerId)) {
      blockManagerId.isDriver && !isLocal
    } else {
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      true
    }
  }

  // Remove a block from the slaves that have it. This can only be used to remove
  // blocks that the master knows about.
  //将该数据块从work节点移除
  private def removeBlockFromWorkers(blockId: BlockId) {
    val locations = blockLocations.get(blockId) //找到该数据块在哪些节点上存在
    if (locations != null) {
      //循环每一个节点
      locations.foreach { blockManagerId: BlockManagerId =>
        val blockManager = blockManagerInfo.get(blockManagerId)//找到该节点对应的对象
        if (blockManager.isDefined) {
          // Remove the block from the slave's BlockManager.
          // Doesn't actually wait for a confirmation and the message might get lost.
          // If message loss becomes frequent, we should add retry logic here.
          blockManager.get.slaveEndpoint.ask[Boolean](RemoveBlock(blockId)) //通知该节点删除该数据块
        }
      }
    }
  }

  // Return a map from the block manager id to max memory and remaining memory.
  //返回所有的BlockManagerId,与之该BlockManagerId相关的最大内存和剩余内存,即Map[BlockManagerId, (Long, Long)]
  private def memoryStatus: Map[BlockManagerId, (Long, Long)] = {
    blockManagerInfo.map { case(blockManagerId, info) =>
      (blockManagerId, (info.maxMem, info.remainingMem))
    }.toMap
  }

  /**
   * 1.循环HashMap[BlockManagerId, BlockManagerInfo]
   * 2.每一个BlockManagerId对应一个StorageStatus被返回,因此返回的是 Array[StorageStatus]数组
   */
  private def storageStatus: Array[StorageStatus] = {
    blockManagerInfo.map { case (blockManagerId, info) =>
      new StorageStatus(blockManagerId, info.maxMem, info.blocks)
    }.toArray
  }

  /**
   * Return the block's status for all block managers, if any. NOTE: This is a
   * potentially expensive operation and should only be used for testing.
   * 注意,这个是一个很耗时的操作,仅仅用于测试
   * If askSlaves is true, the master queries each block manager for the most updated block
   * statuses. This is useful when the master is not informed of the given block by all block
   * managers.
   * 返回该数据块在每一个BlockManagerId节点上的状态集合,格式即Map[BlockManagerId, Future[Option[BlockStatus]]]
   * 参数askSlaves 为true,则表示这些状态要去executor节点去现查
   */
  private def blockStatus(
      blockId: BlockId,
      askSlaves: Boolean): Map[BlockManagerId, Future[Option[BlockStatus]]] = {
    val getBlockStatus = GetBlockStatus(blockId)
    /*
     * Rather than blocking on the block status query, master endpoint should simply return
     * Futures to avoid potential deadlocks. This can arise if there exists a block manager
     * that is also waiting for this master endpoint's response to a previous message.
     */
    blockManagerInfo.values.map { info =>
      val blockStatusFuture =
        if (askSlaves) {
          info.slaveEndpoint.ask[Option[BlockStatus]](getBlockStatus)
        } else {
          Future { info.getStatus(blockId) }
        }
      (info.blockManagerId, blockStatusFuture)
    }.toMap
  }

  /**
   * Return the ids of blocks present in all the block managers that match the given filter.
   * NOTE: This is a potentially expensive operation and should only be used for testing.
   *
   * If askSlaves is true, the master queries each block manager for the most updated block
   * statuses. This is useful when the master is not informed of the given block by all block
   * managers.
   * 查询所有的数据块,找到跟参数filter函数匹配的数据块集合
   */
  private def getMatchingBlockIds(
      filter: BlockId => Boolean,
      askSlaves: Boolean): Future[Seq[BlockId]] = {
    val getMatchingBlockIds = GetMatchingBlockIds(filter)
    Future.sequence(
      blockManagerInfo.values.map { info =>
        val future =
          if (askSlaves) {
            info.slaveEndpoint.ask[Seq[BlockId]](getMatchingBlockIds)
          } else {
            Future { info.blocks.keys.filter(filter).toSeq }
          }
        future
      }
    ).map(_.flatten.toSeq)
  }

  //在slave节点注册了一个BlockManagerId,最多允许使用maxMemSize内存
  /**
   * @param id
   * @param maxMemSize
   * @param slaveEndpoint 表示该executor节点的代理对象
   */
  private def register(id: BlockManagerId, maxMemSize: Long, slaveEndpoint: RpcEndpointRef) {
    val time = System.currentTimeMillis()
    if (!blockManagerInfo.contains(id)) {//说明我们还不知道该BlockManagerId
      blockManagerIdByExecutor.get(id.executorId) match {//必须一个executorId对应一个BlockManagerId对象
        case Some(oldId) =>
          // A block manager of the same executor already exists, so remove it (assumed dead)
          logError("Got two different block manager registrations on same executor - "
              + s" will replace old one $oldId with new one $id")
          removeExecutor(id.executorId)
        case None =>
      }

      //记录刚刚注册进来的节点ip信息以及内存信息
      logInfo("Registering block manager %s with %s RAM, %s".format(
        id.hostPort, Utils.bytesToString(maxMemSize), id))

      //一个executorId对应一个BlockManagerId对象
      blockManagerIdByExecutor(id.executorId) = id

      //在driver本地建立一个BlockManagerId对象的内存映射
      blockManagerInfo(id) = new BlockManagerInfo(
        id, System.currentTimeMillis(), maxMemSize, slaveEndpoint)
    }

    //发送事件
    listenerBus.post(SparkListenerBlockManagerAdded(time, id, maxMemSize))
  }

  //更新一个数据块信息--包含该数据块所在节点、数据块ID、该数据块存储级别、该数据块所占内存、磁盘大小
  private def updateBlockInfo(
      blockManagerId: BlockManagerId,
      blockId: BlockId,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long,
      externalBlockStoreSize: Long): Boolean = {

    if (!blockManagerInfo.contains(blockManagerId)) {//说明目前不识别该节点,这个不正常
      if (blockManagerId.isDriver && !isLocal) {
        // We intentionally do not register the master (except in local mode),
        // so we should not indicate failure.
        return true
      } else {
        return false
      }
    }

    if (blockId == null) {//仅仅更新最后访问时间,说明该executor节点还活着
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      return true
    }

    //更新driver关于该数据块的该节点的内存信息
    blockManagerInfo(blockManagerId).updateBlockInfo(
      blockId, storageLevel, memSize, diskSize, externalBlockStoreSize)

      //更新该数据块对应的BlockManagerId集合
    var locations: mutable.HashSet[BlockManagerId] = null
    if (blockLocations.containsKey(blockId)) {
      locations = blockLocations.get(blockId)
    } else {
      locations = new mutable.HashSet[BlockManagerId]
      blockLocations.put(blockId, locations)
    }

    //将该节点存储到该数据块集合中
    if (storageLevel.isValid) {
      locations.add(blockManagerId)
    } else {//如果该存储不可用,则删除该数据块
      locations.remove(blockManagerId)
    }

    // Remove the block from master tracking if it has been removed on all slaves.
    if (locations.size == 0) {//说明没有该数据块对应的节点,则删除该数据块的映射
      blockLocations.remove(blockId)
    }
    true
  }

  //获取该数据块对应存储在哪些节点上
  private def getLocations(blockId: BlockId): Seq[BlockManagerId] = {
    if (blockLocations.containsKey(blockId)) blockLocations.get(blockId).toSeq else Seq.empty
  }

  /**
   * 获取一组数据块的集合
   * 参数 blockIds是等待获取数据块的集合数组
   * 返回值 是IndexedSeq[Seq[BlockManagerId]],每一个元素是一个数据块对应的BlockManagerId集合,相当于一个二维数组
   */
  private def getLocationsMultipleBlockIds(
      blockIds: Array[BlockId]): IndexedSeq[Seq[BlockManagerId]] = {
    blockIds.map(blockId => getLocations(blockId))
  }

  /** Get the list of the peers of the given block manager
   * 返回除了driver和自己之外的  BlockManagerId集合
   **/
  private def getPeers(blockManagerId: BlockManagerId): Seq[BlockManagerId] = {
    val blockManagerIds = blockManagerInfo.keySet //该driver所有的节点集合
    if (blockManagerIds.contains(blockManagerId)) {//说明该节点是其中一个,因此可以回复一些信息给该节点
      blockManagerIds.filterNot { _.isDriver }.filterNot { _ == blockManagerId }.toSeq
    } else {//说明该节点没权限,因此返回空集合
      Seq.empty
    }
  }

  /**
   * Returns the hostname and port of an executor, based on the [[RpcEnv]] address of its
   * [[BlockManagerSlaveEndpoint]].
   * 返回一个执行者的rcp对应的host和port
   */
  private def getRpcHostPortForExecutor(executorId: String): Option[(String, Int)] = {
    for (
      blockManagerId <- blockManagerIdByExecutor.get(executorId);
      info <- blockManagerInfo.get(blockManagerId)
    ) yield {
      (info.slaveEndpoint.address.host, info.slaveEndpoint.address.port)
    }
  }

  override def onStop(): Unit = {
    askThreadPool.shutdownNow()
  }
}

@DeveloperApi
case class BlockStatus(
    storageLevel: StorageLevel,//该数据块的存储级别
    memSize: Long,//该数据块的内存使用
    diskSize: Long,//该数据块的磁盘使用
    externalBlockStoreSize: Long) {//该数据块的外部存储使用
  
  //true表示有数据,则就要去缓存
  def isCached: Boolean = memSize + diskSize + externalBlockStoreSize > 0
}

@DeveloperApi
object BlockStatus {
  def empty: BlockStatus = BlockStatus(StorageLevel.NONE, 0L, 0L, 0L)
}

/**
 * 该对象是在driver节点上缓存的一个对象,表示一个BlockManagerId对应的信息
 */
private[spark] class BlockManagerInfo(
    val blockManagerId: BlockManagerId,//确定哪个执行者在哪个节点上的管理器
    timeMs: Long,//创建时间
    val maxMem: Long,//最多允许使用内存
    val slaveEndpoint: RpcEndpointRef) //与该slove节点进行通信的客户端
  extends Logging {

  private var _lastSeenMs: Long = timeMs //最后访问时间
  private var _remainingMem: Long = maxMem //剩余可用内存

  // Mapping from block id to its status.映射该BlockManagerId上存储的数据块以及数据块状态
  private val _blocks = new JHashMap[BlockId, BlockStatus]

  // Cached blocks held by this BlockManager. This does not include broadcast blocks.
  private val _cachedBlocks = new mutable.HashSet[BlockId]

  //获取给定的BlockId对应的状态
  def getStatus(blockId: BlockId): Option[BlockStatus] = Option(_blocks.get(blockId))

  //更新最后访问时间
  def updateLastSeenMs() {
    _lastSeenMs = System.currentTimeMillis()
  }

  //更新一个数据块
  def updateBlockInfo(
      blockId: BlockId, //数据块ID
      storageLevel: StorageLevel,//该数据块的存储级别
      memSize: Long, //该数据块使用内存大小
      diskSize: Long,//该数据块使用磁盘大小
      externalBlockStoreSize: Long) {//该数据块使用外部存储大小

    updateLastSeenMs() //更新最后访问时间

    if (_blocks.containsKey(blockId)) { //更新该数据块的状态
      // The block exists on the slave already.
      val blockStatus: BlockStatus = _blocks.get(blockId) //以前存储的该数据块对应的状态对象
      val originalLevel: StorageLevel = blockStatus.storageLevel //原始存储级别
      val originalMemSize: Long = blockStatus.memSize //原始内存使用量

      if (originalLevel.useMemory) {
        _remainingMem += originalMemSize //先释放内存,因为后续会添加内存
      }
    }

    if (storageLevel.isValid) {//有效,则更新
      /* isValid means it is either stored in-memory, on-disk or on-externalBlockStore.
       * 合法意味着他是in-memory, on-disk or on-externalBlockStore.三者之一存储方式
       * The memSize here indicates the data size in or dropped from memory,memSize值指代着是内存
       * externalBlockStoreSize here indicates the data size in or dropped from externalBlockStore,externalBlockStoreSize值指代着是外部存储
       * and the diskSize here indicates the data size in or dropped to disk.disk值指代着是磁盘存储
       * They can be both larger than 0, when a block is dropped from memory to disk.
       * Therefore, a safe way to set BlockStatus is to set its info in accurate modes. */
      var blockStatus: BlockStatus = null
      if (storageLevel.useMemory) {//减少内存内存使用量
        blockStatus = BlockStatus(storageLevel, memSize, 0, 0)
        _blocks.put(blockId, blockStatus)
        _remainingMem -= memSize
        logInfo("Added %s in memory on %s (size: %s, free: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(memSize),
          Utils.bytesToString(_remainingMem)))
      }
      if (storageLevel.useDisk) {
        blockStatus = BlockStatus(storageLevel, 0, diskSize, 0)
        _blocks.put(blockId, blockStatus)
        logInfo("Added %s on disk on %s (size: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(diskSize)))
      }
      if (storageLevel.useOffHeap) {
        blockStatus = BlockStatus(storageLevel, 0, 0, externalBlockStoreSize)
        _blocks.put(blockId, blockStatus)
        logInfo("Added %s on ExternalBlockStore on %s (size: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(externalBlockStoreSize)))
      }
      if (!blockId.isBroadcast && blockStatus.isCached) {
        _cachedBlocks += blockId
      }
    } else if (_blocks.containsKey(blockId)) {//无效,则删除
      // If isValid is not true, drop the block.
      val blockStatus: BlockStatus = _blocks.get(blockId)
      _blocks.remove(blockId)
      _cachedBlocks -= blockId
      if (blockStatus.storageLevel.useMemory) {
        logInfo("Removed %s on %s in memory (size: %s, free: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(blockStatus.memSize),
          Utils.bytesToString(_remainingMem)))
      }
      if (blockStatus.storageLevel.useDisk) {
        logInfo("Removed %s on %s on disk (size: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(blockStatus.diskSize)))
      }
      if (blockStatus.storageLevel.useOffHeap) {
        logInfo("Removed %s on %s on externalBlockStore (size: %s)".format(
          blockId, blockManagerId.hostPort,
          Utils.bytesToString(blockStatus.externalBlockStoreSize)))
      }
    }
  }

  //移除一个数据块,将该driver的内存映射移除
  def removeBlock(blockId: BlockId) {
    if (_blocks.containsKey(blockId)) {
      _remainingMem += _blocks.get(blockId).memSize //还原内存
      _blocks.remove(blockId)
    }
    _cachedBlocks -= blockId
  }

  def remainingMem: Long = _remainingMem

  def lastSeenMs: Long = _lastSeenMs

  def blocks: JHashMap[BlockId, BlockStatus] = _blocks

  // This does not include broadcast blocks.
  def cachedBlocks: collection.Set[BlockId] = _cachedBlocks

  override def toString: String = "BlockManagerInfo " + timeMs + " " + _remainingMem

  def clear() {
    _blocks.clear()
  }
}
