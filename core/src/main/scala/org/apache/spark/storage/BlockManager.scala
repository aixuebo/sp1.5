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

import java.io._
import java.nio.{ByteBuffer, MappedByteBuffer}

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.concurrent.duration._
import scala.util.Random

import sun.nio.ch.DirectBuffer

import org.apache.spark._
import org.apache.spark.executor.{DataReadMethod, ShuffleWriteMetrics}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.network._
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.ExternalShuffleClient
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.serializer.{SerializerInstance, Serializer}
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.shuffle.hash.HashShuffleManager
import org.apache.spark.util._


//要存储的数据块内容,可以是字节数组  可以是一个对象数组  可以是一个对象的迭代器集合
private[spark] sealed trait BlockValues //数据块的类型
private[spark] case class ByteBufferValues(buffer: ByteBuffer) extends BlockValues //用字节存储数据块
private[spark] case class IteratorValues(iterator: Iterator[Any]) extends BlockValues //数据块存储的是一个迭代器集合
private[spark] case class ArrayValues(buffer: Array[Any]) extends BlockValues //数据块存储的是一个数组集合

/* Class for returning a fetched block and associated metrics.
* 表示该数据块的结果集
*/
private[spark] class BlockResult(
    val data: Iterator[Any],//数据块存储的字节内容,已经反序列化后的对象迭代器
    val readMethod: DataReadMethod.Value,//数据如何被读,从内存、磁盘、hdfs、网络
    val bytes: Long) //数据块最终的字节大小

/**
 * Manager running on every node (driver and executors) which provides interfaces for putting and
 * retrieving blocks both locally and remotely into various stores (memory, disk, and off-heap).
 * 运行在每一个node上,每一个driver and executors都有一个该对象,管理属于该driver and executors的数据块信息,包括存储数据块和获取数据块,存储方式内存、磁盘、外部存储都可以
 * Note that #initialize() must be called before the BlockManager is usable.
 * 一个SparkEnv创建一个该对象,运行在node节点上
 */
private[spark] class BlockManager(
    executorId: String,//drver节点上,则该内容是driver,executor暂时未知
    rpcEnv: RpcEnv,//是driver节点或者executor节点的服务器对象RpcEnv
    val master: BlockManagerMaster,//driver和executor通信的客户端。如果是driver节点,则是BlockManagerMaster对象,如果是executor节点,则是driver节点的RpcEndpointRef引用,driver中用于管理每一个execute节点上有哪些数据块以该节点上内存使用情况
    defaultSerializer: Serializer,//如果序列化
    maxMemory: Long,//最大使用多少内存,存储数据块信息
    val conf: SparkConf,
    mapOutputTracker: MapOutputTracker,//如果是driver节点,则是MapOutputTrackerMaster,如果是executor节点,则是MapOutputTrackerWorker
    shuffleManager: ShuffleManager,
    blockTransferService: BlockTransferService,//数据块传输协议,是NettyBlockTransferService或者NioBlockTransferService
    securityManager: SecurityManager,
    numUsableCores: Int)//driver需要多少cpu去执行本地模式.非本地模式都是返回0
  extends BlockDataManager with Logging {

  val diskBlockManager = new DiskBlockManager(this, conf) //磁盘存储管理者

  //key是BlockId,value是BlockInfo和存储该映射关系时候的时间戳,这些数据块是在本地节点存在的数据
  private val blockInfo = new TimeStampedHashMap[BlockId, BlockInfo]

  //创建线程池
  private val futureExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("block-manager-future", 128))

  // Actual storage of where blocks are kept
  private var externalBlockStoreInitialized = false //true表示外部存储器已经初始化完成----因为是懒加载模式启动外部存储,.因此一旦调用了外部存储后,则externalBlockStoreInitialized会设置为true
  private[spark] val memoryStore = new MemoryStore(this, maxMemory) //内存存储
  private[spark] val diskStore = new DiskStore(this, diskBlockManager) //真正意义去执行磁盘存储

  //因为是懒加载模式启动外部存储,.因此一旦调用了外部存储后,则externalBlockStoreInitialized会设置为true
  private[spark] lazy val externalBlockStore: ExternalBlockStore = { //外部存储器,懒加载模式,一般情况下不会被启动
    externalBlockStoreInitialized = true
    new ExternalBlockStore(this, executorId)
  }

  private[spark]
  val externalShuffleServiceEnabled = conf.getBoolean("spark.shuffle.service.enabled", false)

  // Port used by the external shuffle service. In Yarn mode, this may be already be
  // set through the Hadoop configuration as the server is launched in the Yarn NM.
  private val externalShuffleServicePort =
    Utils.getSparkOrYarnConfig(conf, "spark.shuffle.service.port", "7337").toInt

  // Check that we're not using external shuffle service with consolidated shuffle files.
  if (externalShuffleServiceEnabled
      && conf.getBoolean("spark.shuffle.consolidateFiles", false)
      && shuffleManager.isInstanceOf[HashShuffleManager]) {
    throw new UnsupportedOperationException("Cannot use external shuffle service with consolidated"
      + " shuffle files in hash-based shuffle. Please disable spark.shuffle.consolidateFiles or "
      + " switch to sort-based shuffle.")
  }

  var blockManagerId: BlockManagerId = _

  // Address of the server that serves this executor's shuffle files. This is either an external
  // service, or just our own Executor's BlockManager.
  private[spark] var shuffleServerId: BlockManagerId = _

  // Client to read other executors' shuffle files. This is either an external service, or just the
  // standard BlockTransferService to directly connect to other Executors.
  private[spark] val shuffleClient = if (externalShuffleServiceEnabled) {
    val transConf = SparkTransportConf.fromSparkConf(conf, numUsableCores)
    new ExternalShuffleClient(transConf, securityManager, securityManager.isAuthenticationEnabled(),
      securityManager.isSaslEncryptionEnabled())
  } else {
    blockTransferService
  }

  //以下配置信息,决定数据块类型是否要压缩处理
  // Whether to compress broadcast variables that are stored
  private val compressBroadcast = conf.getBoolean("spark.broadcast.compress", true)
  // Whether to compress shuffle output that are stored
  private val compressShuffle = conf.getBoolean("spark.shuffle.compress", true)
  // Whether to compress RDD partitions that are stored serialized
  private val compressRdds = conf.getBoolean("spark.rdd.compress", false)
  // Whether to compress shuffle output temporarily spilled to disk
  private val compressShuffleSpill = conf.getBoolean("spark.shuffle.spill.compress", true)

  
  
  private val slaveEndpoint = rpcEnv.setupEndpoint(
    "BlockManagerEndpoint" + BlockManager.ID_GENERATOR.next,
    new BlockManagerSlaveEndpoint(rpcEnv, this, mapOutputTracker))

  // Pending re-registration action being executed asynchronously or null if none is pending.
  // Accesses should synchronize on asyncReregisterLock.
  private var asyncReregisterTask: Future[Unit] = null
  private val asyncReregisterLock = new Object

  private val metadataCleaner = new MetadataCleaner(
    MetadataCleanerType.BLOCK_MANAGER, this.dropOldNonBroadcastBlocks, conf)
  private val broadcastCleaner = new MetadataCleaner(
    MetadataCleanerType.BROADCAST_VARS, this.dropOldBroadcastBlocks, conf)

  // Field related to peer block managers that are necessary for block replication
  @volatile private var cachedPeers: Seq[BlockManagerId] = _ //返回除了driver和自己之外的  BlockManagerId集合 Seq[BlockManagerId]
  private val peerFetchLock = new Object
  private var lastPeerFetchTime = 0L

  /* The compression codec to use. Note that the "lazy" val is necessary because we want to delay
   * the initialization of the compression codec until it is first used. The reason is that a Spark
   * program could be using a user-defined codec in a third party jar, which is loaded in
   * Executor.updateDependencies. When the BlockManager is initialized, user level jars hasn't been
   * loaded yet. */
  private lazy val compressionCodec: CompressionCodec = CompressionCodec.createCodec(conf)

  /**
   * Construct a BlockManager with a memory limit set based on system properties.
   * 此构造函数的内存是计算出本地节点的内存
   */
  def this(
      execId: String,
      rpcEnv: RpcEnv,
      master: BlockManagerMaster,
      serializer: Serializer,
      conf: SparkConf,
      mapOutputTracker: MapOutputTracker,
      shuffleManager: ShuffleManager,
      blockTransferService: BlockTransferService,
      securityManager: SecurityManager,
      numUsableCores: Int) = {
    this(execId, rpcEnv, master, serializer, BlockManager.getMaxMemory(conf),
      conf, mapOutputTracker, shuffleManager, blockTransferService, securityManager, numUsableCores)
  }

  /**
   * Initializes the BlockManager with the given appId. This is not performed in the constructor as
   * the appId may not be known at BlockManager instantiation time (in particular for the driver,
   * where it is only learned after registration with the TaskScheduler).
   *
   * This method initializes the BlockTransferService and ShuffleClient, registers with the
   * BlockManagerMaster, starts the BlockManagerWorker endpoint, and registers with a local shuffle
   * service if configured.
   * 参数是appid---是sparkContext产生yarn上的applicationId后,初始化该方法的
   */
  def initialize(appId: String): Unit = {
    blockTransferService.init(this)
    shuffleClient.init(appId)

    //产生一个唯一ID
    blockManagerId = BlockManagerId(
      executorId, blockTransferService.hostName, blockTransferService.port)

    shuffleServerId = if (externalShuffleServiceEnabled) {
      BlockManagerId(executorId, blockTransferService.hostName, externalShuffleServicePort)
    } else {
      blockManagerId
    }

    master.registerBlockManager(blockManagerId, maxMemory, slaveEndpoint)//向master发送注册信息

    // Register Executors' configuration with the local shuffle service, if one should exist.
    if (externalShuffleServiceEnabled && !blockManagerId.isDriver) {
      registerWithExternalShuffleServer()
    }
  }

  private def registerWithExternalShuffleServer() {
    logInfo("Registering executor with local external shuffle service.")
    val shuffleConfig = new ExecutorShuffleInfo(
      diskBlockManager.localDirs.map(_.toString),
      diskBlockManager.subDirsPerLocalDir,
      shuffleManager.getClass.getName)

    val MAX_ATTEMPTS = 3//尝试次数
    val SLEEP_TIME_SECS = 5//每次睡眠时间

    for (i <- 1 to MAX_ATTEMPTS) {
      try {
        // Synchronous and will throw an exception if we cannot connect.
        shuffleClient.asInstanceOf[ExternalShuffleClient].registerWithShuffleServer(
          shuffleServerId.host, shuffleServerId.port, shuffleServerId.executorId, shuffleConfig)
        return
      } catch {
        case e: Exception if i < MAX_ATTEMPTS =>
          logError(s"Failed to connect to external shuffle server, will retry ${MAX_ATTEMPTS - i}"
            + s" more times after waiting $SLEEP_TIME_SECS seconds...", e)
          Thread.sleep(SLEEP_TIME_SECS * 1000)
      }
    }
  }

  /**
   * Report all blocks to the BlockManager again. This may be necessary if we are dropped
   * by the BlockManager and come back or if we become capable of recovering blocks on disk after
   * an executor crash.
   *
   * This function deliberately fails silently if the master returns false (indicating that
   * the slave needs to re-register). The error condition will be detected again by the next
   * heart beat attempt or new block registration and another try to re-register all blocks
   * will be made then.
   */
  private def reportAllBlocks(): Unit = {
    logInfo(s"Reporting ${blockInfo.size} blocks to the master.")
    for ((blockId, info) <- blockInfo) {
      val status = getCurrentBlockStatus(blockId, info)
      if (!tryToReportBlockStatus(blockId, info, status)) {//每次上报一个数据块
        logError(s"Failed to report $blockId to master; giving up.")
        return
      }
    }
  }

  /**
   * Re-register with the master and report all blocks to it. This will be called by the heart beat
   * thread if our heartbeat to the block manager indicates that we were not registered.
   *
   * Note that this method must be called without any BlockInfo locks held.
   * 重新注册
   * 当executor的心跳时候会调用该方法
   */
  def reregister(): Unit = {
    // TODO: We might need to rate limit re-registering.
    logInfo("BlockManager re-registering with master")
    master.registerBlockManager(blockManagerId, maxMemory, slaveEndpoint)//注册
    reportAllBlocks()//上报所有的数据块
  }

  /**
   * Re-register with the master sometime soon.
   */
  private def asyncReregister(): Unit = {
    asyncReregisterLock.synchronized {
      if (asyncReregisterTask == null) {
        asyncReregisterTask = Future[Unit] {
          // This is a blocking action and should run in futureExecutionContext which is a cached
          // thread pool
          reregister()
          asyncReregisterLock.synchronized {
            asyncReregisterTask = null
          }
        }(futureExecutionContext)
      }
    }
  }

  /**
   * For testing. Wait for any pending asynchronous re-registration; otherwise, do nothing.
   */
  def waitForAsyncReregister(): Unit = {
    val task = asyncReregisterTask
    if (task != null) {
      Await.ready(task, Duration.Inf)
    }
  }

  /**
   * Interface to get local block data. Throws an exception if the block cannot be found or
   * cannot be read successfully.
   * 获取本地的数据块信息---懒加载模式
   */
  override def getBlockData(blockId: BlockId): ManagedBuffer = {
    if (blockId.isShuffle) {
      shuffleManager.shuffleBlockResolver.getBlockData(blockId.asInstanceOf[ShuffleBlockId])
    } else {
      val blockBytesOpt = doGetLocal(blockId, asBlockResult = false)
        .asInstanceOf[Option[ByteBuffer]]
      if (blockBytesOpt.isDefined) {
        val buffer = blockBytesOpt.get
        new NioManagedBuffer(buffer)
      } else {
        throw new BlockNotFoundException(blockId.toString) //说明本地没有发现该数据块,抛异常
      }
    }
  }

  /**
   * Put the block locally, using the given storage level.
   */
  override def putBlockData(blockId: BlockId, data: ManagedBuffer, level: StorageLevel): Unit = {
    putBytes(blockId, data.nioByteBuffer(), level)
  }

  /**
   * Get the BlockStatus for the block identified by the given ID, if it exists.
   * NOTE: This is mainly for testing, and it doesn't fetch information from external block store.
   * 获取某一个数据块在该节点的状态
   */
  def getStatus(blockId: BlockId): Option[BlockStatus] = {
    blockInfo.get(blockId).map { info =>
      val memSize = if (memoryStore.contains(blockId)) memoryStore.getSize(blockId) else 0L //使用内存多少字节
      val diskSize = if (diskStore.contains(blockId)) diskStore.getSize(blockId) else 0L //使用磁盘多少字节
      // Assume that block is not in external block store
      BlockStatus(info.level, memSize, diskSize, 0L)
    }
  }

  /**
   * Get the ids of existing blocks that match the given filter. Note that this will
   * query the blocks stored in the disk block manager (that the block manager
   * may not know of).
   * 参数是一个函数,函数的参数是数据块ID,返回值是boolean
   *
   * 获取匹配的数据块集合
   */
  def getMatchingBlockIds(filter: BlockId => Boolean): Seq[BlockId] = {
    (blockInfo.keys ++ diskBlockManager.getAllBlocks()).filter(filter).toSeq
  }

  /**
   * Tell the master about the current storage status of a block. This will send a block update
   * message reflecting the current status, *not* the desired storage level in its block info.
   * For example, a block with MEMORY_AND_DISK set might have fallen out to be only on disk.
   *
   * droppedMemorySize exists to account for when the block is dropped from memory to disk (so
   * it is still valid). This ensures that update in master will compensate for the increase in
   * memory on slave.
   */
  private def reportBlockStatus(
      blockId: BlockId,
      info: BlockInfo,
      status: BlockStatus,
      droppedMemorySize: Long = 0L): Unit = {//释放多少内存
    val needReregister = !tryToReportBlockStatus(blockId, info, status, droppedMemorySize)
    if (needReregister) {
      logInfo(s"Got told to re-register updating block $blockId")
      // Re-registering will report our new block for free.
      asyncReregister()
    }
    logDebug(s"Told master about block $blockId")
  }

  /**
   * Actually send a UpdateBlockInfo message. Returns the master's response,
   * which will be true if the block was successfully recorded and false if
   * the slave needs to re-register.
   * 上报给master该数据块的信息内容
   * true表示上报成功
   */
  private def tryToReportBlockStatus(
      blockId: BlockId,
      info: BlockInfo,
      status: BlockStatus,
      droppedMemorySize: Long = 0L): Boolean = {
    if (info.tellMaster) {
      val storageLevel = status.storageLevel
      val inMemSize = Math.max(status.memSize, droppedMemorySize)
      val inExternalBlockStoreSize = status.externalBlockStoreSize
      val onDiskSize = status.diskSize

      //向master报告一个数据块内容
      master.updateBlockInfo(
        blockManagerId, blockId, storageLevel, inMemSize, onDiskSize, inExternalBlockStoreSize)//发送master信息
    } else {
      true
    }
  }

  /**
   * Return the updated storage status of the block with the given ID. More specifically, if
   * the block is dropped from memory and possibly added to disk, return the new storage level
   * and the updated in-memory and on-disk sizes.
   * 获取当前数据块在本地的状态信息
   */
  private def getCurrentBlockStatus(blockId: BlockId, info: BlockInfo): BlockStatus = {
    info.synchronized {
      info.level match {
        case null =>
          BlockStatus(StorageLevel.NONE, 0L, 0L, 0L)
        case level =>
          val inMem = level.useMemory && memoryStore.contains(blockId)
          val inExternalBlockStore = level.useOffHeap && externalBlockStore.contains(blockId)
          val onDisk = level.useDisk && diskStore.contains(blockId)
          val deserialized = if (inMem) level.deserialized else false
          val replication = if (inMem || inExternalBlockStore || onDisk) level.replication else 1 //备份次数
          val storageLevel =
            StorageLevel(onDisk, inMem, inExternalBlockStore, deserialized, replication) //存储级别
          val memSize = if (inMem) memoryStore.getSize(blockId) else 0L //使用内存数量
          val externalBlockStoreSize =
            if (inExternalBlockStore) externalBlockStore.getSize(blockId) else 0L
          val diskSize = if (onDisk) diskStore.getSize(blockId) else 0L
          BlockStatus(storageLevel, memSize, diskSize, externalBlockStoreSize)
      }
    }
  }

  /**
   * Get locations of an array of blocks.
   * 获取数据块所在位置集合
   */
  private def getLocationBlockIds(blockIds: Array[BlockId]): Array[Seq[BlockManagerId]] = {
    val startTimeMs = System.currentTimeMillis
    val locations = master.getLocations(blockIds).toArray
    logDebug("Got multiple block location in %s".format(Utils.getUsedTimeMs(startTimeMs)))
    locations
  }

  /**
   * Get block from local block manager.
   * 从本地获取该数据块所对应的结果集
   */
  def getLocal(blockId: BlockId): Option[BlockResult] = {
    logDebug(s"Getting local block $blockId")
    doGetLocal(blockId, asBlockResult = true).asInstanceOf[Option[BlockResult]]
  }

  /**
   * Get block from the local block manager as serialized bytes.
   * 从本地获取数据块对应的字节数组
   */
  def getLocalBytes(blockId: BlockId): Option[ByteBuffer] = {
    logDebug(s"Getting local block $blockId as bytes")
    // As an optimization for map output fetches, if the block is for a shuffle, return it
    // without acquiring a lock; the disk store never deletes (recent) items so this should work
    if (blockId.isShuffle) {
      val shuffleBlockResolver = shuffleManager.shuffleBlockResolver
      // TODO: This should gracefully handle case where local block is not available. Currently
      // downstream code will throw an exception.
      Option(
        shuffleBlockResolver.getBlockData(blockId.asInstanceOf[ShuffleBlockId]).nioByteBuffer())
    } else {
      doGetLocal(blockId, asBlockResult = false).asInstanceOf[Option[ByteBuffer]]
    }
  }

  //获取本地节点上的一个数据块
  private def doGetLocal(blockId: BlockId, asBlockResult: Boolean): Option[Any] = {
    val info = blockInfo.get(blockId).orNull //判断本地节点上是否有该数据块
    if (info != null) {//说明本地是知道该数据块的
      info.synchronized {
        // Double check to make sure the block is still there. There is a small chance that the
        // block has been removed by removeBlock (which also synchronizes on the blockInfo object).
        //双重校验确保该数据块仍然存在,这是一个很小的概率出现该数据块被删除
        // Note that this only checks metadata tracking. If user intentionally deleted the block
        // on disk or from off heap storage without using removeBlock, this conditional check will
        // still pass but eventually we will get an exception because we can't find the block.
        //注意 此时校验仅仅是校验内存的元数据,如果user故意在磁盘上删除这个数据块,或者从内存中没有使用removeBlock,则此时校验仍然会通过,但是最终我们会抛异常,因为我们找不到该数据块
        if (blockInfo.get(blockId).isEmpty) {//说明数据块不存在
          logWarning(s"Block $blockId had been removed")
          return None
        }

        // If another thread is writing the block, wait for it to become ready.
        if (!info.waitForReady()) {//说明该数据块不可用,该方法阻塞,一直会等到是否可用该数据块为止
          // If we get here, the block write failed.
          logWarning(s"Block $blockId was marked as failure.")//说明该数据块出现问题了
          return None
        }

        val level = info.level //该数据块本地存储级别
        logDebug(s"Level for block $blockId is $level")

        // Look for the block in memory
        if (level.useMemory) {//内存存储
          logDebug(s"Getting block $blockId from memory")
          val result = if (asBlockResult) {
            memoryStore.getValues(blockId).map(new BlockResult(_, DataReadMethod.Memory, info.size))
          } else {
            memoryStore.getBytes(blockId)
          }
          result match {
            case Some(values) =>
              return result
            case None =>
              logDebug(s"Block $blockId not found in memory")
          }
        }

        // Look for the block in external block store
        if (level.useOffHeap) {//使用堆外内存
          logDebug(s"Getting block $blockId from ExternalBlockStore")
          if (externalBlockStore.contains(blockId)) {
            val result = if (asBlockResult) {
              externalBlockStore.getValues(blockId)
                .map(new BlockResult(_, DataReadMethod.Memory, info.size))
            } else {
              externalBlockStore.getBytes(blockId)
            }
            result match {
              case Some(values) =>
                return result
              case None =>
                logDebug(s"Block $blockId not found in ExternalBlockStore")
            }
          }
        }

        // Look for block on disk, potentially storing it back in memory if required
        //在磁盘上获取该数据块,并且潜在的存储到内存中(如果需要的话)
        if (level.useDisk) {//使用磁盘
          logDebug(s"Getting block $blockId from disk")
          val bytes: ByteBuffer = diskStore.getBytes(blockId) match {
            case Some(b) => b
            case None =>
              throw new BlockException(
                blockId, s"Block $blockId not found on disk, though it should be")
          }
          assert(0 == bytes.position())

          if (!level.useMemory) {//如果数据块不需要存储到内存,则我们仅仅返回即可
            // If the block shouldn't be stored in memory, we can just return it
            if (asBlockResult) {
              return Some(new BlockResult(dataDeserialize(blockId, bytes), DataReadMethod.Disk,
                info.size))
            } else {
              return Some(bytes)
            }
          } else {//说明需要内存去缓存这个数据块
            // Otherwise, we also have to store something in the memory store
            if (!level.deserialized || !asBlockResult) {
              /* We'll store the bytes in memory if the block's storage level includes
               * "memory serialized", or if it should be cached as objects in memory
               * but we only requested its serialized bytes. */
              memoryStore.putBytes(blockId, bytes.limit, () => {
                // https://issues.apache.org/jira/browse/SPARK-6076
                // If the file size is bigger than the free memory, OOM will happen. So if we cannot
                // put it into MemoryStore, copyForMemory should not be created. That's why this
                // action is put into a `() => ByteBuffer` and created lazily.
                val copyForMemory = ByteBuffer.allocate(bytes.limit)
                copyForMemory.put(bytes)
              })
              bytes.rewind()
            }
            if (!asBlockResult) {//如果不是结果集,则直接返回字节
              return Some(bytes)
            } else {//产生数据块结果
              val values = dataDeserialize(blockId, bytes)
              if (level.deserialized) {
                // Cache the values before returning them
                val putResult = memoryStore.putIterator(
                  blockId, values, level, returnValues = true, allowPersistToDisk = false)
                // The put may or may not have succeeded, depending on whether there was enough
                // space to unroll the block. Either way, the put here should return an iterator.
                putResult.data match {
                  case Left(it) =>
                    return Some(new BlockResult(it, DataReadMethod.Disk, info.size))
                  case _ =>
                    // This only happens if we dropped the values back to disk (which is never)
                    throw new SparkException("Memory store did not return an iterator!")
                }
              } else {
                return Some(new BlockResult(values, DataReadMethod.Disk, info.size))
              }
            }
          }
        }
      }
    } else {
      logDebug(s"Block $blockId not registered locally") //说明该数据块没有注册在本地节点上
    }
    None
  }

  /**
   * Get block from remote block managers.
   */
  def getRemote(blockId: BlockId): Option[BlockResult] = {
    logDebug(s"Getting remote block $blockId")
    doGetRemote(blockId, asBlockResult = true).asInstanceOf[Option[BlockResult]]
  }

  /**
   * Get block from remote block managers as serialized bytes.
   */
  def getRemoteBytes(blockId: BlockId): Option[ByteBuffer] = {
    logDebug(s"Getting remote block $blockId as bytes")
    doGetRemote(blockId, asBlockResult = false).asInstanceOf[Option[ByteBuffer]]
  }

  //获取远程节点上对应该数据块的字节内容
  private def doGetRemote(blockId: BlockId, asBlockResult: Boolean): Option[Any] = {
    require(blockId != null, "BlockId is null")
    val locations = Random.shuffle(master.getLocations(blockId)) //返回一个BlockManagerId集合,这行代码从masert中获取该数据块所在节点集合,然后将集合的顺序打乱
    for (loc <- locations) {//循环每一个数据块位置
      logDebug(s"Getting remote block $blockId from $loc")
      val data = blockTransferService.fetchBlockSync(
        loc.host, loc.port, loc.executorId, blockId.toString).nioByteBuffer() //去远程host port机器抓去属于executor的具体的某一个数据块

      if (data != null) {
        if (asBlockResult) {
          return Some(new BlockResult(
            dataDeserialize(blockId, data),//对数据内容反序列化
            DataReadMethod.Network,
            data.limit()))
        } else {
          return Some(data)
        }
      }
      logDebug(s"The value of block $blockId is null")
    }
    logDebug(s"Block $blockId not found")
    None
  }

  /**
   * Get a block from the block manager (either local or remote).
   * 获取一个数据块,先从本地获取,再从远程获取
   */
  def get(blockId: BlockId): Option[BlockResult] = {
    val local = getLocal(blockId)
    if (local.isDefined) {
      logInfo(s"Found block $blockId locally")
      return local
    }
    val remote = getRemote(blockId)
    if (remote.isDefined) {
      logInfo(s"Found block $blockId remotely")
      return remote
    }
    None
  }

  def putIterator(
      blockId: BlockId,
      values: Iterator[Any],
      level: StorageLevel,
      tellMaster: Boolean = true,
      effectiveStorageLevel: Option[StorageLevel] = None): Seq[(BlockId, BlockStatus)] = {
    require(values != null, "Values is null")
    doPut(blockId, IteratorValues(values), level, tellMaster, effectiveStorageLevel)
  }

  /**
   * A short circuited method to get a block writer that can write data directly to disk.
   * The Block will be appended to the File specified by filename. Callers should handle error
   * cases.
   */
  def getDiskWriter(
      blockId: BlockId,
      file: File,
      serializerInstance: SerializerInstance,
      bufferSize: Int,
      writeMetrics: ShuffleWriteMetrics): DiskBlockObjectWriter = {
    val compressStream: OutputStream => OutputStream = wrapForCompression(blockId, _)
    val syncWrites = conf.getBoolean("spark.shuffle.sync", false)
    new DiskBlockObjectWriter(blockId, file, serializerInstance, bufferSize, compressStream,
      syncWrites, writeMetrics)
  }

  /**
   * Put a new block of values to the block manager.
   * Return a list of blocks updated as a result of this put.
   */
  def putArray(
      blockId: BlockId,
      values: Array[Any],
      level: StorageLevel,
      tellMaster: Boolean = true,
      effectiveStorageLevel: Option[StorageLevel] = None): Seq[(BlockId, BlockStatus)] = {
    require(values != null, "Values is null")
    doPut(blockId, ArrayValues(values), level, tellMaster, effectiveStorageLevel)
  }

  /**
   * Put a new block of serialized bytes to the block manager.
   * Return a list of blocks updated as a result of this put.
   */
  def putBytes(
      blockId: BlockId,
      bytes: ByteBuffer,
      level: StorageLevel,
      tellMaster: Boolean = true,
      effectiveStorageLevel: Option[StorageLevel] = None): Seq[(BlockId, BlockStatus)] = {
    require(bytes != null, "Bytes is null")
    doPut(blockId, ByteBufferValues(bytes), level, tellMaster, effectiveStorageLevel)
  }

  /**
   * Put the given block according to the given level in one of the block stores, replicating
   * the values if necessary.
   *
   * The effective storage level refers to the level according to which the block will actually be
   * handled. This allows the caller to specify an alternate behavior of doPut while preserving
   * the original level specified by the user.
   * 存储一组数据信息,返回存储后的数据块ID以及存储后的状态信息集合
   */
  private def doPut(
      blockId: BlockId,//要存储的数据块ID
      data: BlockValues,//要存储的数据块内容,可以是字节数组  可以是一个对象数组  可以是一个对象的迭代器集合
      level: StorageLevel,//要存储的级别
      tellMaster: Boolean = true,
      effectiveStorageLevel: Option[StorageLevel] = None)
    : Seq[(BlockId, BlockStatus)] = {

    require(blockId != null, "BlockId is null")
    require(level != null && level.isValid, "StorageLevel is null or invalid")
    effectiveStorageLevel.foreach { level =>
      require(level != null && level.isValid, "Effective StorageLevel is null or invalid")
    }

    // Return value 返回值
    val updatedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]

    /* Remember the block's storage level so that we can correctly drop it to disk if it needs
     * to be dropped right after it got put into memory. Note, however, that other threads will
     * not be able to get() this block until we call markReady on its BlockInfo. */
     //定义一个函数---返回该数据块的信息
     val putBlockInfo = {
      val tinfo = new BlockInfo(level, tellMaster)
      // Do atomically !
      val oldBlockOpt = blockInfo.putIfAbsent(blockId, tinfo) //获取已经存在的对象
      if (oldBlockOpt.isDefined) {//如果已经存在
        if (oldBlockOpt.get.waitForReady()) {//阻塞,一直拿到结果
          logWarning(s"Block $blockId already exists on this machine; not re-adding it") //说明该数据块已经存在了,不能再次添加,因此返回数据
          return updatedBlocks
        }
        // TODO: So the block info exists - but previous attempt to load it (?) failed.
        // What do we do now ? Retry on it ?
        oldBlockOpt.get
      } else {
        tinfo
      }
    }

    val startTimeMs = System.currentTimeMillis

    /* If we're storing values and we need to replicate the data, we'll want access to the values,
     * but because our put will read the whole iterator, there will be no values left. For the
     * case where the put serializes data, we'll remember the bytes, above; but for the case where
     * it doesn't, such as deserialized storage, let's rely on the put returning an Iterator. */
    var valuesAfterPut: Iterator[Any] = null

    // Ditto for the bytes after the put
    var bytesAfterPut: ByteBuffer = null

    // Size of the block in bytes
    var size = 0L

    // The level we actually use to put the block
    val putLevel = effectiveStorageLevel.getOrElse(level)

    // If we're storing bytes, then initiate the replication before storing them locally.
    // This is faster as data is already serialized and ready to send.
    val replicationFuture = data match {
        //复制到多个节点上
      case b: ByteBufferValues if putLevel.replication > 1 =>
        // Duplicate doesn't copy the bytes, but just creates a wrapper
        val bufferView = b.buffer.duplicate() //复制要copy的字节内容
        Future {
          // This is a blocking action and should run in futureExecutionContext which is a cached
          // thread pool
          replicate(blockId, bufferView, putLevel)
        }(futureExecutionContext)
      case _ => null
    }

    putBlockInfo.synchronized {
      logTrace("Put for block %s took %s to get into synchronized block"
        .format(blockId, Utils.getUsedTimeMs(startTimeMs)))

      var marked = false
      try {
        // returnValues - Whether to return the values put
        // blockStore - The type of storage to put these values into
        //返回是否有结果,以及采用什么存储器存储该数据块信息
        val (returnValues, blockStore: BlockStore) = {
          if (putLevel.useMemory) {
            // Put it in memory first, even if it also has useDisk set to true;
            // We will drop it to disk later if the memory store can't hold it.
            (true, memoryStore)
          } else if (putLevel.useOffHeap) {
            // Use external block store
            (false, externalBlockStore)
          } else if (putLevel.useDisk) {
            // Don't get back the bytes from put unless we replicate them
            (putLevel.replication > 1, diskStore) //putLevel.replication > 1 结果是否是true,表示是否需要返回值,如果是1,即存储后不需要返回信息给接下来的备份节点,因此就是false
          } else {
            assert(putLevel == StorageLevel.NONE)
            throw new BlockException(
              blockId, s"Attempted to put block $blockId without specifying storage level!")
          }
        }

        // Actually put the values 本地存储器存储这些数据
        val result = data match {
          case IteratorValues(iterator) =>
            blockStore.putIterator(blockId, iterator, putLevel, returnValues)
          case ArrayValues(array) =>
            blockStore.putArray(blockId, array, putLevel, returnValues)
          case ByteBufferValues(bytes) =>
            bytes.rewind()
            blockStore.putBytes(blockId, bytes, putLevel)
        }
        size = result.size
        result.data match {
          case Left (newIterator) if putLevel.useMemory => valuesAfterPut = newIterator //说明返回值是迭代器
          case Right (newBytes) => bytesAfterPut = newBytes //说明返回结果是字节数组
          case _ =>
        }

        // Keep track of which blocks are dropped from memory
        if (putLevel.useMemory) {
          result.droppedBlocks.foreach { updatedBlocks += _ } //向updatedBlocks集合内添加数据
        }

        val putBlockStatus = getCurrentBlockStatus(blockId, putBlockInfo)
        if (putBlockStatus.storageLevel != StorageLevel.NONE) {
          // Now that the block is in either the memory, externalBlockStore, or disk store,
          // let other threads read it, and tell the master about it.
          marked = true
          putBlockInfo.markReady(size)
          if (tellMaster) {
            reportBlockStatus(blockId, putBlockInfo, putBlockStatus)
          }
          updatedBlocks += ((blockId, putBlockStatus))
        }
      } finally {
        // If we failed in putting the block to memory/disk, notify other possible readers
        // that it has failed, and then remove it from the block info map.
        if (!marked) {
          // Note that the remove must happen before markFailure otherwise another thread
          // could've inserted a new BlockInfo before we remove it.
          blockInfo.remove(blockId)
          putBlockInfo.markFailure()
          logWarning(s"Putting block $blockId failed")
        }
      }
    }
    logDebug("Put block %s locally took %s".format(blockId, Utils.getUsedTimeMs(startTimeMs)))

    // Either we're storing bytes and we asynchronously started replication, or we're storing
    // values and need to serialize and replicate them now:
    if (putLevel.replication > 1) {
      data match {
        case ByteBufferValues(bytes) =>
          if (replicationFuture != null) {
            Await.ready(replicationFuture, Duration.Inf)
          }
        case _ =>
          val remoteStartTime = System.currentTimeMillis
          // Serialize the block if not already done //首先序列化成字节数组
          if (bytesAfterPut == null) {
            if (valuesAfterPut == null) {
              throw new SparkException(
                "Underlying put returned neither an Iterator nor bytes! This shouldn't happen.")
            }
            bytesAfterPut = dataSerialize(blockId, valuesAfterPut)
          }
          replicate(blockId, bytesAfterPut, putLevel)
          logDebug("Put block %s remotely took %s"
            .format(blockId, Utils.getUsedTimeMs(remoteStartTime)))
      }
    }
    //销毁字节数组
    BlockManager.dispose(bytesAfterPut)

    if (putLevel.replication > 1) {
      logDebug("Putting block %s with replication took %s"
        .format(blockId, Utils.getUsedTimeMs(startTimeMs)))
    } else {
      logDebug("Putting block %s without replication took %s"
        .format(blockId, Utils.getUsedTimeMs(startTimeMs)))
    }

    updatedBlocks
  }

  /**
   * Get peer block managers in the system.
   * 获取该数据块都分布在哪些节点上了
   */
  private def getPeers(forceFetch: Boolean): Seq[BlockManagerId] = {
    peerFetchLock.synchronized {
      val cachedPeersTtl = conf.getInt("spark.storage.cachedPeersTtl", 60 * 1000) // milliseconds
      val timeout = System.currentTimeMillis - lastPeerFetchTime > cachedPeersTtl
      if (cachedPeers == null || forceFetch || timeout) {
        cachedPeers = master.getPeers(blockManagerId).sortBy(_.hashCode) //返回除了driver和自己之外的  BlockManagerId集合 Seq[BlockManagerId]
        lastPeerFetchTime = System.currentTimeMillis
        logDebug("Fetched peers from master: " + cachedPeers.mkString("[", ",", "]"))
      }
      cachedPeers
    }
  }

  /**
   * Replicate block to another node. Not that this is a blocking call that returns after
   * the block has been replicated.
   * 复制数据块到其他节点,
   * 注意 这个是一个阻塞的调用,当数据块被分布到其他节点后,才会被返回
   */
  private def replicate(blockId: BlockId,//要存储哪个数据块
                        data: ByteBuffer,//数据块内容,这个内容是字节数组
                        level: StorageLevel): Unit = {//存储级别
    val maxReplicationFailures = conf.getInt("spark.storage.maxReplicationFailures", 1) //最大失败次数
    val numPeersToReplicateTo = level.replication - 1 //需要多少个备份,因为本节点备份一个了,因此是总数-1

    val peersForReplication = new ArrayBuffer[BlockManagerId] //在哪些节点上选择要复制的节点

    val peersReplicatedTo = new ArrayBuffer[BlockManagerId] //成功复制到远程的节点集合
    val peersFailedToReplicateTo = new ArrayBuffer[BlockManagerId]//复制过程中遇到失败的节点集合

    val tLevel = StorageLevel(
      level.useDisk, level.useMemory, level.useOffHeap, level.deserialized, 1)

    val startTime = System.currentTimeMillis
    val random = new Random(blockId.hashCode)

    var replicationFailed = false //是否上一次是节点有失败
    var failures = 0 //已经失败次数
    var done = false //true表示复制备份数量完成

    // Get cached list of peers 获取缓存的节点集合,这些节点都存储了相关联的数据块
    peersForReplication ++= getPeers(forceFetch = false)

    // Get a random peer. Note that this selection of a peer is deterministic on the block id.
    // So assuming the list of peers does not change and no replication failures,
    // if there are multiple attempts in the same node to replicate the same block,
    // the same set of peers will be selected.
    //随机选择一个要复制的节点
    def getRandomPeer(): Option[BlockManagerId] = {
      // If replication had failed, then force update the cached list of peers and remove the peers
      // that have been already used
      if (replicationFailed) {
        peersForReplication.clear()
        peersForReplication ++= getPeers(forceFetch = true) //重新抓去节点集合
        peersForReplication --= peersReplicatedTo //刨除已经成功复制的节点集合
        peersForReplication --= peersFailedToReplicateTo //刨除已经失败的节点集合
      }
      if (!peersForReplication.isEmpty) {//如果节点不是空
        Some(peersForReplication(random.nextInt(peersForReplication.size))) //随机选择一个节点
      } else {
        None
      }
    }

    // One by one choose a random peer and try uploading the block to it
    // If replication fails (e.g., target peer is down), force the list of cached peers
    // to be re-fetched from driver and then pick another random peer for replication. Also
    // temporarily black list the peer for which replication failed.
    //
    // This selection of a peer and replication is continued in a loop until one of the
    // following 3 conditions is fulfilled: 完成一下三个条件之一,则会停止循环
    // (i) specified number of peers have been replicated to 成功完成指定复制数量
    // (ii) too many failures in replicating to peers 太多复制失败的可能了
    // (iii) no peer left to replicate to 没有多余的节点可以被复制了
    //
    while (!done) {
      getRandomPeer() match {
        case Some(peer) =>
          try {//存在一个BlockManagerId节点
            val onePeerStartTime = System.currentTimeMillis
            data.rewind()
            logTrace(s"Trying to replicate $blockId of ${data.limit()} bytes to $peer")
            blockTransferService.uploadBlockSync(
              peer.host, peer.port, peer.executorId, blockId, new NioManagedBuffer(data), tLevel) //向该节点提交一个数据块,以及数据块内容
            logTrace(s"Replicated $blockId of ${data.limit()} bytes to $peer in %s ms"
              .format(System.currentTimeMillis - onePeerStartTime))
            peersReplicatedTo += peer
            peersForReplication -= peer //在总集合中减少该节点,因为该节点已经存储成功了一个数据块,不能存储多个相同的数据块
            replicationFailed = false
            if (peersReplicatedTo.size == numPeersToReplicateTo) {//说明复制完成
              done = true  // specified number of peers have been replicated to
            }
          } catch {
            case e: Exception =>
              logWarning(s"Failed to replicate $blockId to $peer, failure #$failures", e)
              failures += 1
              replicationFailed = true
              peersFailedToReplicateTo += peer
              if (failures > maxReplicationFailures) { // too many failures in replcating to peers
                done = true //超过最大失败次数.也会完成
              }
          }
        case None => // no peer left to replicate to
          done = true
      }
    }
    val timeTakeMs = (System.currentTimeMillis - startTime)
    logDebug(s"Replicating $blockId of ${data.limit()} bytes to " +
      s"${peersReplicatedTo.size} peer(s) took $timeTakeMs ms")
    if (peersReplicatedTo.size < numPeersToReplicateTo) {//说明没复制完,因此打印日志
      logWarning(s"Block $blockId replicated to only " +
        s"${peersReplicatedTo.size} peer(s) instead of $numPeersToReplicateTo peers")
    }
  }

  /**
   * Read a block consisting of a single object.
   * 因此该数据块只有一个对象,因此直接通过数据集合中next方法获取第一个对象即可
   */
  def getSingle(blockId: BlockId): Option[Any] = {
    get(blockId).map(_.data.next())
  }

  /**
   * Write a block consisting of a single object.
   * 仅仅存放一个对象
   */
  def putSingle(
      blockId: BlockId,
      value: Any,
      level: StorageLevel,
      tellMaster: Boolean = true): Seq[(BlockId, BlockStatus)] = {
    putIterator(blockId, Iterator(value), level, tellMaster)
  }

  //释放该数据块ID与之对应的数据内容,返回数据块的状态
  def dropFromMemory(
      blockId: BlockId,
      data: Either[Array[Any], ByteBuffer]): Option[BlockStatus] = {//释放的内容可能是对象,也可能是字节数组
    dropFromMemory(blockId, () => data)
  }

  /**
   * Drop a block from memory, possibly putting it on disk if applicable. Called when the memory
   * store reaches its limit and needs to free up space.
   *
   * If `data` is not put on disk, it won't be created.
   *
   * Return the block status if the given block has been updated, else None.
   */
  def dropFromMemory(
      blockId: BlockId,
      data: () => Either[Array[Any], ByteBuffer]): Option[BlockStatus] = {

    logInfo(s"Dropping block $blockId from memory")
    val info = blockInfo.get(blockId).orNull

    // If the block has not already been dropped
    if (info != null) {
      info.synchronized {
        // required ? As of now, this will be invoked only for blocks which are ready
        // But in case this changes in future, adding for consistency sake.
        if (!info.waitForReady()) {//等待后,依然数据块不存在
          // If we get here, the block write failed.
          logWarning(s"Block $blockId was marked as failure. Nothing to drop")
          return None
        } else if (blockInfo.get(blockId).isEmpty) {//数据块已经不存在了
          logWarning(s"Block $blockId was already dropped.")
          return None
        }
        var blockIsUpdated = false //true表示数据块被删除了 或者数据块存储到磁盘上了,总之是发生过改动了
        val level = info.level

        // Drop to disk, if storage level requires 如果需要的话,则存放到磁盘中
        if (level.useDisk && !diskStore.contains(blockId)) {
          logInfo(s"Writing block $blockId to disk")
          data() match {
            case Left(elements) =>
              diskStore.putArray(blockId, elements, level, returnValues = false)
            case Right(bytes) =>
              diskStore.putBytes(blockId, bytes, level)
          }
          blockIsUpdated = true
        }

        // Actually drop from memory store
        //要删除的数据块大小
        val droppedMemorySize =
          if (memoryStore.contains(blockId)) memoryStore.getSize(blockId) else 0L
        val blockIsRemoved = memoryStore.remove(blockId)//删除该数据块
        if (blockIsRemoved) {
          blockIsUpdated = true
        } else {
          logWarning(s"Block $blockId could not be dropped from memory as it does not exist")
        }

        val status = getCurrentBlockStatus(blockId, info)
        if (info.tellMaster) {
          reportBlockStatus(blockId, info, status, droppedMemorySize)
        }
        if (!level.useDisk) {
          // The block is completely gone from this node; forget it so we can put() it again later.
          blockInfo.remove(blockId)
        }
        if (blockIsUpdated) {//说明已经删除,返回该数据块状态
          return Some(status)
        }
      }
    }
    None
  }

  /**
   * Remove all blocks belonging to the given RDD.
   * @return The number of blocks removed.
   * 返回移除多少个数据块
   * 因为一个RDD可以有多个partition
   */
  def removeRdd(rddId: Int): Int = {
    // TODO: Avoid a linear scan by creating another mapping of RDD.id to blocks.
    logInfo(s"Removing RDD $rddId")
    val blocksToRemove = blockInfo.keys.flatMap(_.asRDDId).filter(_.rddId == rddId) //过滤找到属于该rdd的所有partition集合
    blocksToRemove.foreach { blockId => removeBlock(blockId, tellMaster = false) } //依次删除
    blocksToRemove.size
  }

  /**
   * Remove all blocks belonging to the given broadcast.
   * 返回移除多少个数据块
   */
  def removeBroadcast(broadcastId: Long, tellMaster: Boolean): Int = {
    logDebug(s"Removing broadcast $broadcastId")
    val blocksToRemove = blockInfo.keys.collect {
      case bid @ BroadcastBlockId(`broadcastId`, _) => bid //找到broadcastId开头的数据块集合
    }
    blocksToRemove.foreach { blockId => removeBlock(blockId, tellMaster) }//循环删除符合的数据块
    blocksToRemove.size
  }

  /**
   * Remove a block from both memory and disk.
   * 移除一个数据块
   */
  def removeBlock(blockId: BlockId, tellMaster: Boolean = true): Unit = {
    logDebug(s"Removing block $blockId")
    val info = blockInfo.get(blockId).orNull
    if (info != null) {
      info.synchronized {
        // Removals are idempotent in disk store and memory store. At worst, we get a warning.
        val removedFromMemory = memoryStore.remove(blockId)
        val removedFromDisk = diskStore.remove(blockId)
        val removedFromExternalBlockStore =
        if (externalBlockStoreInitialized) externalBlockStore.remove(blockId) else false
        if (!removedFromMemory && !removedFromDisk && !removedFromExternalBlockStore) {//说明该数据块都没有被移除
          logWarning(s"Block $blockId could not be removed as it was not found in either " +
            "the disk, memory, or external block store")
        }
        blockInfo.remove(blockId)
        if (tellMaster && info.tellMaster) {
          val status = getCurrentBlockStatus(blockId, info)
          reportBlockStatus(blockId, info, status)
        }
      }
    } else {
      // The block has already been removed; do nothing.
      logWarning(s"Asked to remove block $blockId, which does not exist")
    }
  }

  //删除非广播数据块
  private def dropOldNonBroadcastBlocks(cleanupTime: Long): Unit = {
    logInfo(s"Dropping non broadcast blocks older than $cleanupTime")
    dropOldBlocks(cleanupTime, !_.isBroadcast)
  }

  //删除广播数据块
  private def dropOldBroadcastBlocks(cleanupTime: Long): Unit = {
    logInfo(s"Dropping broadcast blocks older than $cleanupTime")
    dropOldBlocks(cleanupTime, _.isBroadcast) //因为调用了dropOldBlocks方法,而该方法的参数是已知的,因此第二个参数的_就代表BlockId,返回值是boolean类型的,因此_.isBroadcast就是返回值是boolean类型的
  }

  //删除老的数据块
  private def dropOldBlocks(cleanupTime: Long, shouldDrop: (BlockId => Boolean)): Unit = {
    val iterator = blockInfo.getEntrySet.iterator //循环本地的数据块
    while (iterator.hasNext) {
      val entry = iterator.next()
      val (id, info, time) = (entry.getKey, entry.getValue.value, entry.getValue.timestamp) //key，value，以及时间戳
      if (time < cleanupTime && shouldDrop(id)) {//时间time比cleanupTime小,说明在cleanupTime之前就已经存在该value了,因此是要被删除的
        info.synchronized {
          val level = info.level
          if (level.useMemory) { memoryStore.remove(id) }
          if (level.useDisk) { diskStore.remove(id) }
          if (level.useOffHeap) { externalBlockStore.remove(id) }
          iterator.remove()
          logInfo(s"Dropped block $id")
        }
        val status = getCurrentBlockStatus(id, info)
        reportBlockStatus(id, info, status)
      }
    }
  }

  //true表示该数据块要进行压缩
  private def shouldCompress(blockId: BlockId): Boolean = {
    blockId match {
      case _: ShuffleBlockId => compressShuffle
      case _: BroadcastBlockId => compressBroadcast
      case _: RDDBlockId => compressRdds
      case _: TempLocalBlockId => compressShuffleSpill
      case _: TempShuffleBlockId => compressShuffle
      case _ => false
    }
  }

  /**
   * Wrap an output stream for compression if block compression is enabled for its block type
   * 如果需要压缩的话,对数据块进行压缩包装,返回压缩后的输出流
   */
  def wrapForCompression(blockId: BlockId, s: OutputStream): OutputStream = {
    if (shouldCompress(blockId)) compressionCodec.compressedOutputStream(s) else s
  }

  /**
   * Wrap an input stream for compression if block compression is enabled for its block type
   *  如果需要压缩的话,对数据块进行压缩包装,返回压缩后的输入流
   */
  def wrapForCompression(blockId: BlockId, s: InputStream): InputStream = {
    if (shouldCompress(blockId)) compressionCodec.compressedInputStream(s) else s
  }

  /** Serializes into a stream.
   *  序列化数据到输出流中,该输出流是blockId的输出流
   **/
  def dataSerializeStream(
      blockId: BlockId,//该输出流属于哪个数据块的
      outputStream: OutputStream,//写入的输出流
      values: Iterator[Any],//要向输出流中写入的内容数组迭代器
      serializer: Serializer = defaultSerializer): Unit = {//输出过程中如何序列化
    val byteStream = new BufferedOutputStream(outputStream)
    val ser = serializer.newInstance()
    ser.serializeStream(wrapForCompression(blockId, byteStream)).writeAll(values).close()
  }

  /** Serializes into a byte buffer.序列化到ByteBuffer中 */
  def dataSerialize(
      blockId: BlockId,
      values: Iterator[Any],
      serializer: Serializer = defaultSerializer): ByteBuffer = {
    val byteStream = new ByteArrayOutputStream(4096)
    dataSerializeStream(blockId, byteStream, values, serializer)
    ByteBuffer.wrap(byteStream.toByteArray)
  }

  /**
   * Deserializes a ByteBuffer into an iterator of values and disposes of it when the end of
   * the iterator is reached.
   * 将blockId对应的文件内容bytes,进行反序列化成一组数据
   */
  def dataDeserialize(
      blockId: BlockId,
      bytes: ByteBuffer,
      serializer: Serializer = defaultSerializer): Iterator[Any] = {
    bytes.rewind() //将bytes的position设置为0,limit不更改,即表示可以去从头读取数据了
    dataDeserializeStream(blockId, new ByteBufferInputStream(bytes, true), serializer) //对流进行压缩处理
  }

  /**
   * Deserializes a InputStream into an iterator of values and disposes of it when the end of
   * the iterator is reached.
   * 将blockId对应的文件内容bytes,进行反序列化成一组数据
   * blockId的目的是确定是什么数据,比如是RDD还是shufflee,从而确定输入流是否是压缩文件,因此进行解压缩处理
   */
  def dataDeserializeStream(
      blockId: BlockId,
      inputStream: InputStream,
      serializer: Serializer = defaultSerializer): Iterator[Any] = {
    val stream = new BufferedInputStream(inputStream)
    serializer.newInstance().deserializeStream(wrapForCompression(blockId, stream)).asIterator //对流进行压缩处理
  }

  def stop(): Unit = {
    blockTransferService.close()
    if (shuffleClient ne blockTransferService) {
      // Closing should be idempotent, but maybe not for the NioBlockTransferService.
      shuffleClient.close()
    }
    diskBlockManager.stop()
    rpcEnv.stop(slaveEndpoint)
    blockInfo.clear()
    memoryStore.clear()
    diskStore.clear()
    if (externalBlockStoreInitialized) {
      externalBlockStore.clear()
    }
    metadataCleaner.cancel()
    broadcastCleaner.cancel()
    futureExecutionContext.shutdownNow()
    logInfo("BlockManager stopped")
  }
}


private[spark] object BlockManager extends Logging {
  private val ID_GENERATOR = new IdGenerator //静态方法,因此每一个节点上都有一套唯一ID

  /** Return the total amount of storage memory available. 
   * 返回内存存储允许的最大内存  
   **/
  private def getMaxMemory(conf: SparkConf): Long = {
    val memoryFraction = conf.getDouble("spark.storage.memoryFraction", 0.6) //最大使用内存比例
    val safetyFraction = conf.getDouble("spark.storage.safetyFraction", 0.9) //安全使用内存比例
    (Runtime.getRuntime.maxMemory * memoryFraction * safetyFraction).toLong //获取最终使用内存比例
  }

  /**
   * Attempt to clean up a ByteBuffer if it is memory-mapped. 
   * 尝试去清理MappedByteBuffer内存映射需要的内存
   * This uses an *unsafe* Sun API that might cause errors if one attempts to read from the unmapped buffer, 
   * 在sun的api里面这个操作是不安全的,因为如果一个程序想要去读取的时候,会产生错误 
   * but it's better than waiting for the GC to find it because that could lead to huge numbers of open files. There's unfortunately no standard API to do this.
   * 但是这个却比等待GC发现,并且gc回收要好,因为他能够导致大量的打开文件操作,不幸的是,没有标准的API去做这个事情。
   */
  def dispose(buffer: ByteBuffer): Unit = {
    if (buffer != null && buffer.isInstanceOf[MappedByteBuffer]) {
      logTrace(s"Unmapping $buffer")
      if (buffer.asInstanceOf[DirectBuffer].cleaner() != null) {
        buffer.asInstanceOf[DirectBuffer].cleaner().clean()
      }
    }
  }

  //获取参数数据块集合所在位置
  def blockIdsToHosts(
      blockIds: Array[BlockId],//获取这些数据块的位置
      env: SparkEnv,
      blockManagerMaster: BlockManagerMaster = null): Map[BlockId, Seq[String]] = { //表示master节点

    // blockManagerMaster != null is used in tests
    assert(env != null || blockManagerMaster != null)
    val blockLocations: Seq[Seq[BlockManagerId]] = if (blockManagerMaster == null) {//说明没有master节点
      env.blockManager.getLocationBlockIds(blockIds)//在本地节点上获取这些数据块位置
    } else {
      blockManagerMaster.getLocations(blockIds) //向msater获取这些数据块在哪些位置上
    }

    //返回每一个数据块key,value是该数据块所在host集合
    val blockManagers = new HashMap[BlockId, Seq[String]]
    for (i <- 0 until blockIds.length) {
      blockManagers(blockIds(i)) = blockLocations(i).map(_.host)//第i个数据块所在host集合
    }
    blockManagers.toMap
  }
}
