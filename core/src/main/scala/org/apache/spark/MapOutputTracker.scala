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

package org.apache.spark

import java.io._
import java.util.concurrent.ConcurrentHashMap
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Map}
import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv, RpcCallContext, RpcEndpoint}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.MetadataFetchFailedException
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockId}
import org.apache.spark.util._

private[spark] sealed trait MapOutputTrackerMessage //对map的输出进行跟踪
private[spark] case class GetMapOutputStatuses(shuffleId: Int) extends MapOutputTrackerMessage //获取shuffleId的结果状态
private[spark] case object StopMapOutputTracker extends MapOutputTrackerMessage //停止一个map的输出跟踪

/** RpcEndpoint class for MapOutputTrackerMaster
  * 该类负责接收请求,将对应的信息返回给对方
  * 比如接收请求获取某个shuffleId对应的结果状态
  *
  * rpcEnv是drvier上的服务器对象
  * tracker是MapOutputTrackerMaster,表示driver上的对象
  **/
private[spark] class MapOutputTrackerMasterEndpoint(
    override val rpcEnv: RpcEnv, tracker: MapOutputTrackerMaster, conf: SparkConf)
  extends RpcEndpoint with Logging {
  val maxAkkaFrameSize = AkkaUtils.maxFrameSizeBytes(conf)//返回一个message允许的上限,单位是字节

  //代表一个服务端
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case GetMapOutputStatuses(shuffleId: Int) =>
      val hostPort = context.sender.address.hostPort  //发送者的host和port
      logInfo("Asked to send map output locations for shuffle " + shuffleId + " to " + hostPort)

      //获取该shuffleId对应的结果状态
      val mapOutputStatuses = tracker.getSerializedMapOutputStatuses(shuffleId)
      val serializedSize = mapOutputStatuses.size
      if (serializedSize > maxAkkaFrameSize) {//超过大小了,要抛异常
        val msg = s"Map output statuses were $serializedSize bytes which " +
          s"exceeds spark.akka.frameSize ($maxAkkaFrameSize bytes)."

        /* For SPARK-1244 we'll opt for just logging an error and then sending it to the sender.
         * A bigger refactoring (SPARK-1239) will ultimately remove this entire code path. */
        val exception = new SparkException(msg)
        logError(msg, exception)
        context.sendFailure(exception)
      } else {
        context.reply(mapOutputStatuses)
      }

    case StopMapOutputTracker =>
      logInfo("MapOutputTrackerMasterEndpoint stopped!")//对该Akka进行停止
      context.reply(true)
      stop()
  }
}

/**
 * Class that keeps track of the location of the map output of
 * a stage. This is abstract because different versions of MapOutputTracker
 * (driver and executor) use different HashMap to store its metadata.
 */
private[spark] abstract class MapOutputTracker(conf: SparkConf) extends Logging {

  /** Set to the MapOutputTrackerMasterEndpoint living on the driver.
    *
    * 如果该节点是driver,则该对象是MapOutputTrackerMasterEndpoint对象
    * 如果该节点是executor,则该节点就是driver的RpcEndpointRef引用
    **/
  var trackerEndpoint: RpcEndpointRef = _

  /**
   * This HashMap has different behavior for the driver and the executors.
   * 在driver和executor中是不同的意义
   * On the driver, it serves as the source of map outputs recorded from ShuffleMapTasks.
   *
   * On the executors, it simply serves as a cache, in which a miss triggers a fetch from the
   * driver's corresponding HashMap.
   *
   * Note: because mapStatuses is accessed concurrently, subclasses should make sure it's a
   * thread-safe map.
   * 缓存抓去的结果
   * key是shuffleId,value是该shuffle对应的map结果集信息
   */
  protected val mapStatuses: Map[Int, Array[MapStatus]]

  /**
   * Incremented every time a fetch fails so that client nodes know to clear
   * their cache of map output locations if this happens.
   */
  protected var epoch: Long = 0
  protected val epochLock = new AnyRef

  /** Remembers which map output locations are currently being fetched on an executor.
    * 记录正在抓去哪些shuffleId集合
    **/
  private val fetching = new HashSet[Int]

  /**
   * Send a message to the trackerEndpoint and get its result within a default timeout, or
   * throw a SparkException if this fails.
   * 将message发送到trackerEndpoint端,返回T类型的数据
   */
  protected def askTracker[T: ClassTag](message: Any): T = {
    try {
      trackerEndpoint.askWithRetry[T](message) //阻塞,返回Feture
    } catch {
      case e: Exception =>
        logError("Error communicating with MapOutputTracker", e)
        throw new SparkException("Error communicating with MapOutputTracker", e)
    }
  }

  /** Send a one-way message to the trackerEndpoint, to which we expect it to reply with true. */
  protected def sendTracker(message: Any) {
    val response = askTracker[Boolean](message)  //期待返回值是true
    if (response != true) {//如果返回值不是true,则抛异常
      throw new SparkException(
        "Error reply received from MapOutputTracker. Expecting true, got " + response.toString)
    }
  }

  /**
   * Called from executors to get the server URIs and output sizes for each shuffle block that
   * needs to be read from a given reduce task.
   *
   * @return A sequence of 2-item tuples, where the first item in the tuple is a BlockManagerId,
   *         and the second item is a sequence of (shuffle block id, shuffle block size) tuples
   *         describing the shuffle blocks that are stored at that block manager.
   *  shuffle在reduce阶段核心方法,因为他要知道他需要的数据块在哪些节点上存在。找到该shuffle的某一个reduce节点需要抓去的数据块集合
   */
  def getMapSizesByExecutorId(shuffleId: Int, reduceId: Int)
  : Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    logDebug(s"Fetching outputs for shuffle $shuffleId, reduce $reduceId")
    val startTime = System.currentTimeMillis

    val statuses = mapStatuses.get(shuffleId).orNull
    if (statuses == null) {
      logInfo("Don't have map outputs for shuffle " + shuffleId + ", fetching them")
      var fetchedStatuses: Array[MapStatus] = null
      fetching.synchronized {
        // Someone else is fetching it; wait for them to be done
        while (fetching.contains(shuffleId)) {//如果该shuffle在抓去中,则等待结果即可
          try {
            fetching.wait()
          } catch {
            case e: InterruptedException =>
          }
        }

        // Either while we waited the fetch happened successfully, or
        // someone fetched it in between the get and the fetching.synchronized.
        fetchedStatuses = mapStatuses.get(shuffleId).orNull
        if (fetchedStatuses == null) {//如果说该shuffle没有抓去过程中,则将其添加到抓去队列中
          // We have to do the fetch, get others to wait for us.
          fetching += shuffleId
        }
      }

      if (fetchedStatuses == null) {//抓去的结果还是null
        // We won the race to fetch the output locs; do so
        logInfo("Doing the fetch; tracker endpoint = " + trackerEndpoint)
        // This try-finally prevents hangs due to timeouts:
        try {
          val fetchedBytes = askTracker[Array[Byte]](GetMapOutputStatuses(shuffleId)) //抓去该shuffleId的信息请求
          fetchedStatuses = MapOutputTracker.deserializeMapStatuses(fetchedBytes) //返回值进行反序列化
          logInfo("Got the output locations")
          mapStatuses.put(shuffleId, fetchedStatuses)
        } finally {
          fetching.synchronized {
            fetching -= shuffleId
            fetching.notifyAll()
          }
        }
      }
      logDebug(s"Fetching map output location for shuffle $shuffleId, reduce $reduceId took " +
        s"${System.currentTimeMillis - startTime} ms")

      if (fetchedStatuses != null) {
        fetchedStatuses.synchronized {
          return MapOutputTracker.convertMapStatuses(shuffleId, reduceId, fetchedStatuses)
        }
      } else {
        logError("Missing all output locations for shuffle " + shuffleId)
        throw new MetadataFetchFailedException(
          shuffleId, reduceId, "Missing all output locations for shuffle " + shuffleId)
      }
    } else {
      statuses.synchronized {
        return MapOutputTracker.convertMapStatuses(shuffleId, reduceId, statuses)
      }
    }
  }

  /** Called to get current epoch number. */
  def getEpoch: Long = {
    epochLock.synchronized {
      return epoch
    }
  }

  /**
   * Called from executors to update the epoch number, potentially clearing old outputs
   * because of a fetch failure. Each executor task calls this with the latest epoch
   * number on the driver at the time it was created.
   */
  def updateEpoch(newEpoch: Long) {
    epochLock.synchronized {
      if (newEpoch > epoch) {
        logInfo("Updating epoch to " + newEpoch + " and clearing cache")
        epoch = newEpoch
        mapStatuses.clear()
      }
    }
  }

  /** Unregister shuffle data. */
  def unregisterShuffle(shuffleId: Int) {
    mapStatuses.remove(shuffleId)
  }

  /** Stop the tracker. */
  def stop() { }
}

/**
 * MapOutputTracker for the driver. This uses TimeStampedHashMap to keep track of map
 * output information, which allows old output information based on a TTL.
 */
private[spark] class MapOutputTrackerMaster(conf: SparkConf)
  extends MapOutputTracker(conf) {

  /** Cache a serialized version of the output statuses for each shuffle to send them out faster */
  private var cacheEpoch = epoch

  /**
   * Timestamp based HashMap for storing mapStatuses and cached serialized statuses in the driver,
   * so that statuses are dropped only by explicit de-registering or by TTL-based cleaning (if set).
   * Other than these two scenarios, nothing should be dropped from this HashMap.
   * key是shuffleId,value是该shuffleId对应的Map阶段的状态集合
   */
  protected val mapStatuses = new TimeStampedHashMap[Int, Array[MapStatus]]()
  private val cachedSerializedStatuses = new TimeStampedHashMap[Int, Array[Byte]]() //缓存每一个shuffleId对应的结果集,因为每一次序列化是很耗时的

  // For cleaning up TimeStampedHashMaps
  private val metadataCleaner =
    new MetadataCleaner(MetadataCleanerType.MAP_OUTPUT_TRACKER, this.cleanup, conf)

  //注册一个shuffleId对应多少个Map
  def registerShuffle(shuffleId: Int, numMaps: Int) {
    if (mapStatuses.put(shuffleId, new Array[MapStatus](numMaps)).isDefined) {//返回值如果已经被定义了,说明该shuffleId曾经已经被注册一次了,是不允许注册两次的
      throw new IllegalArgumentException("Shuffle ID " + shuffleId + " registered twice")
    }
  }

  //注册某个shuffleId的某个mapId对应的MapStatus
  def registerMapOutput(shuffleId: Int, mapId: Int, status: MapStatus) {
    val array = mapStatuses(shuffleId)
    array.synchronized {
      array(mapId) = status
    }
  }

  /** Register multiple map output information for the given shuffle 
   *  注册一组MapStatus结果到shuffle中----当driver端收到一个shuffle任务执行成功后,调用该方法
   **/
  def registerMapOutputs(shuffleId: Int, statuses: Array[MapStatus], changeEpoch: Boolean = false) {
    mapStatuses.put(shuffleId, Array[MapStatus]() ++ statuses)
    if (changeEpoch) {
      incrementEpoch()
    }
  }

  /** Unregister map output information of the given shuffle, mapper and block manager 
   *  移除该shuffleId的mapId对应的数据块映射
   **/
  def unregisterMapOutput(shuffleId: Int, mapId: Int, bmAddress: BlockManagerId) {
    val arrayOpt = mapStatuses.get(shuffleId)
    if (arrayOpt.isDefined && arrayOpt.get != null) {
      val array = arrayOpt.get
      array.synchronized {
        if (array(mapId) != null && array(mapId).location == bmAddress) {
          array(mapId) = null
        }
      }
      incrementEpoch()
    } else {
      throw new SparkException("unregisterMapOutput called for nonexistent shuffle ID")
    }
  }

  /** Unregister shuffle data 
   *  移除所有的shuffleId相关信息
   **/
  override def unregisterShuffle(shuffleId: Int) {
    mapStatuses.remove(shuffleId)
    cachedSerializedStatuses.remove(shuffleId)
  }

  /** Check if the given shuffle is being tracked
   *  true表示该shuffle已经存在了
   **/
  def containsShuffle(shuffleId: Int): Boolean = {
    cachedSerializedStatuses.contains(shuffleId) || mapStatuses.contains(shuffleId)
  }

  /**
   * Return a list of locations that each have fraction of map output greater than the specified
   * threshold.
   * 返回一个节点集合,这个集合内产生的输出比伐值要大
   * @param shuffleId id of the shuffle 哪个shuffle
   * @param reducerId id of the reduce task 第几个recude任务来获取该reduce的输入源字节占比
   * @param numReducers total number of reducers in the shuffle,shuffle过程中总的recude数量
   * @param fractionThreshold fraction of total map output size that a location must have
   *                          for it to be considered large.伐值,表示map在该节点的输出结果内容 占 总结果内容的比值,如果比该伐值大,说明该节点数据有点多
   *
   * This method is not thread-safe.
   * 返回比伐值大的输出
   */
  def getLocationsWithLargestOutputs(
      shuffleId: Int,
      reducerId: Int,
      numReducers: Int,
      fractionThreshold: Double)
    : Option[Array[BlockManagerId]] = {

    if (mapStatuses.contains(shuffleId)) {
      val statuses = mapStatuses(shuffleId) //shuffleId对应的Array[MapStatus]集合
      if (statuses.nonEmpty) {
        // HashMap to add up sizes of all blocks at the same location
        val locs = new HashMap[BlockManagerId, Long] //key是status.location,value是该status.location上字节总数
        var totalOutputSize = 0L //该reduce的总输出字节数
        var mapIdx = 0
        while (mapIdx < statuses.length) {//循环该shuffle的所有map任务
          val status = statuses(mapIdx) //返回一个map的返回值对象
          val blockSize = status.getSizeForBlock(reducerId)//计算该map对该reduce的输出大小
          if (blockSize > 0) {
            locs(status.location) = locs.getOrElse(status.location, 0L) + blockSize //该节点的数据存储的大小 = 原始大小+新大小
            totalOutputSize += blockSize
          }
          mapIdx = mapIdx + 1 //继续循环
        }
        val topLocs = locs.filter { case (loc, size) =>
          size.toDouble / totalOutputSize >= fractionThreshold //计算每一个节点上该reduce的占比.如果比伐值大的则保留
        }
        // Return if we have any locations which satisfy the required threshold
        if (topLocs.nonEmpty) {
          return Some(topLocs.map(_._1).toArray) //返回比伐值大的节点host集合
        }
      }
    }
    None
  }

  def incrementEpoch() {
    epochLock.synchronized {
      epoch += 1
      logDebug("Increasing epoch to " + epoch)
    }
  }

  //获取该shuffle的所有map结果集,返回序列化后的字节数组
  def getSerializedMapOutputStatuses(shuffleId: Int): Array[Byte] = {
    var statuses: Array[MapStatus] = null
    var epochGotten: Long = -1 //结果被序列化的epoch码
    epochLock.synchronized {
      if (epoch > cacheEpoch) {
        cachedSerializedStatuses.clear() //说明缓存失效了
        cacheEpoch = epoch //设置新的缓存点
      }
      cachedSerializedStatuses.get(shuffleId) match { //缓存如果失效了,已经不能在get到了
        case Some(bytes) => //如果能get到,说明缓存没清空
          return bytes
        case None =>
          statuses = mapStatuses.getOrElse(shuffleId, Array[MapStatus]())
          epochGotten = epoch //说明结果要去被序列化
      }
    }
    // If we got here, we failed to find the serialized locations in the cache, so we pulled
    // out a snapshot of the locations as "statuses"; let's serialize and return that
    val bytes = MapOutputTracker.serializeMapStatuses(statuses) //对结果集序列化
    logInfo("Size of output statuses for shuffle %d is %d bytes".format(shuffleId, bytes.length))
    // Add them into the table only if the epoch hasn't changed while we were working
    epochLock.synchronized {
      if (epoch == epochGotten) { //说明是本次刚刚清空了缓存,因此要向缓存添加数据
        cachedSerializedStatuses(shuffleId) = bytes //更新缓存序列化的结果集
      }
    }
    bytes
  }

  override def stop() {
    sendTracker(StopMapOutputTracker) //报告说要停止该服务
    mapStatuses.clear() //清理存储的元数据
    trackerEndpoint = null //服务都被停止了,自然引用也没意义了
    metadataCleaner.cancel() //定时清理服务关闭
    cachedSerializedStatuses.clear() //清空缓存内容
  }

  //定时清理服务
  private def cleanup(cleanupTime: Long) {
    mapStatuses.clearOldValues(cleanupTime)
    cachedSerializedStatuses.clearOldValues(cleanupTime)
  }
}

/**
 * MapOutputTracker for the executors, which fetches map output information from the driver's
 * MapOutputTrackerMaster.
 */
private[spark] class MapOutputTrackerWorker(conf: SparkConf) extends MapOutputTracker(conf) {
  protected val mapStatuses: Map[Int, Array[MapStatus]] =
    new ConcurrentHashMap[Int, Array[MapStatus]] //每一个shuffileId作为key,value是该该shuffleId对应的map端结果集
}

private[spark] object MapOutputTracker extends Logging {

  val ENDPOINT_NAME = "MapOutputTracker" //Actor的name

  // Serialize an array of map output locations into an efficient byte format so that we can send
  // it to reduce tasks. We do this by compressing the serialized bytes using GZIP. They will
  // generally be pretty compressible because many map outputs will be on the same hostname.
  //因为同一个host上有很多map的输出,因此压缩一下传输效率会高一些
  //将参数map状态集合序列化成字节数组
  def serializeMapStatuses(statuses: Array[MapStatus]): Array[Byte] = {
    val out = new ByteArrayOutputStream
    val objOut = new ObjectOutputStream(new GZIPOutputStream(out)) //压缩流
    Utils.tryWithSafeFinally {
      // Since statuses can be modified in parallel, sync on it
      statuses.synchronized {
        objOut.writeObject(statuses)
      }
    } {
      objOut.close()
    }
    out.toByteArray
  }

  // Opposite of serializeMapStatuses.
  //范序列化,将字节数组转换成MapStatus数组
  def deserializeMapStatuses(bytes: Array[Byte]): Array[MapStatus] = {
    val objIn = new ObjectInputStream(new GZIPInputStream(new ByteArrayInputStream(bytes)))
    Utils.tryWithSafeFinally {
      objIn.readObject().asInstanceOf[Array[MapStatus]]
    } {
      objIn.close()
    }
  }

  /**
   * Converts an array of MapStatuses for a given reduce ID to a sequence that, for each block
   * manager ID, lists the shuffle block ids and corresponding shuffle block sizes stored at that
   * block manager.
   *
   * If any of the statuses is null (indicating a missing location due to a failed mapper),
   * throws a FetchFailedException.
   *
   * @param shuffleId Identifier for the shuffle
   * @param reduceId Identifier for the reduce task
   * @param statuses List of map statuses, indexed by map ID.
   * @return A sequence of 2-item tuples, where the first item in the tuple is a BlockManagerId,
   *         and the second item is a sequence of (shuffle block id, shuffle block size) tuples
   *         describing the shuffle blocks that are stored at that block manager.
   * 数据转换
   */
  private def convertMapStatuses(
      shuffleId: Int,
      reduceId: Int,
      statuses: Array[MapStatus]): Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    assert (statuses != null)
    val splitsByAddress = new HashMap[BlockManagerId, ArrayBuffer[(BlockId, Long)]]
    for ((status, mapId) <- statuses.zipWithIndex) {
      if (status == null) {
        val errorMessage = s"Missing an output location for shuffle $shuffleId"
        logError(errorMessage)
        throw new MetadataFetchFailedException(shuffleId, reduceId, errorMessage)
      } else {
        splitsByAddress.getOrElseUpdate(status.location, ArrayBuffer()) +=
          ((ShuffleBlockId(shuffleId, mapId, reduceId), status.getSizeForBlock(reduceId)))
      }
    }

    splitsByAddress.toSeq
  }
}