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

import java.io.File
import java.net.Socket

import akka.actor.ActorSystem

import scala.collection.mutable
import scala.util.Properties

import com.google.common.collect.MapMaker

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.python.PythonWorkerFactory
import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.network.BlockTransferService
import org.apache.spark.network.netty.NettyBlockTransferService
import org.apache.spark.network.nio.NioBlockTransferService
import org.apache.spark.rpc.{RpcEndpointRef, RpcEndpoint, RpcEnv}
import org.apache.spark.rpc.akka.AkkaRpcEnv
import org.apache.spark.scheduler.{OutputCommitCoordinator, LiveListenerBus}
import org.apache.spark.scheduler.OutputCommitCoordinator.OutputCommitCoordinatorEndpoint
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{ShuffleMemoryManager, ShuffleManager}
import org.apache.spark.storage._
import org.apache.spark.unsafe.memory.{ExecutorMemoryManager, MemoryAllocator}
import org.apache.spark.util.{RpcUtils, Utils}

/**
 * :: DeveloperApi ::
 * Holds all the runtime environment objects for a running Spark instance (either master or worker),
 * including the serializer, Akka actor system, block manager, map output tracker, etc. Currently
 * Spark code finds the SparkEnv through a global variable, so all the threads can access the same
 * SparkEnv. It can be accessed by SparkEnv.get (e.g. after creating a SparkContext).
 *
 * NOTE: This is not intended for external use. This is exposed for Shark and may be made private
 *       in a future release.
 * 代表spark的执行环境
 *
 * 以下对象都是每一个SparkEnv单独创建一个该对象
 *
 *
 逻辑如下:
 一、创建一个SparkEnv对象需要的参数
      conf: SparkConf,
      executorId: String,//如果是driver,则写入固定的driver名字,表示该执行器是driver的执行器
      hostname: String,//driver或者executor所在host
      port: Int,//driver或者executor所在post
      isDriver: Boolean,//true表示是driver的环境,false表示是执行者的环境
      isLocal: Boolean,//true表示是本地方式启动,不是集群方式,即master == "local" || master.startsWith("local[")
      numUsableCores: Int,//driver需要多少cpu去执行本地模式.非本地模式都是返回0
      listenerBus: LiveListenerBus = null,//该属性仅仅用于driver,非driver的都是null
      mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None
二、创建步骤
1.如果该对象是driver的,则必须有listenerBus,因为上面会有很多监听器,串联整个逻辑关系
2.val securityManager = new SecurityManager(conf)
3.在host和port下创建RpcEnv.create对象,actorSystemName的name看是driver还是execute而不同
  因此返回真正的port,这在设置到spark.driver.port或者spark.executor.port中
4.创建序列化对象,默认是org.apache.spark.serializer.JavaSerializer
  从conf中读取key为spark.serializer的,返回的class就是序列化对象serializer
  从conf中读取key为spark.closure.serializer的,返回的class就是序列化对象closureSerializer
5.创建输出对象
    val mapOutputTracker = if (isDriver) {
      new MapOutputTrackerMaster(conf)
    } else {
      new MapOutputTrackerWorker(conf)
    }
6.通过spark.shuffle.manager获取短名称,从而得到具体shuffler类
      "hash" -> "org.apache.spark.shuffle.hash.HashShuffleManager",
      "sort" -> "org.apache.spark.shuffle.sort.SortShuffleManager",
      "tungsten-sort" -> "org.apache.spark.shuffle.unsafe.UnsafeShuffleManager")
7.val shuffleMemoryManager = ShuffleMemoryManager.create(conf, 多线程数量)
8.数据块传输服务
    val blockTransferService =
      conf.get("spark.shuffle.blockTransferService", "netty").toLowerCase match {
        case "netty" =>
          new NettyBlockTransferService(conf, securityManager, numUsableCores)
        case "nio" =>
          logWarning("NIO-based block transfer service is deprecated, " +
            "and will be removed in Spark 1.6.0.")
          new NioBlockTransferService(conf, securityManager)
      }
9.
    //为BlockManagerMaster设置参数,如果该节点是driver,则该对象是dirver节点上的BlockManagerMasterEndpoint引用     如果该节点是executor,则该对象是driver的RpcEndpointRef引用
    val blockManagerMaster = new BlockManagerMaster(registerOrLookupEndpoint(
      BlockManagerMaster.DRIVER_ENDPOINT_NAME,
      new BlockManagerMasterEndpoint(rpcEnv, isLocal, conf, listenerBus)),
      conf, isDriver)

    // NB: blockManager is not valid until initialize() is called later.
    //参数具体含义,参照BlockManager类详细信息
    val blockManager = new BlockManager(executorId, rpcEnv, blockManagerMaster,
      serializer, conf, mapOutputTracker, shuffleManager, blockTransferService, securityManager,
      numUsableCores)

10.val broadcastManager = new BroadcastManager(isDriver, conf, securityManager)
11.val cacheManager = new CacheManager(blockManager)
12.在本地开启一个下载服务,可以下载指定目录下的文件
spark.fileserver.port 表示服务端口
spark.fileserver.uri 表示最终服务的uri

13.
    // Set the sparkFiles directory, used when downloading dependencies.  In local mode,
    // this is a temporary directory; in distributed mode, this is the executor's current working
    // directory.
    val sparkFilesDir: String = if (isDriver) {
      Utils.createTempDir(Utils.getLocalDir(conf), "userFiles").getAbsolutePath
    } else {
      "."
    }

    val outputCommitCoordinator = mockOutputCommitCoordinator.getOrElse {
      new OutputCommitCoordinator(conf, isDriver)
    }
    val outputCommitCoordinatorRef = registerOrLookupEndpoint("OutputCommitCoordinator",
      new OutputCommitCoordinatorEndpoint(rpcEnv, outputCommitCoordinator))

    outputCommitCoordinator.coordinatorRef = Some(outputCommitCoordinatorRef)

    val executorMemoryManager: ExecutorMemoryManager = {
      val allocator = if (conf.getBoolean("spark.unsafe.offHeap", false)) {
        MemoryAllocator.UNSAFE
      } else {
        MemoryAllocator.HEAP
      }
      new ExecutorMemoryManager(allocator)
    }

注意 class从配置参数读取出来的,都是只能接受SparkConf和是否是driver的boolean变量的构造函数,或者只接受SparkConf对象的构造函数

 */
@DeveloperApi
class SparkEnv (
    val executorId: String,//如果是driver的话这个值就是driver
    private[spark] val rpcEnv: RpcEnv,//提供PRC的服务器端对象
    val serializer: Serializer,//序列化类
    val closureSerializer: Serializer,//闭关代码的序列化类
    val cacheManager: CacheManager,
    val mapOutputTracker: MapOutputTracker,
    val shuffleManager: ShuffleManager,
    val broadcastManager: BroadcastManager,
    val blockTransferService: BlockTransferService,
    val blockManager: BlockManager,
    val securityManager: SecurityManager,
    val httpFileServer: HttpFileServer,
    val sparkFilesDir: String,
    val metricsSystem: MetricsSystem,
    val shuffleMemoryManager: ShuffleMemoryManager,
    val executorMemoryManager: ExecutorMemoryManager,
    val outputCommitCoordinator: OutputCommitCoordinator,
    val conf: SparkConf) extends Logging {

  // TODO Remove actorSystem
  @deprecated("Actor system is no longer supported as of 1.4.0", "1.4.0")
  val actorSystem: ActorSystem = rpcEnv.asInstanceOf[AkkaRpcEnv].actorSystem

  private[spark] var isStopped = false
  private val pythonWorkers = mutable.HashMap[(String, Map[String, String]), PythonWorkerFactory]()

  // A general, soft-reference map for metadata needed during HadoopRDD split computation
  // (e.g., HadoopFileRDD uses this to cache JobConfs and InputFormats).
  private[spark] val hadoopJobMetadata = new MapMaker().softValues().makeMap[String, Any]()

  private var driverTmpDirToDelete: Option[String] = None

  private[spark] def stop() {

    if (!isStopped) {
      isStopped = true
      pythonWorkers.values.foreach(_.stop())
      Option(httpFileServer).foreach(_.stop())
      mapOutputTracker.stop()
      shuffleManager.stop()
      broadcastManager.stop()
      blockManager.stop()
      blockManager.master.stop()
      metricsSystem.stop()
      outputCommitCoordinator.stop()
      rpcEnv.shutdown()

      // Unfortunately Akka's awaitTermination doesn't actually wait for the Netty server to shut
      // down, but let's call it anyway in case it gets fixed in a later release
      // UPDATE: In Akka 2.1.x, this hangs if there are remote actors, so we can't call it.
      // actorSystem.awaitTermination()

      // Note that blockTransferService is stopped by BlockManager since it is started by it.

      // If we only stop sc, but the driver process still run as a services then we need to delete
      // the tmp dir, if not, it will create too many tmp dirs.
      // We only need to delete the tmp dir create by driver, because sparkFilesDir is point to the
      // current working dir in executor which we do not need to delete.
      driverTmpDirToDelete match {
        case Some(path) => {
          try {
            Utils.deleteRecursively(new File(path))
          } catch {
            case e: Exception =>
              logWarning(s"Exception while deleting Spark temp dir: $path", e)
          }
        }
        case None => // We just need to delete tmp dir created by driver, so do nothing on executor
      }
    }
  }

  private[spark]
  def createPythonWorker(pythonExec: String, envVars: Map[String, String]): java.net.Socket = {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.getOrElseUpdate(key, new PythonWorkerFactory(pythonExec, envVars)).create()
    }
  }

  private[spark]
  def destroyPythonWorker(pythonExec: String, envVars: Map[String, String], worker: Socket) {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.get(key).foreach(_.stopWorker(worker))
    }
  }

  private[spark]
  def releasePythonWorker(pythonExec: String, envVars: Map[String, String], worker: Socket) {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.get(key).foreach(_.releaseWorker(worker))
    }
  }
}

object SparkEnv extends Logging {
  @volatile private var env: SparkEnv = _

  private[spark] val driverActorSystemName = "sparkDriver" //为driver设置ActorSystem的名字前缀
  private[spark] val executorActorSystemName = "sparkExecutor" //为执行者设置ActorSystem的名字前缀

  def set(e: SparkEnv) {
    env = e
  }

  /**
   * Returns the SparkEnv.
   */
  def get: SparkEnv = {
    env
  }

  /**
   * Returns the ThreadLocal SparkEnv.
   */
  @deprecated("Use SparkEnv.get instead", "1.2.0")
  def getThreadLocal: SparkEnv = {
    env
  }

  /**
   * Create a SparkEnv for the driver.
   * 为driver创建环境
   */
  private[spark] def createDriverEnv(
      conf: SparkConf,
      isLocal: Boolean,//true表示是本地方式启动,不是集群方式
      listenerBus: LiveListenerBus,
      numCores: Int,//driver需要多少cpu去执行本地模式.非本地模式都是返回0
      mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {
    //必须有host和port
    assert(conf.contains("spark.driver.host"), "spark.driver.host is not set on the driver!")
    assert(conf.contains("spark.driver.port"), "spark.driver.port is not set on the driver!")
    val hostname = conf.get("spark.driver.host")
    val port = conf.get("spark.driver.port").toInt
    create(
      conf,
      SparkContext.DRIVER_IDENTIFIER,//driver
      hostname,//driver所在host
      port,//driver所在port
      isDriver = true,
      isLocal = isLocal,
      numUsableCores = numCores,//driver需要多少cpu去执行本地模式.非本地模式都是返回0
      listenerBus = listenerBus,
      mockOutputCommitCoordinator = mockOutputCommitCoordinator
    )
  }

  /**
   * Create a SparkEnv for an executor.
   * In coarse-grained mode, the executor provides an actor system that is already instantiated.
   * 为execute创建环境
   */
  private[spark] def createExecutorEnv(
      conf: SparkConf,
      executorId: String,
      hostname: String,
      port: Int,
      numCores: Int,
      isLocal: Boolean): SparkEnv = {
    val env = create(
      conf,
      executorId,
      hostname,
      port,
      isDriver = false,
      isLocal = isLocal,
      numUsableCores = numCores
    )
    SparkEnv.set(env)
    env
  }

  /**
   * Helper method to create a SparkEnv for a driver or an executor.
   */
  private def create(
      conf: SparkConf,
      executorId: String,//如果是driver,则写入固定的driver名字,表示该执行器是driver的执行器
      hostname: String,//driver或者executor所在host
      port: Int,//driver或者executor所在post
      isDriver: Boolean,//true表示是driver的环境,false表示是执行者的环境
      isLocal: Boolean,//true表示是本地方式启动,不是集群方式,即master == "local" || master.startsWith("local[")
      numUsableCores: Int,//driver需要多少cpu去执行本地模式.非本地模式都是返回0
      listenerBus: LiveListenerBus = null,//该属性仅仅用于driver,非driver的都是null
      mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {

    // Listener bus is only used on the driver 该属性仅仅用于driver,非driver的都是null
    if (isDriver) {
      assert(listenerBus != null, "Attempted to create driver SparkEnv with null listener bus!") //listenerBus不允许是null
    }

    val securityManager = new SecurityManager(conf)

    // Create the ActorSystem for Akka and get the port it binds to.绑定的Actor名字
    val actorSystemName = if (isDriver) driverActorSystemName else executorActorSystemName
    
    //对host,port创建一个RPC 服务端,在host port上进行监听
    val rpcEnv = RpcEnv.create(actorSystemName, hostname, port, conf, securityManager)
    val actorSystem = rpcEnv.asInstanceOf[AkkaRpcEnv].actorSystem

    // Figure out which port Akka actually bound to in case the original port is 0 or occupied.计算真正的绑定端口
    if (isDriver) {
      conf.set("spark.driver.port", rpcEnv.address.port.toString)
    } else {
      conf.set("spark.executor.port", rpcEnv.address.port.toString)
    }

    // Create an instance of the class with the given name, possibly initializing it with our conf
    //创建参数对应的class的实例对象,该class必须有构造函数为SparkConf和boolean isDriver 或者SparkConf的构造函数
    //返回该class对应的实例对象
    def instantiateClass[T](className: String): T = {
      val cls = Utils.classForName(className) //找到class对象
      // Look for a constructor taking a SparkConf and a boolean isDriver, then one taking just
      // SparkConf, then one taking no arguments
      //首先寻找两个参数的构造函数,SparkConf和boolean isDriver,如果找不到,则找一个SparkConf参数的方法,如果继续找不到则找无参数的构造方法
      try {
        cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
          .newInstance(conf, new java.lang.Boolean(isDriver))
          .asInstanceOf[T]
      } catch {
        case _: NoSuchMethodException =>
          try {
            cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
          } catch {
            case _: NoSuchMethodException =>
              cls.getConstructor().newInstance().asInstanceOf[T]
          }
      }
    }

    // Create an instance of the class named by the given SparkConf property, or defaultClassName
    // if the property is not set, possibly initializing it with our conf
    //通过key是propertyName,读取配置文件对应的内容是具体class,去创建class对象,默认获取defaultClassName对应的class对象
    def instantiateClassFromConf[T](propertyName: String, defaultClassName: String): T = {
      instantiateClass[T](conf.get(propertyName, defaultClassName))
    }

    //获取序列化对象
    val serializer = instantiateClassFromConf[Serializer](
      "spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    logDebug(s"Using serializer: ${serializer.getClass}")

    //获取闭环的序列化对象
    val closureSerializer = instantiateClassFromConf[Serializer](
      "spark.closure.serializer", "org.apache.spark.serializer.JavaSerializer")

    //如果是dirver,则与RpcEndpoint创建连接,返回引用,即driver向RpcEndpoint发送连接
    //如果是execute,则从conf中读取driver的host和port,连接到driver节点上,即RpcEndpoint的功能由driver提供,因此要连接到driver即可
    def registerOrLookupEndpoint(
        name: String, endpointCreator: => RpcEndpoint):
      RpcEndpointRef = {
      if (isDriver) {
        logInfo("Registering " + name)
        rpcEnv.setupEndpoint(name, endpointCreator)//在driver本地向endpointCreator发送请求,建立引用
      } else {
        RpcUtils.makeDriverRef(name, conf, rpcEnv) //executor不需要第二个参数.因为此时rpcEnv是executor的服务器所在对象,他要从conf中获取driver的host和port,向起发送信息,向driver发请求,返回driver的引用
      }
    }

    //输出
    val mapOutputTracker = if (isDriver) {
      new MapOutputTrackerMaster(conf)
    } else {
      new MapOutputTrackerWorker(conf)
    }

    // Have to assign trackerActor after initialization as MapOutputTrackerActor
    // requires the MapOutputTracker itself
    /**
     * 如果该节点是driver,则该对象是MapOutputTrackerMasterEndpoint对象
     * 如果该节点是executor,则该节点就是driver的RpcEndpointRef引用
     */
    mapOutputTracker.trackerEndpoint = registerOrLookupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(
        rpcEnv, mapOutputTracker.asInstanceOf[MapOutputTrackerMaster], conf))

    // Let the user specify short names for shuffle managers
    val shortShuffleMgrNames = Map(
      "hash" -> "org.apache.spark.shuffle.hash.HashShuffleManager",
      "sort" -> "org.apache.spark.shuffle.sort.SortShuffleManager",
      "tungsten-sort" -> "org.apache.spark.shuffle.unsafe.UnsafeShuffleManager")
    val shuffleMgrName = conf.get("spark.shuffle.manager", "sort")//获取短名称
    val shuffleMgrClass = shortShuffleMgrNames.getOrElse(shuffleMgrName.toLowerCase, shuffleMgrName)//通过短名称获取类
    val shuffleManager = instantiateClass[ShuffleManager](shuffleMgrClass)//实例化该类

    val shuffleMemoryManager = ShuffleMemoryManager.create(conf, numUsableCores)//numUsableCores表示多线程数量,如果不是local的,则该值为0

    //数据块传输服务
    val blockTransferService =
      conf.get("spark.shuffle.blockTransferService", "netty").toLowerCase match {
        case "netty" =>
          new NettyBlockTransferService(conf, securityManager, numUsableCores)
        case "nio" =>
          logWarning("NIO-based block transfer service is deprecated, " +
            "and will be removed in Spark 1.6.0.")
          new NioBlockTransferService(conf, securityManager)
      }

    //为BlockManagerMaster设置参数,如果该节点是driver,则该对象是dirver节点上的BlockManagerMasterEndpoint引用     如果该节点是executor,则该对象是driver的RpcEndpointRef引用
    val blockManagerMaster = new BlockManagerMaster(registerOrLookupEndpoint(
      BlockManagerMaster.DRIVER_ENDPOINT_NAME,
      new BlockManagerMasterEndpoint(rpcEnv, isLocal, conf, listenerBus)),
      conf, isDriver)

    // NB: blockManager is not valid until initialize() is called later.
    //参数具体含义,参照BlockManager类详细信息
    val blockManager = new BlockManager(executorId, rpcEnv, blockManagerMaster,
      serializer, conf, mapOutputTracker, shuffleManager, blockTransferService, securityManager,
      numUsableCores)

    val broadcastManager = new BroadcastManager(isDriver, conf, securityManager)

    val cacheManager = new CacheManager(blockManager)

    val httpFileServer =
      if (isDriver) {//driver节点要创建一个服务
        val fileServerPort = conf.getInt("spark.fileserver.port", 0)
        val server = new HttpFileServer(conf, securityManager, fileServerPort)
        server.initialize()
        conf.set("spark.fileserver.uri", server.serverUri)
        server
      } else {
        null
      }

    val metricsSystem = if (isDriver) {
      // Don't start metrics system right now for Driver.
      // We need to wait for the task scheduler to give us an app ID.
      // Then we can start the metrics system.
      MetricsSystem.createMetricsSystem("driver", conf, securityManager)
    } else {
      // We need to set the executor ID before the MetricsSystem is created because sources and
      // sinks specified in the metrics configuration file will want to incorporate this executor's
      // ID into the metrics they report.
      conf.set("spark.executor.id", executorId)
      val ms = MetricsSystem.createMetricsSystem("executor", conf, securityManager)
      ms.start()
      ms
    }

    // Set the sparkFiles directory, used when downloading dependencies.  In local mode,
    // this is a temporary directory; in distributed mode, this is the executor's current working
    // directory.
    val sparkFilesDir: String = if (isDriver) {
      Utils.createTempDir(Utils.getLocalDir(conf), "userFiles").getAbsolutePath
    } else {
      "."
    }

    val outputCommitCoordinator = mockOutputCommitCoordinator.getOrElse {
      new OutputCommitCoordinator(conf, isDriver)
    }
    val outputCommitCoordinatorRef = registerOrLookupEndpoint("OutputCommitCoordinator",
      new OutputCommitCoordinatorEndpoint(rpcEnv, outputCommitCoordinator))

    outputCommitCoordinator.coordinatorRef = Some(outputCommitCoordinatorRef)

    val executorMemoryManager: ExecutorMemoryManager = {
      val allocator = if (conf.getBoolean("spark.unsafe.offHeap", false)) {
        MemoryAllocator.UNSAFE
      } else {
        MemoryAllocator.HEAP
      }
      new ExecutorMemoryManager(allocator)
    }

    val envInstance = new SparkEnv(
      executorId,
      rpcEnv,
      serializer,
      closureSerializer,
      cacheManager,
      mapOutputTracker,
      shuffleManager,
      broadcastManager,
      blockTransferService,
      blockManager,
      securityManager,
      httpFileServer,
      sparkFilesDir,
      metricsSystem,
      shuffleMemoryManager,
      executorMemoryManager,
      outputCommitCoordinator,
      conf)

    // Add a reference to tmp dir created by driver, we will delete this tmp dir when stop() is
    // called, and we only need to do it for driver. Because driver may run as a service, and if we
    // don't delete this tmp dir when sc is stopped, then will create too many tmp dirs.
    if (isDriver) {
      envInstance.driverTmpDirToDelete = Some(sparkFilesDir)
    }

    envInstance
  }

  /**
   * Return a map representation of jvm information, Spark properties, system properties, and
   * class paths. Map keys define the category, and map values represent the corresponding
   * attributes as a sequence of KV pairs. This is used mainly for SparkListenerEnvironmentUpdate.
   */
  private[spark]
  def environmentDetails(
      conf: SparkConf,
      schedulingMode: String,
      addedJars: Seq[String],
      addedFiles: Seq[String]): Map[String, Seq[(String, String)]] = {

    import Properties._
    val jvmInformation = Seq(
      ("Java Version", s"$javaVersion ($javaVendor)"),
      ("Java Home", javaHome),
      ("Scala Version", versionString)
    ).sorted

    // Spark properties
    // This includes the scheduling mode whether or not it is configured (used by SparkUI)
    val schedulerMode =
      if (!conf.contains("spark.scheduler.mode")) {
        Seq(("spark.scheduler.mode", schedulingMode))
      } else {
        Seq[(String, String)]()
      }
    val sparkProperties = (conf.getAll ++ schedulerMode).sorted

    // System properties that are not java classpaths
    val systemProperties = Utils.getSystemProperties.toSeq
    val otherProperties = systemProperties.filter { case (k, _) =>
      k != "java.class.path" && !k.startsWith("spark.")
    }.sorted

    // Class paths including all added jars and files
    val classPathEntries = javaClassPath
      .split(File.pathSeparator)
      .filterNot(_.isEmpty)
      .map((_, "System Classpath"))
    val addedJarsAndFiles = (addedJars ++ addedFiles).map((_, "Added By User"))
    val classPaths = (addedJarsAndFiles ++ classPathEntries).sorted

    Map[String, Seq[(String, String)]](
      "JVM Information" -> jvmInformation,
      "Spark Properties" -> sparkProperties,
      "System Properties" -> otherProperties,
      "Classpath Entries" -> classPaths)
  }
}
