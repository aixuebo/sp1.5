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

package org.apache.spark.deploy.worker

import java.io.File
import java.io.IOException
import java.text.SimpleDateFormat
import java.util.{UUID, Date}
import java.util.concurrent._
import java.util.concurrent.{Future => JFuture, ScheduledFuture => JScheduledFuture}

import scala.collection.JavaConversions._
import scala.collection.mutable.{HashMap, HashSet, LinkedHashMap}
import scala.concurrent.ExecutionContext
import scala.util.Random
import scala.util.control.NonFatal

import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.deploy.{Command, ExecutorDescription, ExecutorState}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.ExternalShuffleService
import org.apache.spark.deploy.master.{DriverState, Master}
import org.apache.spark.deploy.worker.ui.WorkerWebUI
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.rpc._
import org.apache.spark.util.{ThreadUtils, SignalLogger, Utils}

private[deploy] class Worker(
    override val rpcEnv: RpcEnv,
    webUiPort: Int,
    cores: Int,
    memory: Int,
    masterRpcAddresses: Array[RpcAddress],
    systemName: String,
    endpointName: String,
    workDirPath: String = null,
    val conf: SparkConf,
    val securityMgr: SecurityManager)
  extends ThreadSafeRpcEndpoint with Logging {

  private val host = rpcEnv.address.host
  private val port = rpcEnv.address.port

  Utils.checkHost(host, "Expected hostname")
  assert (port > 0)

  // A scheduled executor used to send messages at the specified time.
  private val forwordMessageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-forward-message-scheduler") //定期发送心跳调度器

  // A separated thread to clean up the workDir. Used to provide the implicit parameter of `Future`
  // methods.清理app没有执行者在执行的app文件夹
  private val cleanupThreadExecutor = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonSingleThreadExecutor("worker-cleanup-thread"))

  // For worker and executor IDs
  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
  // Send a heartbeat every (heartbeat timeout) / 4 milliseconds
  private val HEARTBEAT_MILLIS = conf.getLong("spark.worker.timeout", 60) * 1000 / 4 //心跳周期

  // Model retries to connect to the master, after Hadoop's model.
  // The first six attempts to reconnect are in shorter intervals (between 5 and 15 seconds)
  // Afterwards, the next 10 attempts are between 30 and 90 seconds.
  // A bit of randomness is introduced so that not all of the workers attempt to reconnect at
  // the same time.
  private val INITIAL_REGISTRATION_RETRIES = 6
  private val TOTAL_REGISTRATION_RETRIES = INITIAL_REGISTRATION_RETRIES + 10 //最多允许的尝试注册次数
  private val FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND = 0.500
  private val REGISTRATION_RETRY_FUZZ_MULTIPLIER = {
    val randomNumberGenerator = new Random(UUID.randomUUID.getMostSignificantBits)
    randomNumberGenerator.nextDouble + FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND
  }
  
  //初始化,连接master的时候,要延迟多久再连接
  private val INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS = (math.round(10 *
    REGISTRATION_RETRY_FUZZ_MULTIPLIER))
  private val PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS = (math.round(60
    * REGISTRATION_RETRY_FUZZ_MULTIPLIER))

  private val CLEANUP_ENABLED = conf.getBoolean("spark.worker.cleanup.enabled", false) //true表示是否要定期worker自动清理内部内容
  // How often worker will clean up old app folders 定期清理worker上的内容时间间隔
  private val CLEANUP_INTERVAL_MILLIS =
    conf.getLong("spark.worker.cleanup.interval", 60 * 30) * 1000
  // TTL for app folders/data;  after TTL expires it will be cleaned up
  private val APP_DATA_RETENTION_SECONDS =
    conf.getLong("spark.worker.cleanup.appDataTtl", 7 * 24 * 3600)

  private val testing: Boolean = sys.props.contains("spark.testing")
  
  private var master: Option[RpcEndpointRef] = None //与master通信的master通道
  private var activeMasterUrl: String = "" //与master通信的master的地址
  private[worker] var activeMasterWebUiUrl : String = "" //与master通信的master的webUiUrl
  private val workerUri = rpcEnv.uriOf(systemName, rpcEnv.address, endpointName)
  private var registered = false //true表示已经注册到master了
  private var connected = false//成功与master建立连接
  
  //workerId的格式:worker-yyyyMMddHHmmss-host-port
  private val workerId = generateWorkerId()
  
  //获取spark的home File对象
  private val sparkHome =
    if (testing) {
      assert(sys.props.contains("spark.test.home"), "spark.test.home is not set!")
      new File(sys.props("spark.test.home"))
    } else {
      new File(sys.env.get("SPARK_HOME").getOrElse("."))
    }

  var workDir: File = null //该worker的工作目录
  
  //key是appId/execId,value是对应的执行者对象ExecutorRunner
  val executors = new HashMap[String, ExecutorRunner]
  //key是appId/execId,value是对应的已经按成的执行者对象ExecutorRunner
  val finishedExecutors = new LinkedHashMap[String, ExecutorRunner]
  
  //该worker上执行的driver集合,key是DriverRunner.driverId value是对应的DriverRunner
  val drivers = new HashMap[String, DriverRunner]
  val finishedDrivers = new LinkedHashMap[String, DriverRunner]
  
  //key是appId,value是该app相关的执行者存储的路径集合,即:root/executor-uuid的文件夹集合
  val appDirectories = new HashMap[String, Seq[String]]
  val finishedApps = new HashSet[String]

  //针对已经完成的执行者,确认其中保留在worker内存中的数量,则移除10%已经完成的执行者
  val retainedExecutors = conf.getInt("spark.worker.ui.retainedExecutors",
    WorkerWebUI.DEFAULT_RETAINED_EXECUTORS)
    
  //针对已经完成的driver,确认其中保留在worker内存中的数量,则移除10%已经完成的执行者
  val retainedDrivers = conf.getInt("spark.worker.ui.retainedDrivers",
    WorkerWebUI.DEFAULT_RETAINED_DRIVERS)

  // The shuffle service is not actually started unless configured.
  private val shuffleService = new ExternalShuffleService(conf, securityMgr)

  private val publicAddress = {
    val envVar = conf.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else host
  }
  private var webUi: WorkerWebUI = null

  private var connectionAttemptCount = 0 //尝试连接master的次数

  private val metricsSystem = MetricsSystem.createMetricsSystem("worker", conf, securityMgr)
  private val workerSource = new WorkerSource(this)

  private var registerMasterFutures: Array[JFuture[_]] = null //与master建立连接,并且向master提交worker信息,采用future模式,因此返回该future集合
  private var registrationRetryTimer: Option[JScheduledFuture[_]] = None //每隔一定周期,就要向自己发送ReregisterWithMaster,去重新注册worker

  // A thread pool for registering with masters. Because registering with a master is a blocking
  // action, this thread pool must be able to create "masterRpcAddresses.size" threads at the same
  // time so that we can register with all masters.线程池
  private val registerMasterThreadPool = new ThreadPoolExecutor(
    0,//初始化0个线程
    masterRpcAddresses.size, // Make sure we can register with all masters at the same time最多允许有这些个线程
    60L, //idle线程超过60单位时间,则将线程销毁
    TimeUnit.SECONDS,//单位时间
    new SynchronousQueue[Runnable](),
    ThreadUtils.namedThreadFactory("worker-register-master-threadpool"))

  var coresUsed = 0 //该worker已经使用的cpu数量
  var memoryUsed = 0 //该worker已经使用的内存数量

  def coresFree: Int = cores - coresUsed //该worker剩余cpu数量
  def memoryFree: Int = memory - memoryUsed //该worker剩余内存数量

  //创建工作目录,默认是spark_home/work目录
  private def createWorkDir() {
    workDir = Option(workDirPath).map(new File(_)).getOrElse(new File(sparkHome, "work"))
    try {
      // This sporadically fails - not sure why ... !workDir.exists() && !workDir.mkdirs()
      // So attempting to create and then check if directory was created or not.
      workDir.mkdirs()
      if ( !workDir.exists() || !workDir.isDirectory) {
        logError("Failed to create work directory " + workDir)
        System.exit(1)
      }
      assert (workDir.isDirectory)
    } catch {
      case e: Exception =>
        logError("Failed to create work directory " + workDir, e)
        System.exit(1)
    }
  }

  override def onStart() {
    assert(!registered)
    logInfo("Starting Spark worker %s:%d with %d cores, %s RAM".format(
      host, port, cores, Utils.megabytesToString(memory)))
    logInfo(s"Running Spark version ${org.apache.spark.SPARK_VERSION}")
    logInfo("Spark home: " + sparkHome)
    createWorkDir() //创建工作目录
    shuffleService.startIfEnabled()
    webUi = new WorkerWebUI(this, workDir, webUiPort)
    webUi.bind()
    registerWithMaster()

    metricsSystem.registerSource(workerSource)
    metricsSystem.start()
    // Attach the worker metrics servlet handler to the web ui after the metrics system is started.
    metricsSystem.getServletHandlers.foreach(webUi.attachHandler)
  }

  //master更换了,参数是新的master通道与webUiUrl
  private def changeMaster(masterRef: RpcEndpointRef, uiUrl: String) {
    // activeMasterUrl it's a valid Spark url since we receive it from master.
    activeMasterUrl = masterRef.address.toSparkURL ////与master通信的master的地址
    activeMasterWebUiUrl = uiUrl //与master通信的master的webUiUrl
    master = Some(masterRef)
    connected = true
    // Cancel any outstanding re-registration attempts because we found a new master
    //因为发现了一个新的master,所以取消以前注册的master信息
    cancelLastRegistrationRetry()
  }

  //使用线程池,尝试去连接每一个master
  private def tryRegisterAllMasters(): Array[JFuture[_]] = {
    masterRpcAddresses.map { masterAddress =>
      registerMasterThreadPool.submit(new Runnable {
        override def run(): Unit = {
          try {
            //打印日志,连接一个master
            logInfo("Connecting to master " + masterAddress + "...")
            //向master建立一个连接
            val masterEndpoint =
              rpcEnv.setupEndpointRef(Master.SYSTEM_NAME, masterAddress, Master.ENDPOINT_NAME)
              //向master发送注册该worker
            masterEndpoint.send(RegisterWorker(
              workerId, host, port, self, cores, memory, webUi.boundPort, publicAddress))
          } catch {
            case ie: InterruptedException => // Cancelled
            case NonFatal(e) => logWarning(s"Failed to connect to master $masterAddress", e)
          }
        }
      })
    }
  }

  /**
   * Re-register with the master because a network failure or a master failure has occurred.
   * If the re-registration attempt threshold is exceeded, the worker exits with error.
   * Note that for thread-safety this should only be called from the rpcEndpoint.
   */
  private def reregisterWithMaster(): Unit = {
    Utils.tryOrExit {
      connectionAttemptCount += 1
      if (registered) {
        cancelLastRegistrationRetry()
      } else if (connectionAttemptCount <= TOTAL_REGISTRATION_RETRIES) {
        logInfo(s"Retrying connection to master (attempt # $connectionAttemptCount)")
        /**
         * Re-register with the active master this worker has been communicating with. If there
         * is none, then it means this worker is still bootstrapping and hasn't established a
         * connection with a master yet, in which case we should re-register with all masters.
         *
         * It is important to re-register only with the active master during failures. Otherwise,
         * if the worker unconditionally attempts to re-register with all masters, the following
         * race condition may arise and cause a "duplicate worker" error detailed in SPARK-4592:
         *
         *   (1) Master A fails and Worker attempts to reconnect to all masters
         *   (2) Master B takes over and notifies Worker
         *   (3) Worker responds by registering with Master B
         *   (4) Meanwhile, Worker's previous reconnection attempt reaches Master B,
         *       causing the same Worker to register with Master B twice
         *
         * Instead, if we only register with the known active master, we can assume that the
         * old master must have died because another master has taken over. Note that this is
         * still not safe if the old master recovers within this interval, but this is a much
         * less likely scenario.
         */
        master match {
          case Some(masterRef) =>
            // registered == false && master != None means we lost the connection to master, so
            // masterRef cannot be used and we need to recreate it again. Note: we must not set
            // master to None due to the above comments.
            if (registerMasterFutures != null) {
              registerMasterFutures.foreach(_.cancel(true))
            }
            val masterAddress = masterRef.address
            registerMasterFutures = Array(registerMasterThreadPool.submit(new Runnable {
              override def run(): Unit = {
                try {
                  logInfo("Connecting to master " + masterAddress + "...")
                  val masterEndpoint =
                    rpcEnv.setupEndpointRef(Master.SYSTEM_NAME, masterAddress, Master.ENDPOINT_NAME)
                  masterEndpoint.send(RegisterWorker(
                    workerId, host, port, self, cores, memory, webUi.boundPort, publicAddress))
                } catch {
                  case ie: InterruptedException => // Cancelled
                  case NonFatal(e) => logWarning(s"Failed to connect to master $masterAddress", e)
                }
              }
            }))
          case None =>
            if (registerMasterFutures != null) {
              registerMasterFutures.foreach(_.cancel(true))
            }
            // We are retrying the initial registration
            registerMasterFutures = tryRegisterAllMasters()
        }
        // We have exceeded the initial registration retry threshold
        // All retries from now on should use a higher interval
        if (connectionAttemptCount == INITIAL_REGISTRATION_RETRIES) {
          registrationRetryTimer.foreach(_.cancel(true))
          registrationRetryTimer = Some(
            forwordMessageScheduler.scheduleAtFixedRate(new Runnable {
              override def run(): Unit = Utils.tryLogNonFatalError {
                self.send(ReregisterWithMaster)
              }
            }, PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS,
              PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS,
              TimeUnit.SECONDS))
        }
      } else {
        logError("All masters are unresponsive! Giving up.")
        System.exit(1)
      }
    }
  }

  /**
   * Cancel last registeration retry, or do nothing if no retry
   * 因为发现了一个新的master,所以取消以前注册的master信息
   */
  private def cancelLastRegistrationRetry(): Unit = {
    if (registerMasterFutures != null) {
      registerMasterFutures.foreach(_.cancel(true))
      registerMasterFutures = null
    }
    registrationRetryTimer.foreach(_.cancel(true))
    registrationRetryTimer = None
  }

  //与master建立连接
  private def registerWithMaster() {
    // onDisconnected may be triggered multiple times, so don't attempt registration
    // if there are outstanding registration attempts scheduled.
    registrationRetryTimer match {
      case None =>
        registered = false
        //与master建立连接,并且向master提交worker信息,采用future模式,因此返回该future集合
        registerMasterFutures = tryRegisterAllMasters()
        connectionAttemptCount = 0 //初始化尝试连接次数为0
        //每隔一定周期,就要向自己发送ReregisterWithMaster,去重新注册worker
        registrationRetryTimer = Some(forwordMessageScheduler.scheduleAtFixedRate(
          new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              self.send(ReregisterWithMaster)
            }
          },
          INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS,//执行前延迟多久
          INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS,//执行周期
          TimeUnit.SECONDS))
      case Some(_) =>
        logInfo("Not spawning another attempt to register with the master, since there is an" +
          " attempt scheduled already.")
    }
  }

  override def receive: PartialFunction[Any, Unit] = {
    case RegisteredWorker(masterRef, masterWebUiUrl) => //成功注册到master后,master发给worker的信息,参数是master的通道与master的web ui路径
      logInfo("Successfully registered with master " + masterRef.address.toSparkURL)
      registered = true
      changeMaster(masterRef, masterWebUiUrl) //maste已经注册了一个,因此更改注册信息
      
      //定期发送心跳调度器
      forwordMessageScheduler.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = Utils.tryLogNonFatalError {
          self.send(SendHeartbeat)
        }
      }, 0, HEARTBEAT_MILLIS, TimeUnit.MILLISECONDS)
      
      //定期清理worker上的内容时间间隔
      if (CLEANUP_ENABLED) {
        logInfo(s"Worker cleanup enabled; old application directories will be deleted in: $workDir")
        forwordMessageScheduler.scheduleAtFixedRate(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            self.send(WorkDirCleanup)
          }
        }, CLEANUP_INTERVAL_MILLIS, CLEANUP_INTERVAL_MILLIS, TimeUnit.MILLISECONDS)
      }

    case SendHeartbeat =>
      if (connected) { sendToMaster(Heartbeat(workerId, self)) } //将worker自己的通道与workerId发送给master

    case WorkDirCleanup => //定期清理worker上的内容时间间隔
      // Spin up a separate thread (in a future) to do the dir cleanup; don't tie up worker
      // rpcEndpoint.
      // Copy ids so that it can be used in the cleanup thread.
      val appIds = executors.values.map(_.appId).toSet //查找所有执行者所属的app集合
      val cleanupFuture = concurrent.future {//future模式
        val appDirs = workDir.listFiles()
        if (appDirs == null) {
          throw new IOException("ERROR: Failed to list files in " + appDirs)
        }
        
        //清理app没有执行者在执行的app文件夹
        //返回 app没有执行者在执行的app集合
        appDirs.filter { dir =>
          // the directory is used by an application - check that the application is not running
          // when cleaning up
          val appIdFromDir = dir.getName
          val isAppStillRunning = appIds.contains(appIdFromDir) //true 说明该app还有执行者在运行
          
          //如果app没有执行者在执行
          dir.isDirectory && !isAppStillRunning &&
          !Utils.doesDirectoryContainAnyNewFiles(dir, APP_DATA_RETENTION_SECONDS)
        }.foreach { dir =>
          logInfo(s"Removing directory: ${dir.getPath}")
          Utils.deleteRecursively(dir) //真正去删除该目录
        }
      }(cleanupThreadExecutor) 

      cleanupFuture.onFailure {
        case e: Throwable =>
          logError("App dir cleanup failed: " + e.getMessage, e)
      }(cleanupThreadExecutor)

    case MasterChanged(masterRef, masterWebUiUrl) =>
      logInfo("Master has changed, new master is at " + masterRef.address.toSparkURL)
      changeMaster(masterRef, masterWebUiUrl)

      val execs = executors.values.
        map(e => new ExecutorDescription(e.appId, e.execId, e.cores, e.state))
      masterRef.send(WorkerSchedulerStateResponse(workerId, execs.toList, drivers.keys.toSeq))

    case RegisterWorkerFailed(message) =>
      if (!registered) { //注册失败,异常退出程序
        logError("Worker registration failed: " + message)
        System.exit(1)
      }

    //master请求worker,让worker重新连接该master
    case ReconnectWorker(masterUrl) =>
      logInfo(s"Master with url $masterUrl requested this worker to reconnect.")
      registerWithMaster()

     //启动一个执行者,master传递了master的url、appId、执行者的ID、app的描述、需要的cpu和内存
    case LaunchExecutor(masterUrl, appId, execId, appDesc, cores_, memory_) =>
      if (masterUrl != activeMasterUrl) {//如果master的url与worker上缓存的master的url不一致,说明master有更改,worker不知道,或者其他原因,总之master不可信,可以忽略该请求
        logWarning("Invalid Master (" + masterUrl + ") attempted to launch executor.")
      } else {
        try {
          //打印日志,说尝试去启动一个执行者
          logInfo("Asked to launch executor %s/%d for %s".format(appId, execId, appDesc.name))

          // Create the executor's working directory 执行者的工作目录是worker/appID/execId
          val executorDir = new File(workDir, appId + "/" + execId)
          if (!executorDir.mkdirs()) {//创建该工作目录
            throw new IOException("Failed to create directory " + executorDir)
          }

          // Create local dirs for the executor. These are passed to the executor via the
          // SPARK_EXECUTOR_DIRS environment variable, and deleted by the Worker when the
          // application finishes.
          //创建本地的Root根目录,根目录集合被返回,在root/executor-uuid的文件夹集合
          val appLocalDirs = appDirectories.get(appId).getOrElse {
            Utils.getOrCreateLocalRootDirs(conf).map { dir =>
              val appDir = Utils.createDirectory(dir, namePrefix = "executor")
              Utils.chmod700(appDir)
              appDir.getAbsolutePath()
            }.toSeq
          }
          appDirectories(appId) = appLocalDirs
          
          val manager = new ExecutorRunner(
            appId,
            execId,
            appDesc.copy(command = Worker.maybeUpdateSSLSettings(appDesc.command, conf)),
            cores_,
            memory_,
            self,
            workerId,
            host,
            webUi.boundPort,
            publicAddress,
            sparkHome,
            executorDir,
            workerUri,
            conf,
            appLocalDirs, ExecutorState.LOADING)//默认是装载中
          
          //设置内存映射关系
          executors(appId + "/" + execId) = manager
          
          //启动该执行者
          manager.start()
          
          //计算使用的资源情况
          coresUsed += cores_
          memoryUsed += memory_
          //向master发送信息,通知master执行者已经更改完状态
          sendToMaster(ExecutorStateChanged(appId, execId, manager.state, None, None))
        } catch {
          case e: Exception => {
            logError(s"Failed to launch executor $appId/$execId for ${appDesc.name}.", e)
            if (executors.contains(appId + "/" + execId)) {
              executors(appId + "/" + execId).kill()
              executors -= appId + "/" + execId
            }
            sendToMaster(ExecutorStateChanged(appId, execId, ExecutorState.FAILED,
              Some(e.toString), None))
          }
        }
      }

    //执行者状态变更
    case executorStateChanged @ ExecutorStateChanged(appId, execId, state, message, exitStatus) =>
      handleExecutorStateChanged(executorStateChanged)

    //杀死一个执行者
    case KillExecutor(masterUrl, appId, execId) =>
      if (masterUrl != activeMasterUrl) {//判断worker持有的master是否与命令的master相同,如果不同则可以忽略,因为传递命令的master是非法的master
        logWarning("Invalid Master (" + masterUrl + ") attempted to launch executor " + execId)
      } else {
        val fullId = appId + "/" + execId //对应的执行者对象ExecutorRunner
        executors.get(fullId) match {
          case Some(executor) =>
            logInfo("Asked to kill executor " + fullId)
            executor.kill() //执行ExecutorRunner的kill方法
          case None =>
            logInfo("Asked to kill unknown executor " + fullId)
        }
      }

    //在worker上启动一个driver,给出driverId和driver的描述
    case LaunchDriver(driverId, driverDesc) => {
      logInfo(s"Asked to launch driver $driverId")
      val driver = new DriverRunner(
        conf,
        driverId,
        workDir,
        sparkHome,
        driverDesc.copy(command = Worker.maybeUpdateSSLSettings(driverDesc.command, conf)),
        self,
        workerUri,
        securityMgr)
      drivers(driverId) = driver
      driver.start()

      //记录被使用的资源
      coresUsed += driverDesc.cores
      memoryUsed += driverDesc.mem
    }

    case KillDriver(driverId) => { //接受master传递过来的要kill一个driver
      logInfo(s"Asked to kill driver $driverId")
      drivers.get(driverId) match { //调用DriverRunner的kill方法
        case Some(runner) =>
          runner.kill()
        case None =>
          logError(s"Asked to kill unknown driver $driverId")
      }
    }

    //driver状态有更改
    case driverStateChanged @ DriverStateChanged(driverId, state, exception) => {
      handleDriverStateChanged(driverStateChanged)
    }

    //重新注册master
    case ReregisterWithMaster =>
      reregisterWithMaster()

    //一个app已经完成
    case ApplicationFinished(id) =>
      finishedApps += id
      maybeCleanupApplication(id) //尝试看看是否要删除该应用,参数是appId
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RequestWorkerState =>
      context.reply(WorkerStateResponse(host, port, workerId, executors.values.toList,
        finishedExecutors.values.toList, drivers.values.toList,
        finishedDrivers.values.toList, activeMasterUrl, cores, memory,
        coresUsed, memoryUsed, activeMasterWebUiUrl))
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    if (master.exists(_.address == remoteAddress)) {
      logInfo(s"$remoteAddress Disassociated !")
      masterDisconnected()
    }
  }

  private def masterDisconnected() {
    logError("Connection to master failed! Waiting for master to reconnect...")
    connected = false
    registerWithMaster()
  }

  //尝试看看是否要删除该应用,参数是appId
  private def maybeCleanupApplication(id: String): Unit = {
    //如果app已经完成,并且没有执行者属于该app了,则返回true
    val shouldCleanup = finishedApps.contains(id) && !executors.values.exists(_.appId == id)
    if (shouldCleanup) {//去真正删除该app在work上的信息
      finishedApps -= id //移除该app
      //移除该app在worker上的目录
      appDirectories.remove(id).foreach { dirList =>
        logInfo(s"Cleaning up local directories for application $id")
        dirList.foreach { dir =>
          Utils.deleteRecursively(new File(dir))
        }
      }
      shuffleService.applicationRemoved(id)
    }
  }

  /**
   * Send a message to the current master. If we have not yet registered successfully with any
   * master, the message will be dropped.
   * 将worker自己的通道与workerId发送给master
   * @message Heartbeat(workerId, self)对象
   */
  private def sendToMaster(message: Any): Unit = {
    master match {
      case Some(masterRef) => masterRef.send(message) //向master通道发送message对象
      case None =>
        logWarning(
          s"Dropping $message because the connection to master has not yet been established")
    }
  }

  //workerId的格式:worker-yyyyMMddHHmmss-host-port
  private def generateWorkerId(): String = {
    "worker-%s-%s-%d".format(createDateFormat.format(new Date), host, port)
  }

  override def onStop() {
    cleanupThreadExecutor.shutdownNow()
    metricsSystem.report()
    cancelLastRegistrationRetry()
    forwordMessageScheduler.shutdownNow()
    registerMasterThreadPool.shutdownNow()
    executors.values.foreach(_.kill())
    drivers.values.foreach(_.kill())
    shuffleService.stop()
    webUi.stop()
    metricsSystem.stop()
  }

  //去决定是否移除一些已经完成的执行者
  private def trimFinishedExecutorsIfNecessary(): Unit = {
    // do not need to protect with locks since both WorkerPage and Restful server get data through
    // thread-safe RpcEndPoint
    if (finishedExecutors.size > retainedExecutors) { //已经完成的执行者超过一定数量,则移除10%已经完成的执行者
      finishedExecutors.take(math.max(finishedExecutors.size / 10, 1)).foreach {
        case (executorId, _) => finishedExecutors.remove(executorId)
      }
    }
  }

  //去决定是否移除一些已经完成的driver
  private def trimFinishedDriversIfNecessary(): Unit = {
    // do not need to protect with locks since both WorkerPage and Restful server get data through
    // thread-safe RpcEndPoint
    if (finishedDrivers.size > retainedDrivers) {  //针对已经完成的driver,确认其中保留在worker内存中的数量,则移除10%已经完成的执行者
      finishedDrivers.take(math.max(finishedDrivers.size / 10, 1)).foreach {
        case (driverId, _) => finishedDrivers.remove(driverId)
      }
    }
  }

  //driver状态有更改
  private[worker] def handleDriverStateChanged(driverStateChanged: DriverStateChanged): Unit = {
    val driverId = driverStateChanged.driverId
    val exception = driverStateChanged.exception
    val state = driverStateChanged.state
    state match {
      case DriverState.ERROR =>
        logWarning(s"Driver $driverId failed with unrecoverable exception: ${exception.get}")
      case DriverState.FAILED =>
        logWarning(s"Driver $driverId exited with failure")
      case DriverState.FINISHED =>
        logInfo(s"Driver $driverId exited successfully")
      case DriverState.KILLED =>
        logInfo(s"Driver $driverId was killed by user")
      case _ =>
        logDebug(s"Driver $driverId changed state to $state")
    }
    sendToMaster(driverStateChanged) //向master发送该driver的状态信息
    //移除该driver
    val driver = drivers.remove(driverId).get
    finishedDrivers(driverId) = driver
    
    trimFinishedDriversIfNecessary() //去决定是否移除一些已经完成的driver
    
    //还原资源情况
    memoryUsed -= driver.driverDesc.mem
    coresUsed -= driver.driverDesc.cores
  }

  //更新一个执行者的状态
  private[worker] def handleExecutorStateChanged(executorStateChanged: ExecutorStateChanged):
    Unit = {
    sendToMaster(executorStateChanged) //向master发送该执行者的状态
    val state = executorStateChanged.state 
    if (ExecutorState.isFinished(state)) { //判断执行者状态是否完成
      val appId = executorStateChanged.appId
      val fullId = appId + "/" + executorStateChanged.execId
      val message = executorStateChanged.message
      val exitStatus = executorStateChanged.exitStatus
      executors.get(fullId) match {//查找对应的执行者对象
        case Some(executor) =>
          logInfo("Executor " + fullId + " finished with state " + state +
            message.map(" message " + _).getOrElse("") +
            exitStatus.map(" exitStatus " + _).getOrElse("")) //记录执行者要完成的状态
          executors -= fullId
          finishedExecutors(fullId) = executor
          
          //去决定是否移除一些已经完成的执行者
          trimFinishedExecutorsIfNecessary()
          
          //减少内部的资源信息
          coresUsed -= executor.cores
          memoryUsed -= executor.memory
        case None =>
          logInfo("Unknown Executor " + fullId + " finished with state " + state +
            message.map(" message " + _).getOrElse("") +
            exitStatus.map(" exitStatus " + _).getOrElse(""))
      }
      
      //尝试看看是否要删除该应用
      maybeCleanupApplication(appId)
    }
  }
}

private[deploy] object Worker extends Logging {
  val SYSTEM_NAME = "sparkWorker"
  val ENDPOINT_NAME = "Worker"

  def main(argStrings: Array[String]) {
    SignalLogger.register(log)
    val conf = new SparkConf
    val args = new WorkerArguments(argStrings, conf)
    val rpcEnv = startRpcEnvAndEndpoint(args.host, args.port, args.webUiPort, args.cores,
      args.memory, args.masters, args.workDir)
    rpcEnv.awaitTermination()
  }

  def startRpcEnvAndEndpoint(
      host: String,
      port: Int,
      webUiPort: Int,
      cores: Int,
      memory: Int,
      masterUrls: Array[String],//master的url集合
      workDir: String,
      workerNumber: Option[Int] = None,
      conf: SparkConf = new SparkConf): RpcEnv = {

    // The LocalSparkCluster runs multiple local sparkWorkerX RPC Environments
    val systemName = SYSTEM_NAME + workerNumber.map(_.toString).getOrElse("")
    val securityMgr = new SecurityManager(conf)
    val rpcEnv = RpcEnv.create(systemName, host, port, conf, securityMgr)
    val masterAddresses = masterUrls.map(RpcAddress.fromSparkURL(_)) //获取master的host和port 集合
    rpcEnv.setupEndpoint(ENDPOINT_NAME, new Worker(rpcEnv, webUiPort, cores, memory,
      masterAddresses, systemName, ENDPOINT_NAME, workDir, conf, securityMgr))
    rpcEnv
  }

  //true表示-Dspark.ssl.useNodeLocalConf属性配置为true
  def isUseLocalNodeSSLConfig(cmd: Command): Boolean = {
    val pattern = """\-Dspark\.ssl\.useNodeLocalConf\=(.+)""".r
    val result = cmd.javaOpts.collectFirst {
      case pattern(_result) => _result.toBoolean
    }
    result.getOrElse(false)
  }

  def maybeUpdateSSLSettings(cmd: Command, conf: SparkConf): Command = {
    val prefix = "spark.ssl."
    val useNLC = "spark.ssl.useNodeLocalConf"
    if (isUseLocalNodeSSLConfig(cmd)) {
      val newJavaOpts = cmd.javaOpts
          .filter(opt => !opt.startsWith(s"-D$prefix")) ++
          conf.getAll.collect { case (key, value) if key.startsWith(prefix) => s"-D$key=$value" } :+
          s"-D$useNLC=true"
      cmd.copy(javaOpts = newJavaOpts)
    } else {
      cmd
    }
  }
}
