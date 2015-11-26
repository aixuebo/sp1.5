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

package org.apache.spark.deploy.master

import java.io.FileNotFoundException
import java.net.URLEncoder
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.{ScheduledFuture, TimeUnit}

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.language.postfixOps
import scala.util.Random

import org.apache.hadoop.fs.Path

import org.apache.spark.rpc._
import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.{ApplicationDescription, DriverDescription,
  ExecutorState, SparkHadoopUtil}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.history.HistoryServer
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.deploy.master.MasterMessages._
import org.apache.spark.deploy.master.ui.MasterWebUI
import org.apache.spark.deploy.rest.StandaloneRestServer
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.scheduler.{EventLoggingListener, ReplayListenerBus}
import org.apache.spark.serializer.{JavaSerializer, Serializer}
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.{ThreadUtils, SignalLogger, Utils}

private[deploy] class Master(
    override val rpcEnv: RpcEnv,
    address: RpcAddress,
    webUiPort: Int,
    val securityMgr: SecurityManager,
    val conf: SparkConf)
  extends ThreadSafeRpcEndpoint with Logging with LeaderElectable {

  private val forwardMessageThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-forward-message-thread")

  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)//hadoop对应的配置信息

  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss") // For application IDs

  private val WORKER_TIMEOUT_MS = conf.getLong("spark.worker.timeout", 60) * 1000 //最后worker心跳间隔,超过该时间内没有心跳,说明该worker死了
  private val RETAINED_APPLICATIONS = conf.getInt("spark.deploy.retainedApplications", 200) //即当application完成的时候,要保持多少个已经完成的application
  private val RETAINED_DRIVERS = conf.getInt("spark.deploy.retainedDrivers", 200) //即当driver完成的时候,要保持多少个已经完成的driver
  private val REAPER_ITERATIONS = conf.getInt("spark.dead.worker.persistence", 15) //对死亡的worker节点,要重复执行多少次,才确定将其移除worker集合
  private val RECOVERY_MODE = conf.get("spark.deploy.recoveryMode", "NONE") //恢复模式



  //#######worker映射########################
  //当前注册的所有worker集合,如果worker被移除,仅仅设置其死亡,不从内存中删除
  val workers = new HashSet[WorkerInfo]
  //key是worker.id,value是对应的worker对象
  private val idToWorker = new HashMap[String, WorkerInfo]
  //key是WorkerInfo.endpoint.address,value是对应的workder对象
  private val addressToWorker = new HashMap[RpcAddress, WorkerInfo]
  //###############################
  
  //#######driver映射########################
  //存储所有的driver,当driver完成时,会从该内存映射中删除掉
  private val drivers = new HashSet[DriverInfo]
  //已经完成的driver集合
  private val completedDrivers = new ArrayBuffer[DriverInfo]
  // Drivers currently spooled for scheduling
  //说明master收到了哪些driver,但是还没有任何调度,仅仅是RequestSubmitDriver函数中driver被提交给master了
  private val waitingDrivers = new ArrayBuffer[DriverInfo]
  private var nextDriverNumber = 0 //driver的自增长序列
  //###############################
  
  //#######app映射########################
  //系统内所有的app信息
  val apps = new HashSet[ApplicationInfo]
  //ApplicationInfo.id与ApplicationInfo对象的映射关系
  val idToApp = new HashMap[String, ApplicationInfo]
  //等候,没有被调度的app集合
  val waitingApps = new ArrayBuffer[ApplicationInfo]
  //application与driver所在节点通信的通道作为key,value是对应的ApplicationInfo对象
  private val endpointToApp = new HashMap[RpcEndpointRef, ApplicationInfo]
  //key是app在哪个地址上,value是该地址的app对象
  private val addressToApp = new HashMap[RpcAddress, ApplicationInfo]
  //完成的app集合
  private val completedApps = new ArrayBuffer[ApplicationInfo]
  private var nextAppNumber = 0
  //###############################

  private val appIdToUI = new HashMap[String, SparkUI]

  Utils.checkHost(address.host, "Expected hostname")

  private val masterMetricsSystem = MetricsSystem.createMetricsSystem("master", conf, securityMgr)
  private val applicationMetricsSystem = MetricsSystem.createMetricsSystem("applications", conf,
    securityMgr)
  private val masterSource = new MasterSource(this)

  // After onStart, webUi will be set
  private var webUi: MasterWebUI = null

  //master的host
  private val masterPublicAddress = {
    val envVar = conf.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else address.host
  }

  private val masterUrl = address.toSparkURL //master的host,spark://host:port
  private var masterWebUiUrl: String = _ //http://masterhost:webPort web页面

  private var state = RecoveryState.STANDBY

  private var persistenceEngine: PersistenceEngine = _ //存储数据信息的引擎,是文件系统还是zookeeper还是自定义

  private var leaderElectionAgent: LeaderElectionAgent = _ //选举master的代理类

  private var recoveryCompletionTask: ScheduledFuture[_] = _

  private var checkForWorkerTimeOutTask: ScheduledFuture[_] = _

  // As a temporary workaround before better ways of configuring memory, we allow users to set
  // a flag that will perform round-robin scheduling across the nodes (spreading out each app
  // among all the nodes) instead of trying to consolidate each app onto a small # of nodes.
  private val spreadOutApps = conf.getBoolean("spark.deploy.spreadOut", true)

  // Default maxCores for applications that don't specify it (i.e. pass Int.MaxValue)
  private val defaultCores = conf.getInt("spark.deploy.defaultCores", Int.MaxValue)
  if (defaultCores < 1) {
    throw new SparkException("spark.deploy.defaultCores must be positive")
  }

  // Alternative application submission gateway that is stable across Spark versions
  private val restServerEnabled = conf.getBoolean("spark.master.rest.enabled", true)
  private var restServer: Option[StandaloneRestServer] = None
  private var restServerBoundPort: Option[Int] = None

  override def onStart(): Unit = {
    logInfo("Starting Spark master at " + masterUrl) //打印日志在host+port上开启了master
    logInfo(s"Running Spark version ${org.apache.spark.SPARK_VERSION}") //正在运行的spark的版本
    
    //master上的web页面
    webUi = new MasterWebUI(this, webUiPort)
    webUi.bind()
    masterWebUiUrl = "http://" + masterPublicAddress + ":" + webUi.boundPort
    
    
    checkForWorkerTimeOutTask = forwardMessageThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        self.send(CheckForWorkerTimeOut)
      }
    }, 0, WORKER_TIMEOUT_MS, TimeUnit.MILLISECONDS)

    if (restServerEnabled) {
      val port = conf.getInt("spark.master.rest.port", 6066)
      restServer = Some(new StandaloneRestServer(address.host, port, conf, self, masterUrl))
    }
    restServerBoundPort = restServer.map(_.start())

    masterMetricsSystem.registerSource(masterSource)
    masterMetricsSystem.start()
    applicationMetricsSystem.start()
    // Attach the master and app metrics servlet handler to the web ui after the metrics systems are
    // started.
    masterMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)
    applicationMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)

    val serializer = new JavaSerializer(conf)
    
    //设置 "存储数据信息的引擎,是文件系统还是zookeeper还是自定义" 和 选举master的代理类
    val (persistenceEngine_, leaderElectionAgent_) = RECOVERY_MODE match {
      case "ZOOKEEPER" =>
        logInfo("Persisting recovery state to ZooKeeper")
        val zkFactory =
          new ZooKeeperRecoveryModeFactory(conf, serializer)
        (zkFactory.createPersistenceEngine(), zkFactory.createLeaderElectionAgent(this))
      case "FILESYSTEM" =>
        val fsFactory =
          new FileSystemRecoveryModeFactory(conf, serializer)
        (fsFactory.createPersistenceEngine(), fsFactory.createLeaderElectionAgent(this))
      case "CUSTOM" =>
        val clazz = Utils.classForName(conf.get("spark.deploy.recoveryMode.factory"))
        val factory = clazz.getConstructor(classOf[SparkConf], classOf[Serializer])
          .newInstance(conf, serializer)
          .asInstanceOf[StandaloneRecoveryModeFactory]
        (factory.createPersistenceEngine(), factory.createLeaderElectionAgent(this))
      case _ =>
        (new BlackHolePersistenceEngine(), new MonarchyLeaderAgent(this))
    }
    persistenceEngine = persistenceEngine_
    leaderElectionAgent = leaderElectionAgent_
  }

  override def onStop() {
    masterMetricsSystem.report()
    applicationMetricsSystem.report()
    // prevent the CompleteRecovery message sending to restarted master
    if (recoveryCompletionTask != null) {
      recoveryCompletionTask.cancel(true)
    }
    if (checkForWorkerTimeOutTask != null) {
      checkForWorkerTimeOutTask.cancel(true)
    }
    forwardMessageThread.shutdownNow()
    webUi.stop()
    restServer.foreach(_.stop())
    masterMetricsSystem.stop()
    applicationMetricsSystem.stop()
    persistenceEngine.close()
    leaderElectionAgent.stop()
  }

  override def electedLeader() {
    self.send(ElectedLeader)
  }

  override def revokedLeadership() {
    self.send(RevokedLeadership)
  }

  override def receive: PartialFunction[Any, Unit] = {
    case ElectedLeader => {
      //选举已经成功
      //读取所有数据,分别返回三组集合,即所有app,所有driver,所有worker
      val (storedApps, storedDrivers, storedWorkers) = persistenceEngine.readPersistedData(rpcEnv)
      state = if (storedApps.isEmpty && storedDrivers.isEmpty && storedWorkers.isEmpty) {
        RecoveryState.ALIVE
      } else {
        RecoveryState.RECOVERING
      }
      //打印日志,选举的leader节点现在处于什么状态
      logInfo("I have been elected leader! New state: " + state)
      if (state == RecoveryState.RECOVERING) {
        beginRecovery(storedApps, storedDrivers, storedWorkers) //开始恢复master工作
        recoveryCompletionTask = forwardMessageThread.schedule(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            self.send(CompleteRecovery) //调用master自己,通知master已经恢复完成
          }
        }, WORKER_TIMEOUT_MS, TimeUnit.MILLISECONDS)
      }
    }

    case CompleteRecovery => completeRecovery() //完成恢复工作

    case RevokedLeadership => {
      logError("Leadership has been revoked -- master shutting down.")
      System.exit(0)
    }

    case RegisterWorker(//一个worker注册进来
        id, workerHost, workerPort, workerRef, cores, memory, workerUiPort, publicAddress) => {
      logInfo("Registering worker %s:%d with %d cores, %s RAM".format(
        workerHost, workerPort, cores, Utils.megabytesToString(memory))) //打印日志,关于该worker的详细信息
      if (state == RecoveryState.STANDBY) {
        // ignore, don't send response
      } else if (idToWorker.contains(id)) {//如果该worker已经存在了,则恢复RegisterWorkerFailed信息
        workerRef.send(RegisterWorkerFailed("Duplicate worker ID")) //注册worker失败,worker已经存在
      } else {
        val worker = new WorkerInfo(id, workerHost, workerPort, cores, memory,
          workerRef, workerUiPort, publicAddress)
        if (registerWorker(worker)) { //真正意义去注册一个worker对象
          persistenceEngine.addWorker(worker) //在文件系统中注册该worker
          workerRef.send(RegisteredWorker(self, masterWebUiUrl)) //向worker发送信息,发送master通信的通道和web页面
          schedule() //进行调度,因为worker新增了一个,就说明有新的资源可以被调度了
        } else {
          //发送注册失败信息
          val workerAddress = worker.endpoint.address
          logWarning("Worker registration failed. Attempted to re-register worker at same " +
            "address: " + workerAddress)
          workerRef.send(RegisterWorkerFailed("Attempted to re-register worker at same address: "
            + workerAddress))
        }
      }
    }

    case RegisterApplication(description, driver) => {
      // TODO Prevent repeated registrations from some driver
      if (state == RecoveryState.STANDBY) {
        // ignore, don't send response
      } else {
        logInfo("Registering app " + description.name)
        //@driver 是如何与该application对应的driver通信的通道
        val app = createApplication(description, driver) //创建一个app
        registerApplication(app)
        logInfo("Registered app " + description.name + " with ID " + app.id)
        persistenceEngine.addApplication(app)
        driver.send(RegisteredApplication(app.id, self)) //向driver发送信息,说注册app成功
        schedule()
      }
    }

    //更新一个执行者的状态
    //通过一个应用ID和执行者ID,获取执行者对象ExecutorDesc,更新该执行者的状态state,exitStatus退出状态,0表示正常退出
    case ExecutorStateChanged(appId, execId, state, message, exitStatus) => {
      val execOption = idToApp.get(appId).flatMap(app => app.executors.get(execId)) //返回该执行者对象ExecutorDesc
      execOption match {
        case Some(exec) => {
          val appInfo = idToApp(appId)
          exec.state = state //更新执行者的状态
          if (state == ExecutorState.RUNNING) { appInfo.resetRetryCount() } //如果更新的参数状态是RUNNING,则要重新设置尝试次数
          exec.application.driver.send(ExecutorUpdated(execId, state, message, exitStatus)) //向执行者的driver发送更新信息
          if (ExecutorState.isFinished(state)) {//如果状态最终是完成的状态
            // Remove this executor from the worker and app
            logInfo(s"Removing executor ${exec.fullId} because it is $state")
            // If an application has already finished, preserve its
            // state to display its information properly on the UI
            if (!appInfo.isFinished) {//如果app没有完成,则移除该执行者
              appInfo.removeExecutor(exec)
            }
            exec.worker.removeExecutor(exec) //worker上移除该执行者

            val normalExit = exitStatus == Some(0)
            // Only retry certain number of times so we don't go into an infinite loop.
            if (!normalExit) {//不是正常退出
              if (appInfo.incrementRetryCount() < ApplicationState.MAX_NUM_RETRY) {//重试次数不够多,继续重试执行
                schedule()
              } else {//说明该应用执行全部失败,则杀死执行者以及移除应用
                val execs = appInfo.executors.values
                if (!execs.exists(_.state == ExecutorState.RUNNING)) {
                  logError(s"Application ${appInfo.desc.name} with ID ${appInfo.id} failed " +
                    s"${appInfo.retryCount} times; removing it")
                  removeApplication(appInfo, ApplicationState.FAILED)
                }
              }
            }
          }
        }
        case None =>
          logWarning(s"Got status update for unknown executor $appId/$execId")
      }
    }

    //更新driver
    case DriverStateChanged(driverId, state, exception) => {
      state match {
        case DriverState.ERROR | DriverState.FINISHED | DriverState.KILLED | DriverState.FAILED =>
          removeDriver(driverId, state, exception)
        case _ =>
          throw new Exception(s"Received unexpected state update for driver $driverId: $state")
      }
    }

    case Heartbeat(workerId, worker) => { //worker发来了心跳
      idToWorker.get(workerId) match { //找到该worker
        case Some(workerInfo) =>
          workerInfo.lastHeartbeat = System.currentTimeMillis() //更新最后心跳时间
        case None =>
          if (workers.map(_.id).contains(workerId)) { //如果worker不在关联master,但是以前关联过
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " Asking it to re-register.")
            worker.send(ReconnectWorker(masterUrl)) //向worker发送重新连接master命令
          } else {//说明一直该worker就没有注册到master上来,因此打印信息,说明该worker不是内部worker
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " This worker was never registered, so ignoring the heartbeat.")
          }
      }
    }

    case MasterChangeAcknowledged(appId) => {
      idToApp.get(appId) match {
        case Some(app) =>
          logInfo("Application has been re-registered: " + appId)
          app.state = ApplicationState.WAITING
        case None =>
          logWarning("Master change ack from unknown app: " + appId)
      }

      if (canCompleteRecovery) { completeRecovery() }
    }

    case WorkerSchedulerStateResponse(workerId, executors, driverIds) => {
      idToWorker.get(workerId) match {
        case Some(worker) =>
          logInfo("Worker has been re-registered: " + workerId)
          worker.state = WorkerState.ALIVE

          val validExecutors = executors.filter(exec => idToApp.get(exec.appId).isDefined)
          for (exec <- validExecutors) {
            val app = idToApp.get(exec.appId).get
            val execInfo = app.addExecutor(worker, exec.cores, Some(exec.execId))
            worker.addExecutor(execInfo)
            execInfo.copyState(exec)
          }

          for (driverId <- driverIds) {
            drivers.find(_.id == driverId).foreach { driver =>
              driver.worker = Some(worker)
              driver.state = DriverState.RUNNING
              worker.drivers(driverId) = driver
            }
          }
        case None =>
          logWarning("Scheduler state from unknown worker: " + workerId)
      }

      if (canCompleteRecovery) { completeRecovery() }
    }

    //完成一个应用
    case UnregisterApplication(applicationId) =>
      logInfo(s"Received unregister request from application $applicationId")
      idToApp.get(applicationId).foreach(finishApplication)

      //检查单位时间内没有心跳的worker,将其设置为dead,然后很久时间没有被心跳,才会被移除该worker
    case CheckForWorkerTimeOut => {
      timeOutDeadWorkers()
    }
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RequestSubmitDriver(description) => {//接受driver的提交
      if (state != RecoveryState.ALIVE) {//如果当前master状态不是alive,则不能接受driver的提交
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          "Can only accept driver submissions in ALIVE state."
        context.reply(SubmitDriverResponse(self, false, None, msg))//将错误信息发送给客户端
      } else {//代码进入到这里,说明master的状态是alive
        logInfo("Driver submitted " + description.command.mainClass)
        val driver = createDriver(description) //master受收到客户端submitDriver后,创建一个DriverInfo对象
        persistenceEngine.addDriver(driver) //在文件系统中添加一个driver,说明master收到了一个driver
        waitingDrivers += driver //内存存储该driver
        drivers.add(driver)
        
        schedule() //将driver进入调度器,进行调度该driver

        // TODO: It might be good to instead have the submission client poll the master to determine
        //       the current status of the driver. For now it's simply "fire and forget".
        //恢复客户端driver成功被提交给master了,并且恢复客户端driver的ID
        context.reply(SubmitDriverResponse(self, true, Some(driver.id),
          s"Driver successfully submitted as ${driver.id}"))
      }
    }

    case RequestKillDriver(driverId) => {//杀死一个driver的请求
      if (state != RecoveryState.ALIVE) {//如果当前master状态不是alive,则不能接受driver的提交
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          s"Can only kill drivers in ALIVE state."
        context.reply(KillDriverResponse(self, driverId, success = false, msg))
      } else {
        logInfo("Asked to kill driver " + driverId)
        val driver = drivers.find(_.id == driverId) //找到该driver对象
        driver match {
          case Some(d) =>
            if (waitingDrivers.contains(d)) {//如果该driver尚未调度,则仅发给master信息即可,通知该master,driver被kill掉了
              waitingDrivers -= d
              self.send(DriverStateChanged(driverId, DriverState.KILLED, None))
            } else {//如果driver已经被调用,则要发送给worker,让worker去kill掉该driver
              // We just notify the worker to kill the driver here. The final bookkeeping occurs
              // on the return path when the worker submits a state change back to the master
              // to notify it that the driver was successfully killed.
              d.worker.foreach { w =>
                w.endpoint.send(KillDriver(driverId))
              }
            }
            // TODO: It would be nice for this to be a synchronous response
            //发送信息给调用者,通知该kill请求,master已经处理了
            val msg = s"Kill request for $driverId submitted"
            logInfo(msg)
            context.reply(KillDriverResponse(self, driverId, success = true, msg))
          case None =>
            //打印信息说该driver没有被找到
            val msg = s"Driver $driverId has already finished or does not exist"
            logWarning(msg)
            context.reply(KillDriverResponse(self, driverId, success = false, msg))
        }
      }
    }

    case RequestDriverStatus(driverId) => {//获取该driver的状态
      if (state != RecoveryState.ALIVE) {
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          "Can only request driver status in ALIVE state."
        context.reply(
          DriverStatusResponse(found = false, None, None, None, Some(new Exception(msg))))
      } else {
        (drivers ++ completedDrivers).find(_.id == driverId) match {//查找driver
          case Some(driver) =>
            context.reply(DriverStatusResponse(found = true, Some(driver.state),
              driver.worker.map(_.id), driver.worker.map(_.hostPort), driver.exception))//恢复该driver的信息,即driver是否存在,状态,在哪个worker上运行,是否有异常产生
          case None =>
            context.reply(DriverStatusResponse(found = false, None, None, None, None))
        }
      }
    }

    case RequestMasterState => {//获取整个master的状态信息
      context.reply(MasterStateResponse(
        address.host, address.port, restServerBoundPort,
        workers.toArray, apps.toArray, completedApps.toArray,
        drivers.toArray, completedDrivers.toArray,state))
    }

    case BoundPortsRequest => {//获取master的绑定端口信息
      context.reply(BoundPortsResponse(address.port, webUi.boundPort, restServerBoundPort))
    }

    case RequestExecutors(appId, requestedTotal) =>
      context.reply(handleRequestExecutors(appId, requestedTotal)) //设置app的执行者的限制数量,即appInfo.executorLimit

    case KillExecutors(appId, executorIds) => 
      val formattedExecutorIds = formatExecutorIds(executorIds) //将执行者ID集合转换成Int集合
      context.reply(handleKillExecutors(appId, formattedExecutorIds)) //kill掉一个应用下一组执行者
  }

  override def onDisconnected(address: RpcAddress): Unit = {
    // The disconnected client could've been either a worker or an app; remove whichever it was
    logInfo(s"$address got disassociated, removing it.")
    addressToWorker.get(address).foreach(removeWorker)
    addressToApp.get(address).foreach(finishApplication)
    if (state == RecoveryState.RECOVERING && canCompleteRecovery) { completeRecovery() }
  }

  //当worker和application没有为止的时候,则返回true,表示已经完成的恢复操作
  private def canCompleteRecovery =
    workers.count(_.state == WorkerState.UNKNOWN) == 0 &&
      apps.count(_.state == ApplicationState.UNKNOWN) == 0

  //masert节点选举成功后,开始恢复数据
  //参数含义:读取所有数据,分别返回三组集合,即所有app,所有driver,所有worker
  private def beginRecovery(storedApps: Seq[ApplicationInfo], storedDrivers: Seq[DriverInfo],
      storedWorkers: Seq[WorkerInfo]) {
    for (app <- storedApps) {
      logInfo("Trying to recover app: " + app.id)
      try {
        registerApplication(app)
        app.state = ApplicationState.UNKNOWN
        //master在恢复阶段向worker和application客户端发送该信息,即通知master已经更换了一个新的
        app.driver.send(MasterChanged(self, masterWebUiUrl))
      } catch {
        case e: Exception => logInfo("App " + app.id + " had exception on reconnect")
      }
    }

    for (driver <- storedDrivers) {
      // Here we just read in the list of drivers. Any drivers associated with now-lost workers
      // will be re-launched when we detect that the worker is missing.
      drivers += driver
    }

    for (worker <- storedWorkers) {
      logInfo("Trying to recover worker: " + worker.id)
      try {
        registerWorker(worker)
        worker.state = WorkerState.UNKNOWN
        //master在恢复阶段向worker和application客户端发送该信息,即通知master已经更换了一个新的
        worker.endpoint.send(MasterChanged(self, masterWebUiUrl))
      } catch {
        case e: Exception => logInfo("Worker " + worker.id + " had exception on reconnect")
      }
    }
  }

  //master节点启动后,进行恢复工作,当恢复完成时,调用该方法
  private def completeRecovery() {
    // Ensure "only-once" recovery semantics using a short synchronization period.
    //状态一定是RECOVERING,将其状态改成COMPLETING_RECOVERY
    if (state != RecoveryState.RECOVERING) { return }
    state = RecoveryState.COMPLETING_RECOVERY

    //以下进行恢复完成后的处理逻辑
    // Kill off any workers and apps that didn't respond to us.
    //杀死掉任何不能有恢复的worker和app
    workers.filter(_.state == WorkerState.UNKNOWN).foreach(removeWorker)
    apps.filter(_.state == ApplicationState.UNKNOWN).foreach(finishApplication)

    // Reschedule drivers which were not claimed by any workers
    //重新调度driver
    //1.找到driver中worker是空的driver集合
    drivers.filter(_.worker.isEmpty).foreach { d =>
      logWarning(s"Driver ${d.id} was not found after master recovery")//打印日志说明master恢复后,该driver没有发现被工作在哪个worker上
      if (d.desc.supervise) {//该driver是否要被重启
        logWarning(s"Re-launching ${d.id}")
        relaunchDriver(d) //重新启动该driver
      } else {
        removeDriver(d.id, DriverState.ERROR, None) //删除该driver
        logWarning(s"Did not re-launch ${d.id} because it was not supervised")
      }
    }

    state = RecoveryState.ALIVE
    //重新开始调度
    schedule()
    logInfo("Recovery complete - resuming operations!")
  }

  /**
   * Schedule executors to be launched on the workers.
   * 调度执行者在workers节点集合上被启动
   * Returns an array containing number of cores assigned to each worker.
   * 返回一个数组,数组内容表示每一个worker上多少个cpu被分配了
   * 
   * There are two modes of launching executors. The first attempts to spread out an application's
   * executors on as many workers as possible, while the second does the opposite (i.e. launch them
   * on as few workers as possible). The former is usually better for data locality purposes and is
   * the default.
   * 有两个启动执行者模式,
   * 首先尝试在扩展应用的执行者在多个worker上
   * 第二个与第一个相反,启动他们在少数worker上,因为数据本地化,这个模式通常会更好一些,并且该模式是默认的模式
   * 
   * 
   * The number of cores assigned to each executor is configurable. When this is explicitly set,
   * multiple executors from the same application may be launched on the same worker if the worker
   * has enough cores and memory. Otherwise, each executor grabs all the cores available on the
   * worker by default, in which case only one executor may be launched on each worker.
   * 每一个执行者被分配多少个cpu是配置好的,当明确的被设置了,同一个应用的多个执行者可能在相同的worker上被启动,(如果该worker有足够的内存和cpu资源).
   * 否则默认情况下,每一个执行者霸占worker上所有可用的cpu资源
   * 
   * It is important to allocate coresPerExecutor on each worker at a time (instead of 1 core
   * at a time). Consider the following example: cluster has 4 workers with 16 cores each.
   * User requests 3 executors (spark.cores.max = 48, spark.executor.cores = 16). 
   * 每一个执行者在一个worker上分配多少cpu,这个是很重要的
   * 考虑到以下情况,集群有4个worker,总共有16个cpu,用户请求3个执行者
   * If 1 core is allocated at a time, 12 cores from each worker would be assigned to each executor.
   * Since 12 < 16, no executors would launch [SPARK-8881].
   * 
   * 
   * 该方法有schedule方法调度,即表示在worker集合上调度该app
   * 
   * 返回每一个worker分配多少个cpu,用数组表示
   */
  private def scheduleExecutorsOnWorkers(
      app: ApplicationInfo,//被调度的app
      usableWorkers: Array[WorkerInfo],//该app要在以下worker上被调度
      spreadOutApps: Boolean): Array[Int] = { //调度模式,选择1还是2方案
    val coresPerExecutor = app.desc.coresPerExecutor //该应用每一个执行者默认需要多少cpu
    val minCoresPerExecutor = coresPerExecutor.getOrElse(1) //默认是1个cpu
    val oneExecutorPerWorker = coresPerExecutor.isEmpty //没有设置每一个执行者默认需要多少cpu,则允许worker上执行多个执行者
    val memoryPerExecutor = app.desc.memoryPerExecutorMB //该应用每一个执行者默认需要多少内存
    val numUsable = usableWorkers.length //可以使用的worker个数
    val assignedCores = new Array[Int](numUsable) // Number of cores to give to each worker 每一个worker分配多少个cpu
    val assignedExecutors = new Array[Int](numUsable) // Number of new executors on each worker //每一个worker上分配多少个执行者
    
    //<计算app还需要多少cpu,以及worker一共提供多少个空闲的cpu>两者的最小值,该值就是这次提供应该提供多少cpu
    var coresToAssign = math.min(app.coresLeft, usableWorkers.map(_.coresFree).sum)

    /** Return whether the specified worker can launch an executor for this app.
     *  返回是否参数对应的worker能否启动该app的一个执行者 
     * 参数pos是worker的index下标 
     * true表示可以在worker上启动该执行者
     **/
    def canLaunchExecutor(pos: Int): Boolean = {
      val keepScheduling = coresToAssign >= minCoresPerExecutor //true表示要继续调度
      val enoughCores = usableWorkers(pos).coresFree - assignedCores(pos) >= minCoresPerExecutor //该worker上是否还有资源继续分配

      // If we allow multiple executors per worker, then we can always launch new executors.
      //我们允许一个worker上多个执行者,那样我们总是开启一个新的执行者
      // Otherwise, if there is already an executor on this worker, just give it more cores.
      //否则如果worker上已经存在一个执行者了,仅仅给他更多的cpu
      
      //true表示需要重新开启一个执行者
      //没有设置每一个执行者默认需要多少cpu,则允许worker上执行多个执行者,则设置为true
      //如果该worker上从来没有分配过该执行者,因此也设置为true
      val launchingNewExecutor = !oneExecutorPerWorker || assignedExecutors(pos) == 0
      if (launchingNewExecutor) {
        val assignedMemory = assignedExecutors(pos) * memoryPerExecutor //计算所需要内存数量
        val enoughMemory = usableWorkers(pos).memoryFree - assignedMemory >= memoryPerExecutor //true表示内存充足
        val underLimit = assignedExecutors.sum + app.executors.size < app.executorLimit
        keepScheduling && enoughCores && enoughMemory && underLimit //必须可以调度 && cpu足够 && 内存足够 && 没有达到分配上限 即可
      } else {
        // We're adding cores to an existing executor, so no need
        // to check memory and executor limits 我们仅仅添加多余的cpu在现存的执行者中,因此不需要你校验内存和执行者限制
        keepScheduling && enoughCores //只要可以调度 && cpu足够即可
      }
    }

    // Keep launching executors until no more workers can accommodate any
    // more executors, or if we have reached this application's limits
    //过滤可以启动该app的执行者的worker集合
    var freeWorkers = (0 until numUsable).filter(canLaunchExecutor)
    while (freeWorkers.nonEmpty) {
      freeWorkers.foreach { pos =>
        var keepScheduling = true
        while (keepScheduling && canLaunchExecutor(pos)) {//真正去设计调度资源申请
          coresToAssign -= minCoresPerExecutor
          assignedCores(pos) += minCoresPerExecutor

          // If we are launching one executor per worker, then every iteration assigns 1 core
          // to the executor. Otherwise, every iteration assigns cores to a new executor.
          if (oneExecutorPerWorker) {
            assignedExecutors(pos) = 1
          } else {
            assignedExecutors(pos) += 1
          }

          // Spreading out an application means spreading out its executors across as
          // many workers as possible. If we are not spreading out, then we should keep
          // scheduling executors on this worker until we use all of its resources.
          // Otherwise, just move on to the next worker.
          if (spreadOutApps) {
            keepScheduling = false
          }
        }
      }
      freeWorkers = freeWorkers.filter(canLaunchExecutor)
    }
    assignedCores //每一个worker分配多少个cpu
  }

  /**
   * Schedule and launch executors on workers
   * Schedule方法调用该函数,在worker上去启动一些执行者
   */
  private def startExecutorsOnWorkers(): Unit = {
    // Right now this is a very simple FIFO scheduler. We keep trying to fit in the first app
    // in the queue, then the second app, etc.
    //这个是一个很简单的FIFO队列,一个app,一个app的进行调度执行者去worker节点执行
    for (app <- waitingApps if app.coresLeft > 0) {//如果app还有资源没有被调用
      val coresPerExecutor: Option[Int] = app.desc.coresPerExecutor //该应用每一个执行者需要多少cpu
      // Filter out workers that don't have enough resources to launch an executor
      //过滤掉没有足够资源开启应用的workers
      /**
       * 1.找到ALIVE状态的worker
       * 2.找到worker上可以满足应用的内存和cpu的worker集合,按照空闲的cpu数量排序
       * 
       * 返回值是该app可以使用的worker集合
       */
      val usableWorkers = workers.toArray.filter(_.state == WorkerState.ALIVE)
        .filter(worker => worker.memoryFree >= app.desc.memoryPerExecutorMB &&
          worker.coresFree >= coresPerExecutor.getOrElse(1))
        .sortBy(_.coresFree).reverse
        
      //在该可以的worker集合上执行该app,返回数组表示每一个worker上分配多少个cpu
      val assignedCores = scheduleExecutorsOnWorkers(app, usableWorkers, spreadOutApps)

      // Now that we've decided how many cores to allocate on each worker, let's allocate them
      //我们现在要去每一个worker上真正分配多少个cpu去执行app
      for (pos <- 0 until usableWorkers.length if assignedCores(pos) > 0) {//循环每一个要去执行的worker,如果该worker上分配了cpu,则进入if
        allocateWorkerResourceToExecutors(
          app, assignedCores(pos), coresPerExecutor, usableWorkers(pos))
      }
    }
  }

  /**
   * Allocate a worker's resources to one or more executors. 为一个或者更多个执行者分配worker的资源
   * @param app the info of the application which the executors belong to 执行者所属的app
   * @param assignedCores number of cores on this worker for this application 该应用在该worker上被分配了多少个cpu
   * @param coresPerExecutor number of cores per executor 每一个执行者需要多少个cpu
   * @param worker the worker info 去哪个worker执行
   * 调度器调用该函数
   * 表示将app分配到worker上去执行,一共分配了多少个cpu,以及每一个执行者需要几个cpu
   */
  private def allocateWorkerResourceToExecutors(
      app: ApplicationInfo,
      assignedCores: Int,
      coresPerExecutor: Option[Int],
      worker: WorkerInfo): Unit = {
    // If the number of cores per executor is specified, we divide the cores assigned
    // to this worker evenly among the executors with no remainder.如果每一个执行者需要多少个cpu被指定了,则我们可以计算需要多少个执行者被执行
    // Otherwise, we launch a single executor that grabs all the assignedCores on this worker.否则启动一个执行者,将被分配的cpu都消耗在一个执行者上
    val numExecutors = coresPerExecutor.map { assignedCores / _ }.getOrElse(1) //计算该worker上有多少个执行者要被分配执行,如果没有分配coresPerExecutor,则返回1,即1个执行者即可
    val coresToAssign = coresPerExecutor.getOrElse(assignedCores) //如果没有分配coresPerExecutor,则返回assignedCores,即分配了多少个cpu,都会给该执行者执行
    for (i <- 1 to numExecutors) {
      val exec = app.addExecutor(worker, coresToAssign) //将该应用分配到该worker上,分配多少个cpu,记录到内存映射中,返回被创建的执行者
      launchExecutor(worker, exec)
      app.state = ApplicationState.RUNNING
    }
  }

  /**
   * Schedule the currently available resources among waiting apps. This method will be called
   * every time a new app joins or resource availability changes.
   */
  private def schedule(): Unit = {
    if (state != RecoveryState.ALIVE) { return }
    // Drivers take strict precedence over executors
    val shuffledWorkers = Random.shuffle(workers) // Randomization helps balance drivers 打乱workder的排序
    for (worker <- shuffledWorkers if worker.state == WorkerState.ALIVE) {//从乱序的worker中选择第一个alive状态的workder
      for (driver <- waitingDrivers) {//循环等候的driver
        if (worker.memoryFree >= driver.desc.mem && worker.coresFree >= driver.desc.cores) {//如果worker的内存和cpu满足任意一个driver
          launchDriver(worker, driver)//则在worker上启动该driver
          waitingDrivers -= driver //等待的driver减少一个
        }
      }
    }
    startExecutorsOnWorkers()
  }

  //在worker上启动该执行者
  private def launchExecutor(worker: WorkerInfo, exec: ExecutorDesc): Unit = {
    logInfo("Launching executor " + exec.fullId + " on worker " + worker.id) //打印日志在worker上执行该执行者
    worker.addExecutor(exec) //在给worker上添加该执行者内存映射
    //向worker发送启动执行者信息
    worker.endpoint.send(LaunchExecutor(masterUrl,
      exec.application.id, exec.id, exec.application.desc, exec.cores, exec.memory))
    //向应用的driver发送执行者被执行了
    exec.application.driver.send(ExecutorAdded(
      exec.id, worker.id, worker.hostPort, exec.cores, exec.memory))
  }

  //注册一个worker
  private def registerWorker(worker: WorkerInfo): Boolean = {
    // There may be one or more refs to dead workers on this same node (w/ different ID's),
    // remove them.
    //可能存在一个或者多个已经死亡的workder,该worker与注册的worker在同样的节点上,只是ID不同,则我们要移除他们
    workers.filter { w =>
      (w.host == worker.host && w.port == worker.port) && (w.state == WorkerState.DEAD) //找到可以移除的worker集合
    }.foreach { w =>
      workers -= w //移除他们
    }

    
    //判断该worker地址是否有worker,并且非未知的,如果存在,则添加失败,返回false
    val workerAddress = worker.endpoint.address
    if (addressToWorker.contains(workerAddress)) {
      val oldWorker = addressToWorker(workerAddress)
      if (oldWorker.state == WorkerState.UNKNOWN) {
        // A worker registering from UNKNOWN implies that the worker was restarted during recovery.
        // The old worker must thus be dead, so we will remove it and accept the new worker.
        removeWorker(oldWorker)
      } else {
        logInfo("Attempted to re-register worker at same address: " + workerAddress)
        return false
      }
    }

    //内存中存储对应关系
    workers += worker
    idToWorker(worker.id) = worker
    addressToWorker(workerAddress) = worker
    true
  }

  //移除该worker
  private def removeWorker(worker: WorkerInfo) {
    logInfo("Removing worker " + worker.id + " on " + worker.host + ":" + worker.port)
    worker.setState(WorkerState.DEAD) //设置该workder状态为死亡,仅仅设置其死亡,不从内存中删除
    
    //清理workder的内存关系
    idToWorker -= worker.id
    addressToWorker -= worker.endpoint.address
    
    //处理该worker上所有的executors
    for (exec <- worker.executors.values) {
      logInfo("Telling app of lost executor: " + exec.id)
      //对应执行者所在的应用对应的driver,发送信息,标示该driver的应用下某一个执行者状态更新,变成丢失,因为worker被抛弃了,在该worker上执行的进程都要被丢弃发送给driver,通知driver的某个应用下某一个执行者状态更新,变成丢失,因为worker被抛弃了,在该worker上执行的进程都要被丢弃
      exec.application.driver.send(ExecutorUpdated(exec.id, ExecutorState.LOST, Some("worker lost"), None))
      //移除该执行者
      exec.application.removeExecutor(exec)
    }
    
    //处理该worker上所有的driver
    for (driver <- worker.drivers.values) {
      if (driver.desc.supervise) {//重新启动开driver
        logInfo(s"Re-launching ${driver.id}")
        relaunchDriver(driver)
      } else {//移除该driver
        logInfo(s"Not re-launching ${driver.id} because it was not supervised")
        removeDriver(driver.id, DriverState.ERROR, None)
      }
    }
    
    //移除该workder
    persistenceEngine.removeWorker(worker)
  }

  //启动一个driver
  private def relaunchDriver(driver: DriverInfo) {
    driver.worker = None
    driver.state = DriverState.RELAUNCHING
    waitingDrivers += driver
    schedule()
  }

  /**
   * 常见一个应用
   * @driver 是如何与该application对应的driver通信的通道
   */
  private def createApplication(desc: ApplicationDescription, driver: RpcEndpointRef):
      ApplicationInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    new ApplicationInfo(now, newApplicationId(date), desc, date, driver, defaultCores)
  }

  //注册一个应用
  private def registerApplication(app: ApplicationInfo): Unit = {
    val appAddress = app.driver.address //获取app对应的driver的地址
    if (addressToApp.contains(appAddress)) { //一个地址仅需要注册一个app即可
      logInfo("Attempted to re-register application at same address: " + appAddress)
      return
    }

    applicationMetricsSystem.registerSource(app.appSource)
    apps += app
    idToApp(app.id) = app
    endpointToApp(app.driver) = app
    addressToApp(appAddress) = app
    waitingApps += app
  }

  //移除一个app
  private def finishApplication(app: ApplicationInfo) {
    removeApplication(app, ApplicationState.FINISHED)
  }

  //移除一个app,参数提供了最后移除时候app的状态
  def removeApplication(app: ApplicationInfo, state: ApplicationState.Value) {
    if (apps.contains(app)) {
      logInfo("Removing app " + app.id)
      apps -= app
      idToApp -= app.id
      endpointToApp -= app.driver
      addressToApp -= app.driver.address
      if (completedApps.size >= RETAINED_APPLICATIONS) {//当application完成的时候,要保持多少个已经完成的application
        val toRemove = math.max(RETAINED_APPLICATIONS / 10, 1) //每次移除10%的完成应用数据
        completedApps.take(toRemove).foreach( a => {
          appIdToUI.remove(a.id).foreach { ui => webUi.detachSparkUI(ui) } //移除webUrl
          applicationMetricsSystem.removeSource(a.appSource)
        })
        completedApps.trimStart(toRemove)
      }
      completedApps += app // Remember it in our history
      waitingApps -= app

      // If application events are logged, use them to rebuild the UI
      rebuildSparkUI(app)

      //kill掉该app对应的所有执行者
      for (exec <- app.executors.values) {
        killExecutor(exec)
      }
      
      app.markFinished(state)
      
      //向driver发送该app已经被删除的事件
      if (state != ApplicationState.FINISHED) {
        app.driver.send(ApplicationRemoved(state.toString))
      }
      
      persistenceEngine.removeApplication(app) //文件系统移除该app
      schedule() //重新调度,因为app已经被移除了,因此有资源了可以调用其他app了

      // Tell all workers that the application has finished, so they can clean up any app state.
      //通知所有的worker,该app已经完成,不需要在执行该app相关的工作了
      workers.foreach { w =>
        w.endpoint.send(ApplicationFinished(app.id))
      }
    }
  }

  /**
   * Handle a request to set the target number of executors for this application.
   * 去设置一个应用的执行者数量限制
   * 
   * If the executor limit is adjusted upwards, new executors will be launched provided
   * that there are workers with sufficient resources. If it is adjusted downwards, however,
   * we do not kill existing executors until we explicitly receive a kill request.
   *
   * @return whether the application has previously registered with this Master.
   * 
   * @requestedTotal 设置app的执行者的限制数量
   */
  private def handleRequestExecutors(appId: String, requestedTotal: Int): Boolean = {
    idToApp.get(appId) match {//获取该app对象
      case Some(appInfo) =>
        logInfo(s"Application $appId requested to set total executors to $requestedTotal.")
        appInfo.executorLimit = requestedTotal //设置app的执行者的限制
        schedule()
        true
      case None =>
        logWarning(s"Unknown application $appId requested $requestedTotal total executors.")
        false
    }
  }

  /**
   * Handle a kill request from the given application.
   *
   * This method assumes the executor limit has already been adjusted downwards through
   * a separate [[RequestExecutors]] message, such that we do not launch new executors
   * immediately after the old ones are removed.
   *
   * @return whether the application has previously registered with this Master.
   * 
   * kill掉一个应用下一组执行者
   */
  private def handleKillExecutors(appId: String, executorIds: Seq[Int]): Boolean = {
    idToApp.get(appId) match {
      case Some(appInfo) =>
        logInfo(s"Application $appId requests to kill executors: " + executorIds.mkString(", ")) //打印日志,要kill掉哪些执行者
        val (known, unknown) = executorIds.partition(appInfo.executors.contains) //分组,按照是否包含该执行者进行分组
        known.foreach { executorId => //对已知的执行者ID进行处理
          val desc = appInfo.executors(executorId) //获取该执行者对象
          appInfo.removeExecutor(desc) //移除该application上的一个执行者内存映射
          killExecutor(desc) //移除该worker上的一个执行者
        }
        if (unknown.nonEmpty) { //对没找到的执行者进行打印日志
          logWarning(s"Application $appId attempted to kill non-existent executors: "
            + unknown.mkString(", "))
        }
        schedule()
        true
      case None =>
        logWarning(s"Unregistered application $appId requested us to kill executors!")
        false
    }
  }

  /**
   * Cast the given executor IDs to integers and filter out the ones that fail.
   *
   * All executors IDs should be integers since we launched these executors. However,
   * the kill interface on the driver side accepts arbitrary strings, so we need to
   * handle non-integer executor IDs just to be safe.
   * 将执行者ID集合转换成Int集合
   */
  private def formatExecutorIds(executorIds: Seq[String]): Seq[Int] = {
    executorIds.flatMap { executorId =>
      try {
        Some(executorId.toInt)
      } catch {
        case e: NumberFormatException =>
          logError(s"Encountered executor with a non-integer ID: $executorId. Ignoring")
          None
      }
    }
  }

  /**
   * Ask the worker on which the specified executor is launched to kill the executor.
   * 移除该worker上的一个执行者
   */
  private def killExecutor(exec: ExecutorDesc): Unit = {
    exec.worker.removeExecutor(exec)//删除该执行者的内存映射
    //向worker发送杀死该kill的命令
    exec.worker.endpoint.send(KillExecutor(masterUrl, exec.application.id, exec.id))
    //更改状态
    exec.state = ExecutorState.KILLED
  }

  /**
   * Rebuild a new SparkUI from the given application's event logs.
   * Return the UI if successful, else None
   * 从应用的日志中还原SparkUI对象
   */
  private[master] def rebuildSparkUI(app: ApplicationInfo): Option[SparkUI] = {
    val appName = app.desc.name
    val notFoundBasePath = HistoryServer.UI_PATH_PREFIX + "/not-found" //默认路径,当没有发现该应用的事件目录时候,使用它
    try {
      val eventLogDir = app.desc.eventLogDir
        .getOrElse {
          // Event logging is not enabled for this application
          app.desc.appUiUrl = notFoundBasePath
          return None
        }

      val eventLogFilePrefix = EventLoggingListener.getLogPath(
          eventLogDir, app.id, app.desc.eventLogCodec)
      val fs = Utils.getHadoopFileSystem(eventLogDir, hadoopConf)
      val inProgressExists = fs.exists(new Path(eventLogFilePrefix +
          EventLoggingListener.IN_PROGRESS))

      if (inProgressExists) {
        // Event logging is enabled for this application, but the application is still in progress
        logWarning(s"Application $appName is still in progress, it may be terminated abnormally.")
      }

      val (eventLogFile, status) = if (inProgressExists) {
        (eventLogFilePrefix + EventLoggingListener.IN_PROGRESS, " (in progress)")
      } else {
        (eventLogFilePrefix, " (completed)")
      }

      val logInput = EventLoggingListener.openEventLog(new Path(eventLogFile), fs)
      val replayBus = new ReplayListenerBus()
      val ui = SparkUI.createHistoryUI(new SparkConf, replayBus, new SecurityManager(conf),
        appName + status, HistoryServer.UI_PATH_PREFIX + s"/${app.id}", app.startTime)
      val maybeTruncated = eventLogFile.endsWith(EventLoggingListener.IN_PROGRESS)
      try {
        replayBus.replay(logInput, eventLogFile, maybeTruncated)
      } finally {
        logInput.close()
      }
      appIdToUI(app.id) = ui
      webUi.attachSparkUI(ui)
      // Application UI is successfully rebuilt, so link the Master UI to it
      app.desc.appUiUrl = ui.basePath
      Some(ui)
    } catch {
      case fnf: FileNotFoundException =>
        // Event logging is enabled for this application, but no event logs are found
        val title = s"Application history not found (${app.id})"
        var msg = s"No event logs found for application $appName in ${app.desc.eventLogDir.get}."
        logWarning(msg)
        msg += " Did you specify the correct logging directory?"
        msg = URLEncoder.encode(msg, "UTF-8")
        app.desc.appUiUrl = notFoundBasePath + s"?msg=$msg&title=$title"
        None
      case e: Exception =>
        // Relay exception message to application UI page
        val title = s"Application history load error (${app.id})"
        val exception = URLEncoder.encode(Utils.exceptionString(e), "UTF-8")
        var msg = s"Exception in replaying log for application $appName!"
        logError(msg, e)
        msg = URLEncoder.encode(msg, "UTF-8")
        app.desc.appUiUrl = notFoundBasePath + s"?msg=$msg&exception=$exception&title=$title"
        None
    }
  }

  /** Generate a new app ID given a app's submission date
   * 通过提交时间,创建一个applicationId
   * 格式app-yyyyMMddHHmmss-0001
   **/
  private def newApplicationId(submitDate: Date): String = {
    val appId = "app-%s-%04d".format(createDateFormat.format(submitDate), nextAppNumber)
    nextAppNumber += 1
    appId
  }

  /** Check for, and remove, any timed-out workers
   *  检查单位时间内没有心跳的worker,将其设置为dead,然后很久时间没有被心跳,才会被移除该worker
   **/
  private def timeOutDeadWorkers() {
    // Copy the workers into an array so we don't modify the hashset while iterating through it
    val currentTime = System.currentTimeMillis()
    //获取超过单位时间内没有心跳的worker,说明该worker死了
    val toRemove = workers.filter(_.lastHeartbeat < currentTime - WORKER_TIMEOUT_MS).toArray
    for (worker <- toRemove) {
      if (worker.state != WorkerState.DEAD) {
        logWarning("Removing %s because we got no heartbeat in %d seconds".format(
          worker.id, WORKER_TIMEOUT_MS / 1000))
        removeWorker(worker) //去使该worker变成dead状态
      } else {//如果该worker已经死了
        if (worker.lastHeartbeat < currentTime - ((REAPER_ITERATIONS + 1) * WORKER_TIMEOUT_MS)) {
          workers -= worker // we've seen this DEAD worker in the UI, etc. for long enough; cull it
        }
      }
    }
  }

  //根据driver的提交时间,为新的driver生成唯一标示
  //格式:driver-yyyyMMddHHmmss-0001
  private def newDriverId(submitDate: Date): String = {
    val appId = "driver-%s-%04d".format(createDateFormat.format(submitDate), nextDriverNumber)
    nextDriverNumber += 1
    appId
  }

  //master受收到客户端submitDriver后,创建一个DriverInfo对象
  private def createDriver(desc: DriverDescription): DriverInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    new DriverInfo(now, newDriverId(date), desc, date)
  }

  /**
   * 在schedule方法里面,当一个worker的内存和cpu满足一个driver,则在该worker上启动该driver
   */
  private def launchDriver(worker: WorkerInfo, driver: DriverInfo) {
    logInfo("Launching driver " + driver.id + " on worker " + worker.id)
    worker.addDriver(driver) //在worker上映射该driver信息,以及减少该worker的资源情况
    driver.worker = Some(worker)
    worker.endpoint.send(LaunchDriver(driver.id, driver.desc)) //通知worker终端要开启一个driver
    driver.state = DriverState.RUNNING
  }

  /**
   * 移除一个driver
   */
  private def removeDriver(
      driverId: String,
      finalState: DriverState,//driver的最终状态
      exception: Option[Exception]) {//driver是否有异常,以及异常内容
    drivers.find(d => d.id == driverId) match {//先找到该driver
      case Some(driver) =>
        logInfo(s"Removing driver: $driverId")
        drivers -= driver //内存移除该driver映射
        if (completedDrivers.size >= RETAINED_DRIVERS) {//当完成的driver超过该值时,移除一部分driver
          val toRemove = math.max(RETAINED_DRIVERS / 10, 1)//每次删除10%个driver
          completedDrivers.trimStart(toRemove)
        }
        completedDrivers += driver
        persistenceEngine.removeDriver(driver) //文件系统中移除该driver
        //设置driver自己的属性信息
        driver.state = finalState
        driver.exception = exception
        //通知driver所在worder,使worker上删除该driver
        driver.worker.foreach(w => w.removeDriver(driver))
        //重新调度,因为driver的移除,已经有新的资源了
        schedule()
      case None =>
        logWarning(s"Asked to remove unknown driver: $driverId")
    }
  }
}

private[deploy] object Master extends Logging {
  val SYSTEM_NAME = "sparkMaster"
  val ENDPOINT_NAME = "Master"

  def main(argStrings: Array[String]) {
    //当指定信号发生的时候,会向log中打印日志
    SignalLogger.register(log)
    val conf = new SparkConf
    //加载配置文件,以及创建master的host port等信息
    val args = new MasterArguments(argStrings, conf)
    //返回RPC环境变量
    val (rpcEnv, _, _) = startRpcEnvAndEndpoint(args.host, args.port, args.webUiPort, conf)
    rpcEnv.awaitTermination()
  }

  /**
   * Start the Master and return a three tuple of:
   *   (1) The Master RpcEnv 返回RPC环境变量
   *   (2) The web UI bound port 
   *   (3) The REST server bound port, if any
   *   
   *   参数是开启master的host、port、以及web port、配置文件
   *   
   *   返回RPC环境变量
   */
  def startRpcEnvAndEndpoint(
      host: String,
      port: Int,
      webUiPort: Int,
      conf: SparkConf): (RpcEnv, Int, Option[Int]) = {
    val securityMgr = new SecurityManager(conf)
    val rpcEnv = RpcEnv.create(SYSTEM_NAME, host, port, conf, securityMgr)
    val masterEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME,
      new Master(rpcEnv, rpcEnv.address, webUiPort, securityMgr, conf))
    val portsResponse = masterEndpoint.askWithRetry[BoundPortsResponse](BoundPortsRequest)
    (rpcEnv, portsResponse.webUIPort, portsResponse.restPort)
  }
}
