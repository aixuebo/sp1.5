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

package org.apache.spark.scheduler

import java.nio.ByteBuffer
import java.util.{TimerTask, Timer}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.language.postfixOps
import scala.util.Random

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.scheduler.TaskLocality.TaskLocality
import org.apache.spark.util.{ThreadUtils, Utils}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.storage.BlockManagerId

/**
 * Schedules tasks for multiple types of clusters by acting through a SchedulerBackend.
 * It can also work with a local setup by using a LocalBackend and setting isLocal to true.
 * It handles common logic, like determining a scheduling order across jobs, waking up to launch
 * speculative tasks, etc.
 *
 * Clients should first call initialize() and start(), then submit task sets through the
 * runTasks method.
 *
 * THREADING: SchedulerBackends and task-submitting clients can call this class from multiple
 * threads, so it needs locks in public API methods to maintain its state. In addition, some
 * SchedulerBackends synchronize on themselves when they want to send events here, and then
 * acquire a lock on us, so we need to make sure that we don't try to lock the backend while
 * we are holding a lock on ourselves.
 * 每一个应用Applitcation对应一个该对象,该对象存储了该应用在不同阶段的任务调度情况
 */
private[spark] class TaskSchedulerImpl(
    val sc: SparkContext,
    val maxTaskFailures: Int,//最大失败次数,失败次数达到该值,则停止对应的阶段---即每一个阶段都面对一个最大尝试次数,该值是由这个参数决定的
    isLocal: Boolean = false)//是否本地执行的该driver
  extends TaskScheduler with Logging
{
  def this(sc: SparkContext) = this(sc, sc.conf.getInt("spark.task.maxFailures", 4)) //最大失败次数

  val conf = sc.conf

  // How often to check for speculative tasks
  val SPECULATION_INTERVAL_MS = conf.getTimeAsMs("spark.speculation.interval", "100ms")//每隔多久执行一次推测执行检查

  private val speculationScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("task-scheduler-speculation")//线程服务---用于定期执行测试执行任务的检查

  // Threshold above which we warn user initial TaskSet may be starved 多久开启一次task执行
  val STARVATION_TIMEOUT_MS = conf.getTimeAsMs("spark.starvation.timeout", "15s")

  // CPUs to request per task 每一个任务要花费多少个cpu
  val CPUS_PER_TASK = conf.getInt("spark.task.cpus", 1)

  // TaskSetManagers are not thread safe, so any access to one should be synchronized
  // on this class.
  //key是阶段ID,即StageId value的key是该阶段的Attempt尝试次数,value是该尝试次数对应的任务集合
  private val taskSetsByStageIdAndAttempt = new HashMap[Int, HashMap[Int, TaskSetManager]]

  private[scheduler] val taskIdToTaskSetManager = new HashMap[Long, TaskSetManager]
  val taskIdToExecutorId = new HashMap[Long, String]

  @volatile private var hasReceivedTask = false //true表示已经接收了任务
  @volatile private var hasLaunchedTask = false //true表示已经启动了任务
  private val starvationTimer = new Timer(true) //就一开始的时候使用过该线程,主要用于确定该调度器是否能成功调度任务

  // Incrementing task IDs 自增长的taskId
  val nextTaskId = new AtomicLong(0)

  // Which executor IDs we have executors on 已经执行的所有executorId集合
  val activeExecutorIds = new HashSet[String]
  // The set of executors we have on each host; this is used to compute hostsAlive, which
  // in turn is used to decide when we can attain data locality on a given host
  //key是host,value是该host上运行的哪些executorId集合
  protected val executorsByHost = new HashMap[String, HashSet[String]]

  protected val hostsByRack = new HashMap[String, HashSet[String]] //该rack上有哪些host
//每一个executorId对应在哪台节点上运行,key是executorId,value是host节点
  protected val executorIdToHost = new HashMap[String, String]

  // Listener object to pass upcalls into
  var dagScheduler: DAGScheduler = null

  var backend: SchedulerBackend = null

  val mapOutputTracker = SparkEnv.get.mapOutputTracker
  
  var schedulableBuilder: SchedulableBuilder = null //调度器池子
  var rootPool: Pool = null //rootPool调度池
  
  // default scheduler is FIFO 调度模式
  private val schedulingModeConf = conf.get("spark.scheduler.mode", "FIFO")
  val schedulingMode: SchedulingMode = try {
    SchedulingMode.withName(schedulingModeConf.toUpperCase)
  } catch {
    case e: java.util.NoSuchElementException =>
      throw new SparkException(s"Unrecognized spark.scheduler.mode: $schedulingModeConf")
  }

  // This is a var so that we can reset it for testing purposes.
  private[spark] var taskResultGetter = new TaskResultGetter(sc.env, this)

  override def setDAGScheduler(dagScheduler: DAGScheduler) {
    this.dagScheduler = dagScheduler
  }

  //初始化的时候产生调度队列
  def initialize(backend: SchedulerBackend) {
    this.backend = backend
    // temporarily set rootPool name to empty
    rootPool = new Pool("", schedulingMode, 0, 0)
    schedulableBuilder = {
      schedulingMode match {
        case SchedulingMode.FIFO =>
          new FIFOSchedulableBuilder(rootPool)
        case SchedulingMode.FAIR =>
          new FairSchedulableBuilder(rootPool, conf)
      }
    }
    schedulableBuilder.buildPools()
  }

  def newTaskId(): Long = nextTaskId.getAndIncrement()

  override def start() {
    backend.start()

    if (!isLocal && conf.getBoolean("spark.speculation", false)) {//不是本地的任务,并且支持推测执行
      logInfo("Starting speculative execution thread")
      speculationScheduler.scheduleAtFixedRate(new Runnable {//每隔多久执行一次推测执行检查
        override def run(): Unit = Utils.tryOrStopSparkContext(sc) {
          checkSpeculatableTasks() //定时查找有推测执行的任务
        }
      }, SPECULATION_INTERVAL_MS, SPECULATION_INTERVAL_MS, TimeUnit.MILLISECONDS)
    }
  }

  override def postStartHook() {
    waitBackendReady()
  }

  //某个尝试阶段提交了一个任务集合
  override def submitTasks(taskSet: TaskSet) {
    val tasks = taskSet.tasks //真正的任务集合
    //打印日志,说明哪个尝试阶段提交了多少个任务
    logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks") //此时driver节点会看到日志,表示哪个任务集合 有多少个任务被提交到队列了,但是此时尚未执行
    this.synchronized {
      val manager = createTaskSetManager(taskSet, maxTaskFailures) //创建一个任务队列的叶子节点
      
      //设置该尝试的阶段与对应的任务集合映射
      val stage = taskSet.stageId
      val stageTaskSets =
        taskSetsByStageIdAndAttempt.getOrElseUpdate(stage, new HashMap[Int, TaskSetManager])
      stageTaskSets(taskSet.stageAttemptId) = manager //添加该尝试节点对应的任务管理器
      
      //true表示任务集合有冲突
      val conflictingTaskSet = stageTaskSets.exists { case (_, ts) =>
        ts.taskSet != taskSet && !ts.isZombie //说明尝试任务是不同的,但是isZombie=false表示不允许出现尝试任务一起执行
      }
      if (conflictingTaskSet) {
        throw new IllegalStateException(s"more than one active taskSet for stage $stage:" +
          s" ${stageTaskSets.toSeq.map{_._2.taskSet.id}.mkString(",")}")
      }
      
      //调度任务添加该taskSet任务集合
      schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)

      //定时任务,汇报没有资源,资源不满足,直到资源满足为止
      if (!isLocal && !hasReceivedTask) {
        starvationTimer.scheduleAtFixedRate(new TimerTask() {
          override def run() {
            if (!hasLaunchedTask) { //说明一直没有启动过任务
              logWarning("Initial job has not accepted any resources; " +
                "check your cluster UI to ensure that workers are registered " +
                "and have sufficient resources")
            } else {
              this.cancel() //一旦启动了,就不会出现这个字符串了,就可以关闭该任务了
            }
          }
        }, STARVATION_TIMEOUT_MS, STARVATION_TIMEOUT_MS)
      }
      hasReceivedTask = true
    }
    backend.reviveOffers()
  }

  // Label as private[scheduler] to allow tests to swap in different task set managers if necessary
  private[scheduler] def createTaskSetManager(
      taskSet: TaskSet,//一个阶段的任务
      maxTaskFailures: Int): TaskSetManager = {//该阶段的失败次数
    new TaskSetManager(this, taskSet, maxTaskFailures)
  }

  //取消一个阶段的所有尝试阶段的所有任务
  override def cancelTasks(stageId: Int, interruptThread: Boolean): Unit = synchronized {
    logInfo("Cancelling stage " + stageId)
    //获取一个阶段的所有尝试阶段,然后循环每一个TaskSetManager,进行取消任务操作
    taskSetsByStageIdAndAttempt.get(stageId).foreach { attempts =>
      attempts.foreach { case (_, tsm) =>
        // There are two possible cases here:
        // 1. The task set manager has been created and some tasks have been scheduled.
        //    In this case, send a kill signal to the executors to kill the task and then abort
        //    the stage.
        //如果该任务集合已经被创建,并且一些任务已经被调度了,则发送kill信号给executor,然后再使该尝试阶段流产
        // 2. The task set manager has been created but no tasks has been scheduled. In this case,
        //    simply abort the stage.
        //如果该任务集合已经被创建,但是还没有任务被调度.则仅仅使该尝试阶段流产即可
        tsm.runningTasksSet.foreach { tid =>
          val execId = taskIdToExecutorId(tid)
          backend.killTask(tid, execId, interruptThread)
        }
        tsm.abort("Stage %s cancelled".format(stageId))
        logInfo("Stage %d was cancelled".format(stageId))
      }
    }
  }

  /**
   * Called to indicate that all task attempts (including speculated tasks) associated with the
   * given TaskSetManager have completed, so state associated with the TaskSetManager should be
   * cleaned up.
   * 说明一个阶段的任务都完成了
   */
  def taskSetFinished(manager: TaskSetManager): Unit = synchronized {
    taskSetsByStageIdAndAttempt.get(manager.taskSet.stageId).foreach { taskSetsForStage =>
      taskSetsForStage -= manager.taskSet.stageAttemptId
      if (taskSetsForStage.isEmpty) {
        taskSetsByStageIdAndAttempt -= manager.taskSet.stageId
      }
    }
    manager.parent.removeSchedulable(manager)
    logInfo("Removed TaskSet %s, whose tasks have all completed, from pool %s"
      .format(manager.taskSet.id, manager.parent.name))
  }

  /**
   * 真正的为executor分配任务
   */
  private def resourceOfferSingleTaskSet(
      taskSet: TaskSetManager,//要为这些task分配任务,去executor上执行
      maxLocality: TaskLocality,
      shuffledOffers: Seq[WorkerOffer],//可以为哪些executor分配任务
      availableCpus: Array[Int],//每一个shuffledOffers对应的executor空闲多少个cpu可以执行任务
      tasks: Seq[ArrayBuffer[TaskDescription]]) : Boolean = { //用于存储生成的task任务,去executor上要执行的任务
    var launchedTask = false
    for (i <- 0 until shuffledOffers.size) {//循环每一个节点--每一个executor分配一个任务
      val execId = shuffledOffers(i).executorId
      val host = shuffledOffers(i).host
      if (availableCpus(i) >= CPUS_PER_TASK) {//说明该节点上cpu可以够一个任务的
        try {
          for (task <- taskSet.resourceOffer(execId, host, maxLocality)) { //一次只分配一个任务
            tasks(i) += task
            val tid = task.taskId
            taskIdToTaskSetManager(tid) = taskSet
            taskIdToExecutorId(tid) = execId
            executorsByHost(host) += execId
            availableCpus(i) -= CPUS_PER_TASK //减少cpu
            assert(availableCpus(i) >= 0) //必须不能是负数
            launchedTask = true
          }
        } catch {
          case e: TaskNotSerializableException =>
            logError(s"Resource offer failed, task set ${taskSet.name} was not serializable")
            // Do not offer resources for this task, but don't throw an error to allow other
            // task sets to be submitted.
            return launchedTask
        }
      }
    }
    return launchedTask
  }

  /**
   * Called by cluster manager to offer resources on slaves. We respond by asking our active task
   * sets for tasks in order of priority. We fill each node with tasks in a round-robin manner so
   * that tasks are balanced across the cluster.
   * 为参数这些executor分配资源,即参数这些executor已经空闲了
   */
  def resourceOffers(offers: Seq[WorkerOffer]): Seq[Seq[TaskDescription]] = synchronized {
    // Mark each slave as alive and remember its hostname
    // Also track if new executor is added
    var newExecAvail = false
    for (o <- offers) {
      executorIdToHost(o.executorId) = o.host
      activeExecutorIds += o.executorId
      if (!executorsByHost.contains(o.host)) {//说明是新的host,以前未知的host出现了
        executorsByHost(o.host) = new HashSet[String]()
        executorAdded(o.executorId, o.host)
        newExecAvail = true
      }
      for (rack <- getRackForHost(o.host)) {
        hostsByRack.getOrElseUpdate(rack, new HashSet[String]()) += o.host
      }
    }

    // Randomly shuffle offers to avoid always placing tasks on the same set of workers.
    val shuffledOffers = Random.shuffle(offers)
    // Build a list of tasks to assign to each worker.
    val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores)) //表示每一个节点上分配一组任务数组.数组的大小取决于该节点的core数量
    val availableCpus = shuffledOffers.map(o => o.cores).toArray //每一个节点的core空闲数量
    val sortedTaskSets = rootPool.getSortedTaskSetQueue //获取队列上任务的优先级
    for (taskSet <- sortedTaskSets) {
      logDebug("parentName: %s, name: %s, runningTasks: %s".format(
        taskSet.parent.name, taskSet.name, taskSet.runningTasks))
      if (newExecAvail) {
        taskSet.executorAdded()
      }
    }

    // Take each TaskSet in our scheduling order, and then offer it each node in increasing order
    // of locality levels so that it gets a chance to launch local tasks on all of them.
    // NOTE: the preferredLocality order: PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY
    var launchedTask = false
    for (taskSet <- sortedTaskSets; maxLocality <- taskSet.myLocalityLevels) {
      do {
        launchedTask = resourceOfferSingleTaskSet(
            taskSet, maxLocality, shuffledOffers, availableCpus, tasks)
      } while (launchedTask)
    }

    if (tasks.size > 0) {
      hasLaunchedTask = true
    }
    return tasks
  }

  /**
   * executor发送一个任务的统计信息
   * @param tid taskId
   * @param state 任务执行的状态
   * @param serializedData 统计内容
   */
  def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer) {
    var failedExecutor: Option[String] = None //失败的executor
    synchronized {
      try {
        if (state == TaskState.LOST && taskIdToExecutorId.contains(tid)) { //说明executor被丢失了
          // We lost this entire executor, so remember that it's gone
          val execId = taskIdToExecutorId(tid)
          if (activeExecutorIds.contains(execId)) {
            removeExecutor(execId)
            failedExecutor = Some(execId)
          }
        }
        taskIdToTaskSetManager.get(tid) match {//找到任务所对应的阶段对象
          case Some(taskSet) => //有该任务对应的阶段对象
            if (TaskState.isFinished(state)) {//executor的状态是完成了,不管是什么完成,总之是完成了,包含被kill,被丢失等操作
              taskIdToTaskSetManager.remove(tid)
              taskIdToExecutorId.remove(tid)
            }
            if (state == TaskState.FINISHED) {//任务成功完成该任务
              taskSet.removeRunningTask(tid)
              taskResultGetter.enqueueSuccessfulTask(taskSet, tid, serializedData)
            } else if (Set(TaskState.FAILED, TaskState.KILLED, TaskState.LOST).contains(state)) {
              taskSet.removeRunningTask(tid)
              taskResultGetter.enqueueFailedTask(taskSet, tid, state, serializedData)
            }
          case None =>
            logError(
              ("Ignoring update with state %s for TID %s because its task set is gone (this is " +
                "likely the result of receiving duplicate task finished status updates)")
                .format(state, tid))
        }
      } catch {
        case e: Exception => logError("Exception in statusUpdate", e)
      }
    }
    // Update the DAGScheduler without holding a lock on this, since that can deadlock
    if (failedExecutor.isDefined) {
      dagScheduler.executorLost(failedExecutor.get)
      backend.reviveOffers()
    }
  }

  /**
   * Update metrics for in-progress tasks and let the master know that the BlockManager is still
   * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   */
  override def executorHeartbeatReceived(
      execId: String,//表示该executor上发来的心跳报告
      taskMetrics: Array[(Long, TaskMetrics)], // taskId -> TaskMetrics 描述该executor上所有的任务执行情况
      blockManagerId: BlockManagerId): Boolean = {

    //返回值(任务ID,所属阶段ID,所属阶段的尝试任务ID,统计信息)
    val metricsWithStageIds: Array[(Long, Int, Int, TaskMetrics)] = synchronized {
      taskMetrics.flatMap { case (id, metrics) => //循环每一个任务以及报告情况
        taskIdToTaskSetManager.get(id).map { taskSetMgr => //获取该任务对应的阶段情况
          (id, taskSetMgr.stageId, taskSetMgr.taskSet.stageAttemptId, metrics)
        }
      }
    }
    dagScheduler.executorHeartbeatReceived(execId, metricsWithStageIds, blockManagerId)
  }

  def handleTaskGettingResult(taskSetManager: TaskSetManager, tid: Long): Unit = synchronized {
    taskSetManager.handleTaskGettingResult(tid)
  }

  //任务成功,并且已经将结果从executor上抓去到driver本地了
  def handleSuccessfulTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      taskResult: DirectTaskResult[_]): Unit = synchronized {
    taskSetManager.handleSuccessfulTask(tid, taskResult)
  }

  //任务失败
  def handleFailedTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      taskState: TaskState,
      reason: TaskEndReason): Unit = synchronized {
    taskSetManager.handleFailedTask(tid, taskState, reason)
    if (!taskSetManager.isZombie && taskState != TaskState.KILLED) {//重新发起调度,去将该失败的任务再次调度起来
      // Need to revive offers again now that the task set manager state has been updated to
      // reflect failed tasks that need to be re-run.
      backend.reviveOffers()
    }
  }

  //调度器出现错误了,因此所有的阶段系统都要被退出
  def error(message: String) {
    synchronized {
      if (taskSetsByStageIdAndAttempt.nonEmpty) {
        // Have each task set throw a SparkException with the error
        for {
          attempts <- taskSetsByStageIdAndAttempt.values
          manager <- attempts.values
        } {
          try {
            manager.abort(message)
          } catch {
            case e: Exception => logError("Exception in error callback", e)
          }
        }
      } else {
        // No task sets are active but we still got an error. Just exit since this
        // must mean the error is during registration.
        // It might be good to do something smarter here in the future.
        throw new SparkException(s"Exiting due to error from cluster scheduler: $message")
      }
    }
  }

  override def stop() {
    speculationScheduler.shutdown() //关闭推测任务进程
    if (backend != null) {
      backend.stop() //关闭后台进程
    }
    if (taskResultGetter != null) { //关闭获取结果的进程
      taskResultGetter.stop()
    }
    starvationTimer.cancel() //关闭定时器-----就一开始的时候使用过该线程,主要用于确定该调度器是否能成功调度任务
  }

  override def defaultParallelism(): Int = backend.defaultParallelism()

  // Check for speculatable tasks in all our active jobs.
  //检查推测任务
  def checkSpeculatableTasks() {
    var shouldRevive = false
    synchronized {
      shouldRevive = rootPool.checkSpeculatableTasks()
    }
    if (shouldRevive) {
      backend.reviveOffers()
    }
  }

  override def executorLost(executorId: String, reason: ExecutorLossReason): Unit = {
    var failedExecutor: Option[String] = None

    synchronized {
      if (activeExecutorIds.contains(executorId)) {
        val hostPort = executorIdToHost(executorId)
        logError("Lost executor %s on %s: %s".format(executorId, hostPort, reason))
        removeExecutor(executorId)
        failedExecutor = Some(executorId)
      } else {
         // We may get multiple executorLost() calls with different loss reasons. For example, one
         // may be triggered by a dropped connection from the slave while another may be a report
         // of executor termination from Mesos. We produce log messages for both so we eventually
         // report the termination reason.
         logError("Lost an executor " + executorId + " (already removed): " + reason)
      }
    }
    // Call dagScheduler.executorLost without holding the lock on this to prevent deadlock
    if (failedExecutor.isDefined) {
      dagScheduler.executorLost(failedExecutor.get)
      backend.reviveOffers()
    }
  }

  /** Remove an executor from all our data structures and mark it as lost 
   * 删除一个执行的进程映射
    * 1.清空该executor的内存关系
    * 2.通知队列池该executor上不能派发任务了
   **/
  private def removeExecutor(executorId: String) {
    activeExecutorIds -= executorId
    val host = executorIdToHost(executorId)
    val execs = executorsByHost.getOrElse(host, new HashSet)
    execs -= executorId
    if (execs.isEmpty) {
      executorsByHost -= host
      for (rack <- getRackForHost(host); hosts <- hostsByRack.get(rack)) {
        hosts -= host
        if (hosts.isEmpty) {
          hostsByRack -= rack
        }
      }
    }
    executorIdToHost -= executorId
    rootPool.executorLost(executorId, host)
  }

  def executorAdded(execId: String, host: String) {
    dagScheduler.executorAdded(execId, host)
  }

  //获取该host上有哪些executor集合
  def getExecutorsAliveOnHost(host: String): Option[Set[String]] = synchronized {
    executorsByHost.get(host).map(_.toSet)
  }

  //host上是否有executor进程
  def hasExecutorsAliveOnHost(host: String): Boolean = synchronized {
    executorsByHost.contains(host)
  }

  //该rack上是否有executor进程
  def hasHostAliveOnRack(rack: String): Boolean = synchronized {
    hostsByRack.contains(rack)
  }

  def isExecutorAlive(execId: String): Boolean = synchronized {
    activeExecutorIds.contains(execId)
  }

  // By default, rack is unknown
  def getRackForHost(value: String): Option[String] = None

  //等候,直到后台准备就绪
  private def waitBackendReady(): Unit = {
    if (backend.isReady) {
      return
    }
    while (!backend.isReady) {
      synchronized {
        this.wait(100)
      }
    }
  }

  //获取该调度器对应的应用ID
  override def applicationId(): String = backend.applicationId()
//获取该调度器对应的应用尝试次数
  override def applicationAttemptId(): Option[String] = backend.applicationAttemptId()

  //获取第stageAttemptId次尝试阶段对应的TaskSetManager对象
  private[scheduler] def taskSetManagerForAttempt(
      stageId: Int,
      stageAttemptId: Int): Option[TaskSetManager] = {
    for {
      attempts <- taskSetsByStageIdAndAttempt.get(stageId)
      manager <- attempts.get(stageAttemptId) //获取对应的TaskSetManager对象
    } yield {
      manager
    }
  }

}


private[spark] object TaskSchedulerImpl {
  /**
   * Used to balance containers across hosts.
   *
   * Accepts a map of hosts to resource offers for that host, and returns a prioritized list of
   * resource offers representing the order in which the offers should be used.  The resource
   * offers are ordered such that we'll allocate one container on each host before allocating a
   * second container on any host, and so on, in order to reduce the damage if a host fails.
   *
   * For example, given <h1, [o1, o2, o3]>, <h2, [o4]>, <h1, [o5, o6]>, returns
   * [o1, o5, o4, 02, o6, o3]
   * 该函数的意义是打乱顺序,从每一个key中获取一个元素,进行排序
    *
    * 排序规则
    * 1.按照key排序,
    * 2.从key对应的集合中每一次获取一个元素,比如10个key,则每次从10个key中获取一个值,然后在依次获取10个值,一直全部数据获取完结束
    *
    * 这样保证key的队列越大,越先被处理该任务,但是同时??又保证了每一个key对应的队列公平调度,每一个人拿出一个元素出来
   */
  def prioritizeContainers[K, T] (map: HashMap[K, ArrayBuffer[T]]): List[T] = {
    val _keyList = new ArrayBuffer[K](map.size) //获取所有的key集合
    _keyList ++= map.keys

    // order keyList based on population of value in map 对key集合进行排序,排序规则按照每一个key的内容数量从大到小排序
    val keyList = _keyList.sortWith(
      (left, right) => map(left).size > map(right).size //left和right分别表示连续的两个key,因此看key对应的集合的size大小进行排序
    )

    val retval = new ArrayBuffer[T](keyList.size * 2) //最终存储的集合
    var index = 0//该遍历第几个元素了
    var found = true

    while (found) {//只要一直true,就不断运行
      found = false//初始化为false,因为已经进入循环了,此时设置false目的是如果key循环后找不到比index大的,则不再循环
      //按照顺序从每一个key中获取一个元素,每次都获取每一个key对应的集合中最小元素,添加到最终集合中
      for (key <- keyList) {//从每一个队列中获取第index个元素内容(如果该元素不存在,则不需要获取)
        val containerList: ArrayBuffer[T] = map.get(key).getOrElse(null) //value内容
        assert(containerList != null)
        // Get the index'th entry for this host - if present
        if (index < containerList.size){//如果key中的元素不足,则不需要在添加该key对应的值了
          retval += containerList.apply(index)
          found = true //设置true,说明本次循环所有的key时,发现有index下标的元素
        }
      }
      index += 1//下标+1
    }

    retval.toList
  }

}
