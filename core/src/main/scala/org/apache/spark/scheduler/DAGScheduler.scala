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

import java.io.NotSerializableException
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.Map
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Stack}
import scala.concurrent.duration._
import scala.language.existentials
import scala.language.postfixOps
import scala.util.control.NonFatal

import org.apache.commons.lang3.SerializationUtils

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.partial.{ApproximateActionListener, ApproximateEvaluator, PartialResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.RpcTimeout
import org.apache.spark.storage._
import org.apache.spark.util._
import org.apache.spark.storage.BlockManagerMessages.BlockManagerHeartbeat

/**
 * The high-level scheduling layer that implements stage-oriented scheduling. 
 * 更高级别的调度层,实现以阶段为调度方向
 * It computes a DAG of  stages for each job, keeps track of which RDDs and stage outputs are materialized, and finds a
 * minimal schedule to run the job. It then submits stages as TaskSets to an underlying
 * TaskScheduler implementation that runs them on the cluster.
 *
 * In addition to coming up with a DAG of stages, this class also determines the preferred
 * locations to run each task on, based on the current cache status, and passes these to the
 * low-level TaskScheduler. Furthermore, it handles failures due to shuffle output files being
 * lost, in which case old stages may need to be resubmitted. Failures *within* a stage that are
 * not caused by shuffle file loss are handled by the TaskScheduler, which will retry each task
 * a small number of times before cancelling the whole stage.
 *
 * Here's a checklist to use when making or reviewing changes to this class:
 *
 *  - When adding a new data structure, update `DAGSchedulerSuite.assertDataStructuresEmpty` to
 *    include the new structure. This will help to catch memory leaks.
 *
 * 方法入口
 * def onReceive(event: DAGSchedulerEvent): 接受事件
 */
private[spark]
class DAGScheduler(
    private[scheduler] val sc: SparkContext,
    private[scheduler] val taskScheduler: TaskScheduler,
    listenerBus: LiveListenerBus,
    mapOutputTracker: MapOutputTrackerMaster,
    blockManagerMaster: BlockManagerMaster,
    env: SparkEnv,
    clock: Clock = new SystemClock())
  extends Logging {

  def this(sc: SparkContext, taskScheduler: TaskScheduler) = {
    this(
      sc,
      taskScheduler,
      sc.listenerBus,
      sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster],
      sc.env.blockManager.master,
      sc.env)
  }

  def this(sc: SparkContext) = this(sc, sc.taskScheduler)

  private[scheduler] val metricsSource: DAGSchedulerSource = new DAGSchedulerSource(this)

  //一个action对应一个job,因此会有多个job
  private[scheduler] val nextJobId = new AtomicInteger(0)
  private[scheduler] def numTotalJobs: Int = nextJobId.get()//总的job数量,即多少个action
  //一个job有多个阶段组成,因此有一个阶段的计数器
  private val nextStageId = new AtomicInteger(0)

  private[scheduler] val jobIdToStageIds = new HashMap[Int, HashSet[Int]]//该jobid 有哪些阶段集合
  private[scheduler] val stageIdToStage = new HashMap[Int, Stage] //存储每一个Stage的Id与Stage对象的内存映射
  private[scheduler] val shuffleToMapStage = new HashMap[Int, ShuffleMapStage]
  private[scheduler] val jobIdToActiveJob = new HashMap[Int, ActiveJob]//可以运行的job

  // Stages we need to run whose parents aren't done
  private[scheduler] val waitingStages = new HashSet[Stage]

  // Stages we are running right now
  private[scheduler] val runningStages = new HashSet[Stage]

  // Stages that must be resubmitted due to fetch failures
  private[scheduler] val failedStages = new HashSet[Stage]

  private[scheduler] val activeJobs = new HashSet[ActiveJob] //可以运行的job集合

  /**
   * Contains the locations that each RDD's partitions are cached on.  This map's keys are RDD ids
   * and its values are arrays indexed by partition numbers. Each array value is the set of
   * locations where that RDD partition is cached.
   * 获取每一个RDD的partition被缓存的位置,
   *
   * All accesses to this map should be guarded by synchronizing on it (see SPARK-4454).
   * 如果RDD的存储级别不是NONE,则存储每一个RDD的位置
   * key是rdd的唯一ID,value是RDD的每一个数据块对应的缓存在哪个位置,如果元素为Nui,说明该数据块没有被缓存
   */
  private val cacheLocs = new HashMap[Int, IndexedSeq[Seq[TaskLocation]]]

  // For tracking failed nodes, we use the MapOutputTracker's epoch number, which is sent with
  // every task. When we detect a node failing, we note the current epoch number and failed
  // executor, increment it for new tasks, and use this to ignore stray ShuffleMapTask results.
  //
  // TODO: Garbage collect information about failure epochs when we know there are no more
  //       stray messages to detect.
  private val failedEpoch = new HashMap[String, Long]

  private [scheduler] val outputCommitCoordinator = env.outputCommitCoordinator

  // A closure serializer that we reuse.
  // This is only safe because DAGScheduler runs in a single thread.
  private val closureSerializer = SparkEnv.get.closureSerializer.newInstance()

  /** If enabled, FetchFailed will not cause stage retry, in order to surface the problem. */
  private val disallowStageRetryForTest = sc.getConf.getBoolean("spark.test.noStageRetry", false)

  private val messageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("dag-scheduler-message")

  private[scheduler] val eventProcessLoop = new DAGSchedulerEventProcessLoop(this) //如何处理事件
  taskScheduler.setDAGScheduler(this)

  // Flag to control if reduce tasks are assigned preferred locations
  private val shuffleLocalityEnabled =
    sc.getConf.getBoolean("spark.shuffle.reduceLocality.enabled", false)
  // Number of map, reduce tasks above which we do not assign preferred locations
  // based on map output sizes. We limit the size of jobs for which assign preferred locations
  // as computing the top locations by size becomes expensive.
  private[this] val SHUFFLE_PREF_MAP_THRESHOLD = 1000
  // NOTE: This should be less than 2000 as we use HighlyCompressedMapStatus beyond that
  private[this] val SHUFFLE_PREF_REDUCE_THRESHOLD = 1000

  // Fraction of total map output that must be at a location for it to considered as a preferred
  // location for a reduce task.
  // Making this larger will focus on fewer locations where most data can be read locally, but
  // may lead to more delay in scheduling if those locations are busy.
  private[scheduler] val REDUCER_PREF_LOCS_FRACTION = 0.2

  /**
   * Called by the TaskSetManager to report task's starting.
   */
  def taskStarted(task: Task[_], taskInfo: TaskInfo) {
    eventProcessLoop.post(BeginEvent(task, taskInfo))
  }

  /**
   * Called by the TaskSetManager to report that a task has completed
   * and results are being fetched remotely.
   */
  def taskGettingResult(taskInfo: TaskInfo) {
    eventProcessLoop.post(GettingResultEvent(taskInfo))
  }

  /**
   * Called by the TaskSetManager to report task completions or failures.
   * 调度去通知该任务执行完成
   */
  def taskEnded(
      task: Task[_],
      reason: TaskEndReason,
      result: Any,
      accumUpdates: Map[Long, Any],
      taskInfo: TaskInfo,
      taskMetrics: TaskMetrics): Unit = {
    eventProcessLoop.post(
      CompletionEvent(task, reason, result, accumUpdates, taskInfo, taskMetrics))
  }

  /**
   * Update metrics for in-progress tasks and let the master know that the BlockManager is still
   * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   */
  def executorHeartbeatReceived(
      execId: String,
      taskMetrics: Array[(Long, Int, Int, TaskMetrics)], // (taskId, stageId, stateAttempt, metrics)
      blockManagerId: BlockManagerId): Boolean = {
    listenerBus.post(SparkListenerExecutorMetricsUpdate(execId, taskMetrics))
    blockManagerMaster.driverEndpoint.askWithRetry[Boolean](
      BlockManagerHeartbeat(blockManagerId), new RpcTimeout(600 seconds, "BlockManagerHeartbeat"))
  }

  /**
   * Called by TaskScheduler implementation when an executor fails.
   */
  def executorLost(execId: String): Unit = {
    eventProcessLoop.post(ExecutorLost(execId))
  }

  /**
   * Called by TaskScheduler implementation when a host is added.
   */
  def executorAdded(execId: String, host: String): Unit = {
    eventProcessLoop.post(ExecutorAdded(execId, host))
  }

  /**
   * Called by the TaskSetManager to cancel an entire TaskSet due to either repeated failures or
   * cancellation of the job itself.
   */
  def taskSetFailed(taskSet: TaskSet, reason: String, exception: Option[Throwable]): Unit = {
    eventProcessLoop.post(TaskSetFailed(taskSet, reason, exception))
  }

  private[scheduler]
  def getCacheLocs(rdd: RDD[_]): IndexedSeq[Seq[TaskLocation]] = cacheLocs.synchronized {
    // Note: this doesn't use `getOrElse()` because this method is called O(num tasks) times
    if (!cacheLocs.contains(rdd.id)) {//说明缓存未命中
      // Note: if the storage level is NONE, we don't need to get locations from block manager.如果存储级别为NONE,我们是不需要从数据块管理器中获取路径的
      val locs: IndexedSeq[Seq[TaskLocation]] = if (rdd.getStorageLevel == StorageLevel.NONE) {
        IndexedSeq.fill(rdd.partitions.length)(Nil)//因此返回partition数量的nil即可
      } else {//说明有存储
        val blockIds =
          rdd.partitions.indices.map(index => RDDBlockId(rdd.id, index)).toArray[BlockId] //返回BlockId集合
        blockManagerMaster.getLocations(blockIds).map { bms =>
          bms.map(bm => TaskLocation(bm.host, bm.executorId)) //获取每一个数据块的位置
        }
      }
      cacheLocs(rdd.id) = locs //存储到缓存中
    }
    cacheLocs(rdd.id) //说明缓存命中
  }

  private def clearCacheLocs(): Unit = cacheLocs.synchronized {
    cacheLocs.clear()
  }

  /**
   * Get or create a shuffle map stage for the given shuffle dependency's map side.
   */
  private def getShuffleMapStage(
      shuffleDep: ShuffleDependency[_, _, _],
      firstJobId: Int): ShuffleMapStage = {
    shuffleToMapStage.get(shuffleDep.shuffleId) match {
      case Some(stage) => stage
      case None =>
        // We are going to register ancestor shuffle dependencies
        registerShuffleDependencies(shuffleDep, firstJobId)
        // Then register current shuffleDep
        val stage = newOrUsedShuffleStage(shuffleDep, firstJobId)
        shuffleToMapStage(shuffleDep.shuffleId) = stage

        stage
    }
  }

  /**
   * Helper function to eliminate some code re-use when creating new stages.
   * 解析该jobId基于rdd会产生多少个阶段,以及每一个阶段会产生的子阶段
   *
   * 返回唯一的ID和该job产生的阶段集合
   */
  private def getParentStagesAndId(rdd: RDD[_], firstJobId: Int): (List[Stage], Int) = {
    val parentStages = getParentStages(rdd, firstJobId)//解析该job需要的Stage集合
    val id = nextStageId.getAndIncrement()//设置stage唯一ID
    (parentStages, id)
  }

  /**
   * Create a ShuffleMapStage as part of the (re)-creation of a shuffle map stage in
   * newOrUsedShuffleStage.  The stage will be associated with the provided firstJobId.
   * Production of shuffle map stages should always use newOrUsedShuffleStage, not
   * newShuffleMapStage directly.
   */
  private def newShuffleMapStage(
      rdd: RDD[_],
      numTasks: Int,
      shuffleDep: ShuffleDependency[_, _, _],
      firstJobId: Int,
      callSite: CallSite): ShuffleMapStage = {
    val (parentStages: List[Stage], id: Int) = getParentStagesAndId(rdd, firstJobId)
    val stage: ShuffleMapStage = new ShuffleMapStage(id, rdd, numTasks, parentStages,
      firstJobId, callSite, shuffleDep)

    stageIdToStage(id) = stage
    updateJobIdStageIdMaps(firstJobId, stage)
    stage
  }

  /**
   * Create a ResultStage associated with the provided jobId.
   * 为该job创建stage阶段集合对象
   */
  private def newResultStage(
      rdd: RDD[_],//该数据源RDD
      numTasks: Int,//该RDD有多少个分区,因此就有多少个并行任务去执行
      jobId: Int,//该job的ID
      callSite: CallSite): ResultStage = {
    val (parentStages: List[Stage], id: Int) = getParentStagesAndId(rdd, jobId) //为该job对应的RDD创建阶段集合,返回阶段集合以及对应的ID
    val stage: ResultStage = new ResultStage(id, rdd, numTasks, parentStages, jobId, callSite)

    stageIdToStage(id) = stage //存储每一个Stage的Id与Stage对象的内存映射
    updateJobIdStageIdMaps(jobId, stage) //更新jobId和Stage集合的映射关系
    stage
  }

  /**
   * Create a shuffle map Stage for the given RDD.  The stage will also be associated with the
   * provided firstJobId.  If a stage for the shuffleId existed previously so that the shuffleId is
   * present in the MapOutputTracker, then the number and location of available outputs are
   * recovered from the MapOutputTracker
   */
  private def newOrUsedShuffleStage(
      shuffleDep: ShuffleDependency[_, _, _],
      firstJobId: Int): ShuffleMapStage = {
    val rdd = shuffleDep.rdd
    val numTasks = rdd.partitions.length
    val stage = newShuffleMapStage(rdd, numTasks, shuffleDep, firstJobId, rdd.creationSite)
    if (mapOutputTracker.containsShuffle(shuffleDep.shuffleId)) {
      val serLocs = mapOutputTracker.getSerializedMapOutputStatuses(shuffleDep.shuffleId)
      val locs = MapOutputTracker.deserializeMapStatuses(serLocs)
      for (i <- 0 until locs.length) {
        stage.outputLocs(i) = Option(locs(i)).toList // locs(i) will be null if missing
      }
      stage.numAvailableOutputs = locs.count(_ != null)
    } else {
      // Kind of ugly: need to register RDDs with the cache and map output tracker here
      // since we can't do it in the RDD constructor because # of partitions is unknown
      logInfo("Registering RDD " + rdd.id + " (" + rdd.getCreationSite + ")")
      mapOutputTracker.registerShuffle(shuffleDep.shuffleId, rdd.partitions.length)
    }
    stage
  }

  /**
   * Get or create the list of parent stages for a given RDD.  The new Stages will be created with
   * the provided firstJobId.
   * 返回该job依赖哪些Stage集合--所谓的parent表示该job依赖这些阶段,因此这些阶段对于该job来说就是父阶段
   */
  private def getParentStages(rdd: RDD[_], firstJobId: Int): List[Stage] = {
    val parents = new HashSet[Stage] //该job依赖哪些Stage集合
    val visited = new HashSet[RDD[_]]//已经访问了哪些RDD
    // We are manually maintaining a stack here to prevent StackOverflowError caused by recursively visiting
    // 我们手动保持堆栈信息,去防止通过递归访问导致的StackOverflowError产生
    val waitingForVisit = new Stack[RDD[_]]//创建堆栈对象,堆栈存储的是RDD对象作为元素

    def visit(r: RDD[_]) {//每次操作一个RDD对象
      if (!visited(r)) {//先确定该rdd没有被访问过
        visited += r //添加该RDD已经被访问过
        // Kind of ugly: need to register RDDs with the cache here since
        // we can't do it in its constructor because # of partitions is unknown
        for (dep <- r.dependencies) {//看该RDD依赖什么
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              parents += getShuffleMapStage(shufDep, firstJobId) //遇见ShuffleDependency,则需要一个Stage
            case _ =>
              waitingForVisit.push(dep.rdd)//说明不是ShuffleDependency,因此可以保存在一个Stage中处理
          }
        }
      }
    }
    waitingForVisit.push(rdd)//先将本次的RDD存到堆栈中
    while (waitingForVisit.nonEmpty) {//一直到堆栈空为止
      visit(waitingForVisit.pop())//每次获取一个堆栈上的RDD
    }
    parents.toList //返回该job以来的Stage集合
  }

  /** Find ancestor missing shuffle dependencies and register into shuffleToMapStage */
  private def registerShuffleDependencies(shuffleDep: ShuffleDependency[_, _, _], firstJobId: Int) {
    val parentsWithNoMapStage = getAncestorShuffleDependencies(shuffleDep.rdd)
    while (parentsWithNoMapStage.nonEmpty) {
      val currentShufDep = parentsWithNoMapStage.pop()
      val stage = newOrUsedShuffleStage(currentShufDep, firstJobId)
      shuffleToMapStage(currentShufDep.shuffleId) = stage
    }
  }

  /** Find ancestor shuffle dependencies that are not registered in shuffleToMapStage yet */
  private def getAncestorShuffleDependencies(rdd: RDD[_]): Stack[ShuffleDependency[_, _, _]] = {
    val parents = new Stack[ShuffleDependency[_, _, _]]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(r: RDD[_]) {
      if (!visited(r)) {
        visited += r
        for (dep <- r.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              if (!shuffleToMapStage.contains(shufDep.shuffleId)) {
                parents.push(shufDep)
              }

              waitingForVisit.push(shufDep.rdd)
            case _ =>
              waitingForVisit.push(dep.rdd)
          }
        }
      }
    }

    waitingForVisit.push(rdd)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    parents
  }

  //计算缺失的阶段
  private def getMissingParentStages(stage: Stage): List[Stage] = {
    val missing = new HashSet[Stage] //最终缺失的阶段
    val visited = new HashSet[RDD[_]]//存储访问过的RDD集合
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]

    def visit(rdd: RDD[_]) {
      if (!visited(rdd)) {
        visited += rdd
        val rddHasUncachedPartitions = getCacheLocs(rdd).contains(Nil) //获取RDD的所有数据块的缓存位置,如果包含Nil,说明至少有一个数据块是没有被缓存的
        if (rddHasUncachedPartitions) {//说明有数据块没有被缓存
          for (dep <- rdd.dependencies) {
            dep match {
              case shufDep: ShuffleDependency[_, _, _] =>
                val mapStage = getShuffleMapStage(shufDep, stage.firstJobId)
                if (!mapStage.isAvailable) {//说明该阶段不可用
                  missing += mapStage
                }
              case narrowDep: NarrowDependency[_] =>
                waitingForVisit.push(narrowDep.rdd)
            }
          }
        }
      }
    }

    waitingForVisit.push(stage.rdd)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    missing.toList
  }

  /**
   * Registers the given jobId among the jobs that need the given stage and
   * all of that stage's ancestors.
   * 更新jobId和Stage集合的映射关系
   *
   * 参数是job的Id和最终产生的一个阶段ResultStage
   */
  private def updateJobIdStageIdMaps(jobId: Int, stage: Stage): Unit = {
    def updateJobIdStageIdMapsList(stages: List[Stage]) {
      if (stages.nonEmpty) {//只要阶段还存在,则不断递归
        val s = stages.head //获取第一个阶段
        s.jobIds += jobId //可能该阶段被多个job共同使用,因此这里面是使用该阶段的job集合
        jobIdToStageIds.getOrElseUpdate(jobId, new HashSet[Int]()) += s.id //添加该job与阶段映射
        val parents: List[Stage] = getParentStages(s.rdd, jobId)
        val parentsWithoutThisJobId = parents.filter { ! _.jobIds.contains(jobId) }//说明该阶段依赖的父依赖没有该jobId的,要加入进去
        updateJobIdStageIdMapsList(parentsWithoutThisJobId ++ stages.tail) //继续递归
      }
    }
    updateJobIdStageIdMapsList(List(stage))
  }

  /**
   * Removes state for job and any stages that are not needed by any other job.  Does not
   * handle cancelling tasks or notifying the SparkListener about finished jobs/stages/tasks.
   *
   * @param job The job whose state to cleanup.
   * 删除一个job后,清理该job依赖的所有阶段信息
   */
  private def cleanupStateForJobAndIndependentStages(job: ActiveJob): Unit = {
    val registeredStages = jobIdToStageIds.get(job.jobId)//找到该job依赖哪些阶段
    if (registeredStages.isEmpty || registeredStages.get.isEmpty) {//说明不依赖任何阶段
      logError("No stages registered for job " + job.jobId)
    } else {//循环该job依赖的所有阶段集合
      stageIdToStage.filterKeys(stageId => registeredStages.get.contains(stageId)).foreach {
        case (stageId, stage) =>
          val jobSet = stage.jobIds //依赖该阶段的job集合
          if (!jobSet.contains(job.jobId)) {//该job没拥有该阶段,则打印日志即可
            logError(
              "Job %d not registered for stage %d even though that stage was registered for the job"
              .format(job.jobId, stageId))
          } else {
            def removeStage(stageId: Int) {//彻底删除一个阶段
              // data structures based on Stage
              for (stage <- stageIdToStage.get(stageId)) {
                if (runningStages.contains(stage)) {
                  logDebug("Removing running stage %d".format(stageId))
                  runningStages -= stage
                }
                for ((k, v) <- shuffleToMapStage.find(_._2 == stage)) {
                  shuffleToMapStage.remove(k)
                }
                if (waitingStages.contains(stage)) {
                  logDebug("Removing stage %d from waiting set.".format(stageId))
                  waitingStages -= stage
                }
                if (failedStages.contains(stage)) {
                  logDebug("Removing stage %d from failed set.".format(stageId))
                  failedStages -= stage
                }
              }
              // data structures based on StageId
              stageIdToStage -= stageId
              logDebug("After removal of stage %d, remaining stages = %d"
                .format(stageId, stageIdToStage.size))
            }

            jobSet -= job.jobId //使该job不再依赖该阶段
            if (jobSet.isEmpty) { // no other job needs this stage  如果该阶段没有任何job依赖,则删除该阶段
              removeStage(stageId)
            }
          }
      }
    }

    //清理job内容
    jobIdToStageIds -= job.jobId
    jobIdToActiveJob -= job.jobId
    activeJobs -= job
    job.finalStage.resultOfJob = None
  }

  /**
   * Submit a job to the job scheduler and get a JobWaiter object back. The JobWaiter object
   * can be used to block until the the job finishes executing or can be used to cancel the job.
   */
  def submitJob[T, U](
      rdd: RDD[T],//要提交的RDD,在哪个RDD上发生的Action
      func: (TaskContext, Iterator[T]) => U,//如何处理该RDD上每一个partition的每一行数据
      partitions: Seq[Int],//一共有多少个partition
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties): JobWaiter[U] = {
    // Check to make sure we are not launching a task on a partition that does not exist.
    val maxPartitions = rdd.partitions.length //rdd对应多少个分区
    partitions.find(p => p >= maxPartitions || p < 0).foreach { p =>
      throw new IllegalArgumentException(
        "Attempting to access a non-existent partition: " + p + ". " +
          "Total number of partitions: " + maxPartitions)
    }

    val jobId = nextJobId.getAndIncrement() //产生一个新的jobID
    if (partitions.size == 0) {//说明没有分区,基本上不应该
      return new JobWaiter[U](this, jobId, 0, resultHandler)
    }

    assert(partitions.size > 0)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _] //任务上下文和如何处理该partition的每一行记录
    val waiter = new JobWaiter(this, jobId, partitions.size, resultHandler)
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions.toArray, callSite, waiter,
      SerializationUtils.clone(properties)))
    waiter
  }

  def runJob[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties): Unit = {
    val start = System.nanoTime
    val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties)
    waiter.awaitResult() match {//一直等待任务执行完成
      case JobSucceeded =>
        logInfo("Job %d finished: %s, took %f s".format
          (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
      case JobFailed(exception: Exception) =>
        logInfo("Job %d failed: %s, took %f s".format
          (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
        // SPARK-8644: Include user stack trace in exceptions coming from DAGScheduler.
        val callerStackTrace = Thread.currentThread().getStackTrace.tail
        exception.setStackTrace(exception.getStackTrace ++ callerStackTrace)
        throw exception
    }
  }

  def runApproximateJob[T, U, R](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      evaluator: ApproximateEvaluator[U, R],
      callSite: CallSite,
      timeout: Long,
      properties: Properties): PartialResult[R] = {
    val listener = new ApproximateActionListener(rdd, func, evaluator, timeout)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val partitions = (0 until rdd.partitions.length).toArray
    val jobId = nextJobId.getAndIncrement()
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions, callSite, listener, SerializationUtils.clone(properties)))
    listener.awaitResult()    // Will throw an exception if the job fails
  }

  /**
   * Cancel a job that is running or waiting in the queue.
   */
  def cancelJob(jobId: Int): Unit = {
    logInfo("Asked to cancel job " + jobId)
    eventProcessLoop.post(JobCancelled(jobId))
  }

  def cancelJobGroup(groupId: String): Unit = {
    logInfo("Asked to cancel job group " + groupId)
    eventProcessLoop.post(JobGroupCancelled(groupId))
  }

  /**
   * Cancel all jobs that are running or waiting in the queue.
   */
  def cancelAllJobs(): Unit = {
    eventProcessLoop.post(AllJobsCancelled)
  }

  //停止所有的job任务
  private[scheduler] def doCancelAllJobs() {
    // Cancel all running jobs.
    runningStages.map(_.firstJobId).foreach(handleJobCancellation(_,
      reason = "as part of cancellation of all jobs"))
    activeJobs.clear() // These should already be empty by this point,
    jobIdToActiveJob.clear() // but just in case we lost track of some jobs...
    submitWaitingStages()
  }

  /**
   * Cancel all jobs associated with a running or scheduled stage.
   */
  def cancelStage(stageId: Int) {
    eventProcessLoop.post(StageCancelled(stageId))
  }

  /**
   * Resubmit any failed stages. Ordinarily called after a small amount of time has passed since
   * the last fetch failure.
   */
  private[scheduler] def resubmitFailedStages() {
    if (failedStages.size > 0) {
      // Failed stages may be removed by job cancellation, so failed might be empty even if
      // the ResubmitFailedStages event has been scheduled.
      logInfo("Resubmitting failed stages")
      clearCacheLocs()
      val failedStagesCopy = failedStages.toArray
      failedStages.clear()
      for (stage <- failedStagesCopy.sortBy(_.firstJobId)) {
        submitStage(stage)
      }
    }
    submitWaitingStages()
  }

  /**
   * Check for waiting or failed stages which are now eligible for resubmission.
   * Ordinarily run on every iteration of the event loop.
   * 提交等待的阶段
   */
  private def submitWaitingStages() {
    // TODO: We might want to run this less often, when we are sure that something has become
    // runnable that wasn't before.
    logTrace("Checking for newly runnable parent stages")
    logTrace("running: " + runningStages)
    logTrace("waiting: " + waitingStages)
    logTrace("failed: " + failedStages)
    val waitingStagesCopy = waitingStages.toArray //获取等待的阶段的备份
    waitingStages.clear()//清空等待的阶段集合
    for (stage <- waitingStagesCopy.sortBy(_.firstJobId)) {//循环等待的阶段,然后以此提交这些阶段
      submitStage(stage)
    }
  }

  /** Finds the earliest-created active job that needs the stage */
  // TODO: Probably should actually find among the active jobs that need this
  // stage the one with the highest priority (highest-priority pool, earliest created).
  // That should take care of at least part of the priority inversion problem with
  // cross-job dependencies.
  //使用该阶段的job集合 与 现在活着的job集合做交集,返回找到第一个存在的jobId
  private def activeJobForStage(stage: Stage): Option[Int] = {
    val jobsThatUseStage: Array[Int] = stage.jobIds.toArray.sorted //获取该阶段对应的job集合
    jobsThatUseStage.find(jobIdToActiveJob.contains)//与目前存在活着的job集合做交集,找到第一个存在的jobId
  }

  //取消属于该组的所有job
  private[scheduler] def handleJobGroupCancelled(groupId: String) {
    // Cancel all jobs belonging to this job group.
    // First finds all active jobs with this group id, and then kill stages for them.
    //循环所有的job,找到属于该组的job集合
    val activeInGroup = activeJobs.filter { activeJob =>
      Option(activeJob.properties).exists {
        _.getProperty(SparkContext.SPARK_JOB_GROUP_ID) == groupId
      }
    }
    val jobIds = activeInGroup.map(_.jobId) //将其转换成jobID
    jobIds.foreach(handleJobCancellation(_, "part of cancelled job group %s".format(groupId))) //真正取消每一个job
    submitWaitingStages()
  }

  private[scheduler] def handleBeginEvent(task: Task[_], taskInfo: TaskInfo) {
    // Note that there is a chance that this task is launched after the stage is cancelled.
    // In that case, we wouldn't have the stage anymore in stageIdToStage.
    val stageAttemptId = stageIdToStage.get(task.stageId).map(_.latestInfo.attemptId).getOrElse(-1)
    listenerBus.post(SparkListenerTaskStart(task.stageId, stageAttemptId, taskInfo))
    submitWaitingStages()
  }

  private[scheduler] def handleTaskSetFailed(
      taskSet: TaskSet,
      reason: String,
      exception: Option[Throwable]): Unit = {
    stageIdToStage.get(taskSet.stageId).foreach { abortStage(_, reason, exception) }
    submitWaitingStages()
  }

  private[scheduler] def cleanUpAfterSchedulerStop() {
    for (job <- activeJobs) {
      val error = new SparkException("Job cancelled because SparkContext was shut down")
      job.listener.jobFailed(error)
      // Tell the listeners that all of the running stages have ended.  Don't bother
      // cancelling the stages because if the DAG scheduler is stopped, the entire application
      // is in the process of getting stopped.
      val stageFailedMessage = "Stage cancelled because SparkContext was shut down"
      // The `toArray` here is necessary so that we don't iterate over `runningStages` while
      // mutating it.
      runningStages.toArray.foreach { stage =>
        markStageAsFinished(stage, Some(stageFailedMessage))
      }
      listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobFailed(error)))
    }
  }

  private[scheduler] def handleGetTaskResult(taskInfo: TaskInfo) {
    listenerBus.post(SparkListenerTaskGettingResult(taskInfo))
    submitWaitingStages()
  }

  //说明该job提交了,去解析该job的阶段依赖
  private[scheduler] def handleJobSubmitted(jobId: Int,
      finalRDD: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      callSite: CallSite,
      listener: JobListener,
      properties: Properties) {
    var finalStage: ResultStage = null //最终该job分配的阶段集合
    try {
      // New stage creation may throw an exception if, for example, jobs are run on a
      // HadoopRDD whose underlying HDFS files have been deleted.
      //创建新stage阶段可能会有异常抛出,例如job运行一个HadoopRdd,但是该hdfs对应的file不存在,则会抛异常
      finalStage = newResultStage(finalRDD, partitions.length, jobId, callSite) //为该job创建stage阶段集合对象
    } catch {
      case e: Exception =>
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }
    if (finalStage != null) {
      val job = new ActiveJob(jobId, finalStage, func, partitions, callSite, listener, properties)
      clearCacheLocs()
      logInfo("Got job %s (%s) with %d output partitions".format(
        job.jobId, callSite.shortForm, partitions.length))
      logInfo("Final stage: " + finalStage + "(" + finalStage.name + ")")
      logInfo("Parents of final stage: " + finalStage.parents)
      logInfo("Missing parents: " + getMissingParentStages(finalStage))
      val jobSubmissionTime = clock.getTimeMillis()
      jobIdToActiveJob(jobId) = job
      activeJobs += job
      finalStage.resultOfJob = Some(job)
      val stageIds = jobIdToStageIds(jobId).toArray //获取该job对应的阶段集合
      val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo)) //创建StageInfo对象集合
      listenerBus.post(
        SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))//发送job提交事件
      submitStage(finalStage)
    }
    submitWaitingStages()
  }

  /** Submits stage, but first recursively submits any missing parents.
    * 提交一个阶段
    **/
  private def submitStage(stage: Stage) {
    val jobId = activeJobForStage(stage) //使用该阶段的job集合 与 现在活着的job集合做交集,返回找到第一个存在的jobId
    if (jobId.isDefined) {//说明有jobid使用该阶段
      logDebug("submitStage(" + stage + ")")

      //该阶段没有等待中 没有运行中 没有失败中,则可以进行处理
      if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
        val missing = getMissingParentStages(stage).sortBy(_.id) //缺失的阶段,按照id排序
        logDebug("missing: " + missing)
        if (missing.isEmpty) {//没有缺失
          logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
          submitMissingTasks(stage, jobId.get)
        } else {//缺失
          for (parent <- missing) {
            submitStage(parent)
          }
          waitingStages += stage //阶段等待
        }
      }
    } else {//说明该阶段不会被任何jobid使用
      abortStage(stage, "No active job for stage " + stage.id, None)//说明没有job持有该阶段
    }
  }

  /** Called when stage's parents are available and we can now do its task.
    * 对缺失的阶段进行重新运算,创建运算的task任务集合,去运行该集合
    **/
  private def submitMissingTasks(stage: Stage, jobId: Int) {
    logDebug("submitMissingTasks(" + stage + ")")
    // Get our pending tasks and remember them in our pendingTasks entry
    stage.pendingTasks.clear()

    // First figure out the indexes of partition ids to compute.
    //返回元组(所有的partition集合,需要去计算的partition集合)
    val (allPartitions: Seq[Int], partitionsToCompute: Seq[Int]) = {
      stage match {
        case stage: ShuffleMapStage =>
          val allPartitions = 0 until stage.numPartitions //该阶段依赖的所有的partition
          val filteredPartitions = allPartitions.filter { id => stage.outputLocs(id).isEmpty } //仅要parttion没有结果集的partition
          (allPartitions, filteredPartitions)
        case stage: ResultStage =>
          val job = stage.resultOfJob.get
          val allPartitions = 0 until job.numPartitions
          val filteredPartitions = allPartitions.filter { id => !job.finished(id) }//找到尚未完成的partition集合
          (allPartitions, filteredPartitions)
      }
    }

    // Create internal accumulators if the stage has no accumulators initialized.
    // Reset internal accumulators only if this stage is not partially submitted
    // Otherwise, we may override existing accumulator values from some tasks
    if (stage.internalAccumulators.isEmpty || allPartitions == partitionsToCompute) {
      stage.resetInternalAccumulators()
    }

    val properties = jobIdToActiveJob.get(stage.firstJobId).map(_.properties).orNull

    runningStages += stage
    // SparkListenerStageSubmitted should be posted before testing whether tasks are
    // serializable. If tasks are not serializable, a SparkListenerStageCompleted event
    // will be posted, which should always come after a corresponding SparkListenerStageSubmitted
    // event.
    outputCommitCoordinator.stageStart(stage.id)

    //对尚未完成的数据块进行处理,返回要在哪个节点上执行该数据块的元组(partition序号,哪个节点执行)
    val taskIdToLocations = try {
      stage match {
        case s: ShuffleMapStage =>
          partitionsToCompute.map { id => (id, getPreferredLocs(stage.rdd, id))}.toMap
        case s: ResultStage =>
          val job = s.resultOfJob.get
          partitionsToCompute.map { id =>
            val p = job.partitions(id)
            (id, getPreferredLocs(stage.rdd, p))
          }.toMap
      }
    } catch {
      case NonFatal(e) =>
        stage.makeNewStageAttempt(partitionsToCompute.size)
        listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))
        abortStage(stage, s"Task creation failed: $e\n${e.getStackTraceString}", Some(e))
        runningStages -= stage
        return
    }

    stage.makeNewStageAttempt(partitionsToCompute.size, taskIdToLocations.values.toSeq)
    listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))

    // TODO: Maybe we can keep the taskBinary in Stage to avoid serializing it multiple times.
    // Broadcasted binary for the task, used to dispatch tasks to executors. Note that we broadcast
    // the serialized copy of the RDD and for each task we will deserialize it, which means each
    // task gets a different copy of the RDD. This provides stronger isolation between tasks that
    // might modify state of objects referenced in their closures. This is necessary in Hadoop
    // where the JobConf/Configuration object is not thread-safe.
    var taskBinary: Broadcast[Array[Byte]] = null
    try {
      // For ShuffleMapTask, serialize and broadcast (rdd, shuffleDep).
      // For ResultTask, serialize and broadcast (rdd, func).
      val taskBinaryBytes: Array[Byte] = stage match {
        case stage: ShuffleMapStage =>
          closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef).array()
        case stage: ResultStage =>
          closureSerializer.serialize((stage.rdd, stage.resultOfJob.get.func): AnyRef).array()
      }

      taskBinary = sc.broadcast(taskBinaryBytes)
    } catch {
      // In the case of a failure during serialization, abort the stage.
      case e: NotSerializableException =>
        abortStage(stage, "Task not serializable: " + e.toString, Some(e))
        runningStages -= stage

        // Abort execution
        return
      case NonFatal(e) =>
        abortStage(stage, s"Task serialization failed: $e\n${e.getStackTraceString}", Some(e))
        runningStages -= stage
        return
    }

    val tasks: Seq[Task[_]] = try {
      stage match {
        case stage: ShuffleMapStage =>
          partitionsToCompute.map { id =>
            val locs = taskIdToLocations(id)
            val part = stage.rdd.partitions(id)
            new ShuffleMapTask(stage.id, stage.latestInfo.attemptId,
              taskBinary, part, locs, stage.internalAccumulators)
          }

        case stage: ResultStage =>
          val job = stage.resultOfJob.get
          partitionsToCompute.map { id =>
            val p: Int = job.partitions(id)
            val part = stage.rdd.partitions(p)
            val locs = taskIdToLocations(id)
            new ResultTask(stage.id, stage.latestInfo.attemptId,
              taskBinary, part, locs, id, stage.internalAccumulators)
          }
      }
    } catch {
      case NonFatal(e) =>
        abortStage(stage, s"Task creation failed: $e\n${e.getStackTraceString}", Some(e))
        runningStages -= stage
        return
    }

    if (tasks.size > 0) {
      logInfo("Submitting " + tasks.size + " missing tasks from " + stage + " (" + stage.rdd + ")")
      stage.pendingTasks ++= tasks
      logDebug("New pending tasks: " + stage.pendingTasks)
      taskScheduler.submitTasks(new TaskSet(
        tasks.toArray, stage.id, stage.latestInfo.attemptId, stage.firstJobId, properties))
      stage.latestInfo.submissionTime = Some(clock.getTimeMillis())
    } else {
      // Because we posted SparkListenerStageSubmitted earlier, we should mark
      // the stage as completed here in case there are no tasks to run
      markStageAsFinished(stage, None)

      val debugString = stage match {
        case stage: ShuffleMapStage =>
          s"Stage ${stage} is actually done; " +
            s"(available: ${stage.isAvailable}," +
            s"available outputs: ${stage.numAvailableOutputs}," +
            s"partitions: ${stage.numPartitions})"
        case stage : ResultStage =>
          s"Stage ${stage} is actually done; (partitions: ${stage.numPartitions})"
      }
      logDebug(debugString)
    }
  }

  /** Merge updates from a task to our local accumulator values */
  private def updateAccumulators(event: CompletionEvent): Unit = {
    val task = event.task
    val stage = stageIdToStage(task.stageId)
    if (event.accumUpdates != null) {
      try {
        Accumulators.add(event.accumUpdates)

        event.accumUpdates.foreach { case (id, partialValue) =>
          // In this instance, although the reference in Accumulators.originals is a WeakRef,
          // it's guaranteed to exist since the event.accumUpdates Map exists

          val acc = Accumulators.originals(id).get match {
            case Some(accum) => accum.asInstanceOf[Accumulable[Any, Any]]
            case None => throw new NullPointerException("Non-existent reference to Accumulator")
          }

          // To avoid UI cruft, ignore cases where value wasn't updated
          if (acc.name.isDefined && partialValue != acc.zero) {
            val name = acc.name.get
            val value = s"${acc.value}"
            stage.latestInfo.accumulables(id) =
              new AccumulableInfo(id, name, None, value, acc.isInternal)
            event.taskInfo.accumulables +=
              new AccumulableInfo(id, name, Some(s"$partialValue"), value, acc.isInternal)
          }
        }
      } catch {
        // If we see an exception during accumulator update, just log the
        // error and move on.
        case e: Exception =>
          logError(s"Failed to update accumulators for $task", e)
      }
    }
  }

  /**
   * Responds to a task finishing. This is called inside the event loop so it assumes that it can
   * modify the scheduler's internal state. Use taskEnded() to post a task end event from outside.
   */
  private[scheduler] def handleTaskCompletion(event: CompletionEvent) {
    val task = event.task
    val stageId = task.stageId
    val taskType = Utils.getFormattedClassName(task)

    outputCommitCoordinator.taskCompleted(
      stageId,
      task.partitionId,
      event.taskInfo.attemptNumber, // this is a task attempt number
      event.reason)

    // The success case is dealt with separately below, since we need to compute accumulator
    // updates before posting.
    if (event.reason != Success) {
      val attemptId = task.stageAttemptId
      listenerBus.post(SparkListenerTaskEnd(stageId, attemptId, taskType, event.reason,
        event.taskInfo, event.taskMetrics))
    }

    if (!stageIdToStage.contains(task.stageId)) {
      // Skip all the actions if the stage has been cancelled.
      return
    }

    val stage = stageIdToStage(task.stageId)
    event.reason match {
      case Success =>
        listenerBus.post(SparkListenerTaskEnd(stageId, stage.latestInfo.attemptId, taskType,
          event.reason, event.taskInfo, event.taskMetrics))
        stage.pendingTasks -= task
        task match {
          case rt: ResultTask[_, _] =>
            // Cast to ResultStage here because it's part of the ResultTask
            // TODO Refactor this out to a function that accepts a ResultStage
            val resultStage = stage.asInstanceOf[ResultStage]
            resultStage.resultOfJob match {
              case Some(job) =>
                if (!job.finished(rt.outputId)) {
                  updateAccumulators(event)
                  job.finished(rt.outputId) = true //设置该partition完成
                  job.numFinished += 1 //完成数量累加1
                  // If the whole job has finished, remove it
                  if (job.numFinished == job.numPartitions) { //说明全部partition都完成了
                    markStageAsFinished(resultStage) //标记该阶段完成
                    cleanupStateForJobAndIndependentStages(job)
                    listenerBus.post(
                      SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobSucceeded))
                  }

                  // taskSucceeded runs some user code that might throw an exception. Make sure
                  // we are resilient against that.
                  try {
                    job.listener.taskSucceeded(rt.outputId, event.result)
                  } catch {
                    case e: Exception =>
                      // TODO: Perhaps we want to mark the resultStage as failed?
                      job.listener.jobFailed(new SparkDriverExecutionException(e))
                  }
                }
              case None =>
                logInfo("Ignoring result from " + rt + " because its job has finished")
            }

          case smt: ShuffleMapTask =>
            val shuffleStage = stage.asInstanceOf[ShuffleMapStage]
            updateAccumulators(event)
            val status = event.result.asInstanceOf[MapStatus]
            val execId = status.location.executorId
            logDebug("ShuffleMapTask finished on " + execId)
            if (failedEpoch.contains(execId) && smt.epoch <= failedEpoch(execId)) {
              logInfo(s"Ignoring possibly bogus $smt completion from executor $execId")
            } else {
              shuffleStage.addOutputLoc(smt.partitionId, status)
            }

            if (runningStages.contains(shuffleStage) && shuffleStage.pendingTasks.isEmpty) {
              markStageAsFinished(shuffleStage)
              logInfo("looking for newly runnable stages")
              logInfo("running: " + runningStages)
              logInfo("waiting: " + waitingStages)
              logInfo("failed: " + failedStages)

              // We supply true to increment the epoch number here in case this is a
              // recomputation of the map outputs. In that case, some nodes may have cached
              // locations with holes (from when we detected the error) and will need the
              // epoch incremented to refetch them.
              // TODO: Only increment the epoch number if this is not the first time
              //       we registered these map outputs.
              mapOutputTracker.registerMapOutputs(
                shuffleStage.shuffleDep.shuffleId,
                shuffleStage.outputLocs.map(list => if (list.isEmpty) null else list.head),
                changeEpoch = true)

              clearCacheLocs()
              if (shuffleStage.outputLocs.contains(Nil)) {
                // Some tasks had failed; let's resubmit this shuffleStage
                // TODO: Lower-level scheduler should also deal with this
                logInfo("Resubmitting " + shuffleStage + " (" + shuffleStage.name +
                  ") because some of its tasks had failed: " +
                  shuffleStage.outputLocs.zipWithIndex.filter(_._1.isEmpty)
                      .map(_._2).mkString(", "))
                submitStage(shuffleStage)
              } else {
                val newlyRunnable = new ArrayBuffer[Stage]
                for (shuffleStage <- waitingStages) {
                  logInfo("Missing parents for " + shuffleStage + ": " +
                    getMissingParentStages(shuffleStage))
                }
                for (shuffleStage <- waitingStages if getMissingParentStages(shuffleStage).isEmpty)
                {
                  newlyRunnable += shuffleStage
                }
                waitingStages --= newlyRunnable
                runningStages ++= newlyRunnable
                for {
                  shuffleStage <- newlyRunnable.sortBy(_.id)
                  jobId <- activeJobForStage(shuffleStage)
                } {
                  logInfo("Submitting " + shuffleStage + " (" +
                    shuffleStage.rdd + "), which is now runnable")
                  submitMissingTasks(shuffleStage, jobId)
                }
              }
            }
          }

      case Resubmitted =>
        logInfo("Resubmitted " + task + ", so marking it as still running")
        stage.pendingTasks += task

      case FetchFailed(bmAddress, shuffleId, mapId, reduceId, failureMessage) =>
        val failedStage = stageIdToStage(task.stageId)
        val mapStage = shuffleToMapStage(shuffleId)

        if (failedStage.latestInfo.attemptId != task.stageAttemptId) {
          logInfo(s"Ignoring fetch failure from $task as it's from $failedStage attempt" +
            s" ${task.stageAttemptId} and there is a more recent attempt for that stage " +
            s"(attempt ID ${failedStage.latestInfo.attemptId}) running")
        } else {

          // It is likely that we receive multiple FetchFailed for a single stage (because we have
          // multiple tasks running concurrently on different executors). In that case, it is
          // possible the fetch failure has already been handled by the scheduler.
          if (runningStages.contains(failedStage)) {
            logInfo(s"Marking $failedStage (${failedStage.name}) as failed " +
              s"due to a fetch failure from $mapStage (${mapStage.name})")
            markStageAsFinished(failedStage, Some(failureMessage))
          } else {
            logDebug(s"Received fetch failure from $task, but its from $failedStage which is no " +
              s"longer running")
          }

          if (disallowStageRetryForTest) {
            abortStage(failedStage, "Fetch failure will not retry stage due to testing config",
              None)
          } else if (failedStages.isEmpty) {
            // Don't schedule an event to resubmit failed stages if failed isn't empty, because
            // in that case the event will already have been scheduled.
            // TODO: Cancel running tasks in the stage
            logInfo(s"Resubmitting $mapStage (${mapStage.name}) and " +
              s"$failedStage (${failedStage.name}) due to fetch failure")
            messageScheduler.schedule(new Runnable {
              override def run(): Unit = eventProcessLoop.post(ResubmitFailedStages)
            }, DAGScheduler.RESUBMIT_TIMEOUT, TimeUnit.MILLISECONDS)
          }
          failedStages += failedStage
          failedStages += mapStage
          // Mark the map whose fetch failed as broken in the map stage
          if (mapId != -1) {
            mapStage.removeOutputLoc(mapId, bmAddress)
            mapOutputTracker.unregisterMapOutput(shuffleId, mapId, bmAddress)
          }

          // TODO: mark the executor as failed only if there were lots of fetch failures on it
          if (bmAddress != null) {
            handleExecutorLost(bmAddress.executorId, fetchFailed = true, Some(task.epoch))
          }
        }

      case commitDenied: TaskCommitDenied =>
        // Do nothing here, left up to the TaskScheduler to decide how to handle denied commits

      case exceptionFailure: ExceptionFailure =>
        // Do nothing here, left up to the TaskScheduler to decide how to handle user failures

      case TaskResultLost =>
        // Do nothing here; the TaskScheduler handles these failures and resubmits the task.

      case other =>
        // Unrecognized failure - also do nothing. If the task fails repeatedly, the TaskScheduler
        // will abort the job.
    }
    submitWaitingStages()
  }

  /**
   * Responds to an executor being lost. This is called inside the event loop, so it assumes it can
   * modify the scheduler's internal state. Use executorLost() to post a loss event from outside.
   *
   * We will also assume that we've lost all shuffle blocks associated with the executor if the
   * executor serves its own blocks (i.e., we're not using external shuffle) OR a FetchFailed
   * occurred, in which case we presume all shuffle data related to this executor to be lost.
   *
   * Optionally the epoch during which the failure was caught can be passed to avoid allowing
   * stray fetch failures from possibly retriggering the detection of a node as lost.
   */
  private[scheduler] def handleExecutorLost(
      execId: String,
      fetchFailed: Boolean,
      maybeEpoch: Option[Long] = None) {
    val currentEpoch = maybeEpoch.getOrElse(mapOutputTracker.getEpoch)
    if (!failedEpoch.contains(execId) || failedEpoch(execId) < currentEpoch) {
      failedEpoch(execId) = currentEpoch
      logInfo("Executor lost: %s (epoch %d)".format(execId, currentEpoch))
      blockManagerMaster.removeExecutor(execId)

      if (!env.blockManager.externalShuffleServiceEnabled || fetchFailed) {
        // TODO: This will be really slow if we keep accumulating shuffle map stages
        for ((shuffleId, stage) <- shuffleToMapStage) {
          stage.removeOutputsOnExecutor(execId)
          val locs = stage.outputLocs.map(list => if (list.isEmpty) null else list.head)
          mapOutputTracker.registerMapOutputs(shuffleId, locs, changeEpoch = true)
        }
        if (shuffleToMapStage.isEmpty) {
          mapOutputTracker.incrementEpoch()
        }
        clearCacheLocs()
      }
    } else {
      logDebug("Additional executor lost message for " + execId +
               "(epoch " + currentEpoch + ")")
    }
    submitWaitingStages()
  }

  private[scheduler] def handleExecutorAdded(execId: String, host: String) {
    // remove from failedEpoch(execId) ?
    if (failedEpoch.contains(execId)) {
      logInfo("Host added was in lost list earlier: " + host)
      failedEpoch -= execId
    }
    submitWaitingStages()
  }

  //取消一个阶段
  private[scheduler] def handleStageCancellation(stageId: Int) {
    stageIdToStage.get(stageId) match {//找到该阶段
      case Some(stage) =>
        val jobsThatUseStage: Array[Int] = stage.jobIds.toArray //该节点哪些job依赖
        jobsThatUseStage.foreach { jobId =>
          handleJobCancellation(jobId, s"because Stage $stageId was cancelled") //取消该job,因为依赖都被取消了
        }
      case None =>
        logInfo("No active jobs to kill for Stage " + stageId)
    }
    submitWaitingStages()
  }

  //取消一个job,参数是jobId和取消原因
  private[scheduler] def handleJobCancellation(jobId: Int, reason: String = "") {
    if (!jobIdToStageIds.contains(jobId)) {//说明该jobID不再进行中
      logDebug("Trying to cancel unregistered job " + jobId)//仅仅打印日志即可
    } else {//说明jobID在进行中,准备取消该job
      failJobAndIndependentStages(
        jobIdToActiveJob(jobId), "Job %d cancelled %s".format(jobId, reason))
    }
    submitWaitingStages()
  }

  /**
   * Marks a stage as finished and removes it from the list of running stages.
   * 表示该阶段已经完成,并且从运行中的阶段移除走
   *
   * 参数 完成的阶段是哪个
   *      如果阶段失败,则提示信息是什么
   */
  private def markStageAsFinished(stage: Stage, errorMessage: Option[String] = None): Unit = {
    val serviceTime = stage.latestInfo.submissionTime match {//运行时间,单位是秒
      case Some(t) => "%.03f".format((clock.getTimeMillis() - t) / 1000.0)
      case _ => "Unknown"
    }
    if (errorMessage.isEmpty) {
      logInfo("%s (%s) finished in %s s".format(stage, stage.name, serviceTime))//打印完成该阶段花费多长时间
      stage.latestInfo.completionTime = Some(clock.getTimeMillis())
    } else {
      stage.latestInfo.stageFailed(errorMessage.get) //设置失败原因,以及完成时间
      logInfo("%s (%s) failed in %s s".format(stage, stage.name, serviceTime))
    }
    outputCommitCoordinator.stageEnd(stage.id)
    listenerBus.post(SparkListenerStageCompleted(stage.latestInfo)) //发送阶段完成事件
    runningStages -= stage //移除该阶段
  }

  /**
   * Aborts all jobs depending on a particular Stage. This is called in response to a task set
   * being canceled by the TaskScheduler. Use taskSetFailed() to inject this event from outside.
   * 说明该阶段有问题,要被终止
   */
  private[scheduler] def abortStage(
      failedStage: Stage,//失败的阶段对象
      reason: String,//提示描述信息
      exception: Option[Throwable]): Unit = {//出问题时候的异常对象
    if (!stageIdToStage.contains(failedStage.id)) { //说明该阶段都没有了,则直接返回
      // Skip all the actions if the stage has been removed.
      return
    }

    //找到依赖该阶段的job集合
    val dependentJobs: Seq[ActiveJob] =
      activeJobs.filter(job => stageDependsOn(job.finalStage, failedStage)).toSeq
    failedStage.latestInfo.completionTime = Some(clock.getTimeMillis()) //设置该阶段的完成时间
    for (job <- dependentJobs) {//让所有依赖该阶段的job失败
      failJobAndIndependentStages(job, s"Job aborted due to stage failure: $reason", exception)
    }
    if (dependentJobs.isEmpty) {//打印日志,这个阶段终止可以被忽略,因为该阶段没有任何job依赖他
      logInfo("Ignoring failure of " + failedStage + " because all jobs depending on it are done")
    }
  }

  /** Fails a job and all stages that are only used by that job, and cleans up relevant state.
    * 取消一个job
    **/
  private def failJobAndIndependentStages(
      job: ActiveJob,//要取消哪个Job
      failureReason: String,//取消的原因
      exception: Option[Throwable] = None): Unit = {//什么异常导致的取消
    val error = new SparkException(failureReason, exception.getOrElse(null))//取消的原因和异常
    var ableToCancelStages = true

    val shouldInterruptThread =
      if (job.properties == null) false
      else job.properties.getProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, "false").toBoolean

    // Cancel all independent, running stages.
    val stages = jobIdToStageIds(job.jobId)//取消所有运行中的阶段
    if (stages.isEmpty) {
      logError("No stages registered for job " + job.jobId)
    }

    //循环每一个阶段
    stages.foreach { stageId =>
      val jobsForStage: Option[HashSet[Int]] = stageIdToStage.get(stageId).map(_.jobIds) //该节点被哪些job依赖
      if (jobsForStage.isEmpty || !jobsForStage.get.contains(job.jobId)) {//没有job依赖该阶段,或者该阶段没有被该jobID依赖,因此仅仅打印日志即可
        logError(
          "Job %d not registered for stage %d even though that stage was registered for the job"//说明该job没有被注册在该阶段中
            .format(job.jobId, stageId))
      } else if (jobsForStage.get.size == 1) {
        if (!stageIdToStage.contains(stageId)) {//说明系统中都没有该阶段
          logError(s"Missing Stage for stage with id $stageId")
        } else {
          // This is the only job that uses this stage, so fail the stage if it is running.
          //说明该阶段只有一个job在用.因此如果该阶段在运行中,则完全可以将其kill掉
          val stage = stageIdToStage(stageId)
          if (runningStages.contains(stage)) {//说明该阶段正在运行中
            try { // cancelTasks will fail if a SchedulerBackend does not implement killTask
              taskScheduler.cancelTasks(stageId, shouldInterruptThread)
              markStageAsFinished(stage, Some(failureReason)) //标识该阶段已经完成
            } catch {
              case e: UnsupportedOperationException =>
                logInfo(s"Could not cancel tasks for stage $stageId", e)
              ableToCancelStages = false
            }
          }
        }
      }
    }

    if (ableToCancelStages) {
      job.listener.jobFailed(error)
      cleanupStateForJobAndIndependentStages(job)
      listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobFailed(error)))
    }
  }

  /** Return true if one of stage's ancestors is target.
    * 判断是否stage阶段依赖了target阶段
    **/
  private def stageDependsOn(stage: Stage, target: Stage): Boolean = {
    if (stage == target) {//如果阶段相同,则一定是依赖了
      return true
    }

    //循环stage依赖的所有阶段,查看是否有依赖target阶段的
    val visitedRdds = new HashSet[RDD[_]]//已经访问过的RDD
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]//等待去检查的RDD集合
    def visit(rdd: RDD[_]) {
      if (!visitedRdds(rdd)) {
        visitedRdds += rdd
        for (dep <- rdd.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              val mapStage = getShuffleMapStage(shufDep, stage.firstJobId)
              if (!mapStage.isAvailable) {
                waitingForVisit.push(mapStage.rdd)
              }  // Otherwise there's no need to follow the dependency back
            case narrowDep: NarrowDependency[_] =>
              waitingForVisit.push(narrowDep.rdd)
          }
        }
      }
    }
    waitingForVisit.push(stage.rdd)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }

    //判断是否有需要相同的RDD的,有则认为有依赖
    visitedRdds.contains(target.rdd)
  }

  /**
   * Gets the locality information associated with a partition of a particular RDD.
   *
   * This method is thread-safe and is called from both DAGScheduler and SparkContext.
   *
   * @param rdd whose partitions are to be looked at
   * @param partition to lookup locality information for
   * @return list of machines that are preferred by the partition
   */
  private[spark]
  def getPreferredLocs(rdd: RDD[_], partition: Int): Seq[TaskLocation] = {
    getPreferredLocsInternal(rdd, partition, new HashSet)
  }

  /**
   * Recursive implementation for getPreferredLocs.
   *
   * This method is thread-safe because it only accesses DAGScheduler state through thread-safe
   * methods (getCacheLocs()); please be careful when modifying this method, because any new
   * DAGScheduler state accessed by it may require additional synchronization.
   * 找到一个RDD的某一个partition在哪台节点上执行为好
   */
  private def getPreferredLocsInternal(
      rdd: RDD[_],
      partition: Int,
      visited: HashSet[(RDD[_], Int)]): Seq[TaskLocation] = {
    // If the partition has already been visited, no need to re-visit.
    // This avoids exponential path exploration.  SPARK-695
    if (!visited.add((rdd, partition))) {//说明已经存在在集合里面了
      // Nil has already been returned for previously visited partitions.
      return Nil
    }
    // If the partition is cached, return the cache locations 返回缓存的数据
    val cached = getCacheLocs(rdd)(partition)
    if (cached.nonEmpty) {//说明缓存命中
      return cached
    }
    // If the RDD has some placement preferences (as is the case for input RDDs), get those
    //获取该partition对应的文件在哪些路径上可以获取
    val rddPrefs = rdd.preferredLocations(rdd.partitions(partition)).toList
    if (rddPrefs.nonEmpty) {
      return rddPrefs.map(TaskLocation(_))
    }

    // If the RDD has narrow dependencies, pick the first partition of the first narrow dependency
    // that has any placement preferences. Ideally we would choose based on transfer sizes,
    // but this will do for now.
    //该rdd不是一个实体rdd,而是由父RDD转换过来的
    rdd.dependencies.foreach {
      case n: NarrowDependency[_] =>
        for (inPart <- n.getParents(partition)) {//返回属于该partition对应的父RDD的partition集合,即可能父RDD多个partition对应现在的RDD一个partition
          val locs = getPreferredLocsInternal(n.rdd, inPart, visited) //递归查看最终的实体RDD对应的磁盘路径
          if (locs != Nil) {//虽然该RDD对应多个父RDD,但是找到一个父RDD所在节点就可以了
            return locs
          }
        }
      case _ =>
    }

    //说明父RDD或者本身RDD都没有实体路径
    // If the RDD has shuffle dependencies and shuffle locality is enabled, pick locations that
    // have at least REDUCER_PREF_LOCS_FRACTION of data as preferred locations
    if (shuffleLocalityEnabled && rdd.partitions.length < SHUFFLE_PREF_REDUCE_THRESHOLD) {
      rdd.dependencies.foreach {
        case s: ShuffleDependency[_, _, _] =>
          if (s.rdd.partitions.length < SHUFFLE_PREF_MAP_THRESHOLD) {
            // Get the preferred map output locations for this reducer
            val topLocsForReducer = mapOutputTracker.getLocationsWithLargestOutputs(s.shuffleId,
              partition, rdd.partitions.length, REDUCER_PREF_LOCS_FRACTION)
            if (topLocsForReducer.nonEmpty) {
              return topLocsForReducer.get.map(loc => TaskLocation(loc.host, loc.executorId))
            }
          }
        case _ =>
      }
    }
    Nil
  }

  def stop() {
    logInfo("Stopping DAGScheduler")
    messageScheduler.shutdownNow()
    eventProcessLoop.stop()
    taskScheduler.stop()
  }

  // Start the event thread and register the metrics source at the end of the constructor
  env.metricsSystem.registerSource(metricsSource)
  eventProcessLoop.start()
}

private[scheduler] class DAGSchedulerEventProcessLoop(dagScheduler: DAGScheduler)
  extends EventLoop[DAGSchedulerEvent]("dag-scheduler-event-loop") with Logging {

  private[this] val timer = dagScheduler.metricsSource.messageProcessingTimer

  /**
   * The main event loop of the DAG scheduler.
   * 主要入口,处理每一个事件
   */
  override def onReceive(event: DAGSchedulerEvent): Unit = {
    val timerContext = timer.time()
    try {
      doOnReceive(event)
    } finally {
      timerContext.stop()
    }
  }

  private def doOnReceive(event: DAGSchedulerEvent): Unit = event match {
    case JobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties) =>
      dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties) //提交一个job

    case StageCancelled(stageId) =>
      dagScheduler.handleStageCancellation(stageId) //取消一个阶段,属于该阶段的job都会被取消

    case JobCancelled(jobId) =>
      dagScheduler.handleJobCancellation(jobId) //取消一个job

    case JobGroupCancelled(groupId) =>
      dagScheduler.handleJobGroupCancelled(groupId) //取消属于该组的所有job

    case AllJobsCancelled =>
      dagScheduler.doCancelAllJobs() //取消所有的job

    case ExecutorAdded(execId, host) =>
      dagScheduler.handleExecutorAdded(execId, host)

    case ExecutorLost(execId) =>
      dagScheduler.handleExecutorLost(execId, fetchFailed = false)

    case BeginEvent(task, taskInfo) =>
      dagScheduler.handleBeginEvent(task, taskInfo)

    case GettingResultEvent(taskInfo) =>
      dagScheduler.handleGetTaskResult(taskInfo)

    case completion @ CompletionEvent(task, reason, _, _, taskInfo, taskMetrics) =>
      dagScheduler.handleTaskCompletion(completion)

    case TaskSetFailed(taskSet, reason, exception) =>
      dagScheduler.handleTaskSetFailed(taskSet, reason, exception)

    case ResubmitFailedStages =>
      dagScheduler.resubmitFailedStages()
  }

  //调度整体运行失败,则停止所有的job任务
  override def onError(e: Throwable): Unit = {
    logError("DAGSchedulerEventProcessLoop failed; shutting down SparkContext", e)
    try {
      dagScheduler.doCancelAllJobs()
    } catch {
      case t: Throwable => logError("DAGScheduler failed to cancel all jobs.", t)
    }
    dagScheduler.sc.stop() //将上下文也关闭掉
  }

  override def onStop(): Unit = {
    // Cancel any active jobs in postStop hook
    dagScheduler.cleanUpAfterSchedulerStop()
  }
}

private[spark] object DAGScheduler {
  // The time, in millis, to wait for fetch failure events to stop coming in after one is detected;
  // this is a simplistic way to avoid resubmitting tasks in the non-fetchable map stage one by one
  // as more failure events come in
  val RESUBMIT_TIMEOUT = 200
}
