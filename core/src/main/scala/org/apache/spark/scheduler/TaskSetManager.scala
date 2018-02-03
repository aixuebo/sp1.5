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
import java.nio.ByteBuffer
import java.util.Arrays
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.math.{min, max}
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.SchedulingMode._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.util.{Clock, SystemClock, Utils}

/**
 * Schedules the tasks within a single TaskSet in the TaskSchedulerImpl. This class keeps track of
 * each task, retries tasks if they fail (up to a limited number of times), and
 * handles locality-aware scheduling for this TaskSet via delay scheduling. The main interfaces
 * to it are resourceOffer, which asks the TaskSet whether it wants to run a task on one node,
 * and statusUpdate, which tells it that one of its tasks changed state (e.g. finished).
 *
 * THREADING: This class is designed to only be called from code with a lock on the
 * TaskScheduler (e.g. its event handlers). It should not be called from other threads.
 *
 * @param sched           the TaskSchedulerImpl associated with the TaskSetManager
 * @param taskSet         the TaskSet to manage scheduling for
 * @param maxTaskFailures if any particular task fails this number of times, the entire
 *                        task set will be aborted
 * 该类表示调度器要执行一个阶段的任务集合
 *
调度器队列的具体实现类Pool
该类表示的就是一个具体的调度器队列的非叶子节点,即承担了队列的容器工作

调度器队列的具体实现类TaskSetManager
主要承担了叶子节点的功能
 */
private[spark] class TaskSetManager(
    sched: TaskSchedulerImpl,//该任务的调度器对象
    val taskSet: TaskSet,//任务集合
    val maxTaskFailures: Int,//任务最多失败次数
    clock: Clock = new SystemClock())
  extends Schedulable with Logging {

  //调度器的conf对象
  val conf = sched.sc.conf

  /*
   * Sometimes if an executor is dead or in an otherwise invalid state, the driver
   * does not realize right away leading to repeated task failures. If enabled,
   * this temporarily prevents a task from re-launching on an executor where
   * it just failed.
   * 任务id在executor节点一旦失败后,只有超过该时间才运行再次尝试在该executor节点运行任务
   */
  private val EXECUTOR_TASK_BLACKLIST_TIMEOUT =
    conf.getLong("spark.scheduler.executorTaskBlacklistTime", 0L)

  // Quantile of tasks at which to start speculation
  val SPECULATION_QUANTILE = conf.getDouble("spark.speculation.quantile", 0.75) //计算最小完成数量(达到该数量,才能启动推测执行)
  val SPECULATION_MULTIPLIER = conf.getDouble("spark.speculation.multiplier", 1.5) //运行时间超过多少平均值的倍数,才可以对其进行推测执行

  // Limit of bytes for total size of results (default is 1GB) 默认1G,表示最大的输出结果字节数
  val maxResultSize = Utils.getMaxResultSize(conf)

  // Serializer for closures and tasks.
  val env = SparkEnv.get
  val ser = env.closureSerializer.newInstance()//序列化对象

  val tasks = taskSet.tasks//任务集合
  val numTasks = tasks.length//任务数量

  val copiesRunning = new Array[Int](numTasks) //每一个任务一个元素,元素内容是该任务运行了多少次---表示的是正在运行中的
  val successful = new Array[Boolean](numTasks) //每一个任务一个元素,元素内容是该任务运行是否成功
  private val numFailures = new Array[Int](numTasks) //失败的任务数量

  // key is taskId, value is a Map of executor id to when it failed
  //任务在哪些executor上是总失败的,key是任务id,value中key是executor,value是失败时间
  private val failedExecutors = new HashMap[Int, HashMap[String, Long]]()

  val taskAttempts = Array.fill[List[TaskInfo]](numTasks)(Nil) //每一个任务对应一个List[TaskInfo],List元素是每一个尝试任务对象
  var tasksSuccessful = 0 //成功完成的任务数量

  var weight = 1
  var minShare = 0
  var priority = taskSet.priority //优先级
  var stageId = taskSet.stageId

  var name = "TaskSet_" + taskSet.stageId.toString//调度名称
  var parent: Pool = null //父调度器

  var totalResultSize = 0L
  var calculatedTasks = 0

  val runningTasksSet = new HashSet[Long]

  override def runningTasks: Int = runningTasksSet.size

  // True once no more tasks should be launched for this task set manager. TaskSetManagers enter
  // the zombie state once at least one attempt of each task has completed successfully, or if the
  // task set is aborted (for example, because it was killed).  TaskSetManagers remain in the zombie
  // state until all tasks have finished running; we keep TaskSetManagers that are in the zombie
  // state in order to continue to track and account for the running tasks.
  // TODO: We should kill any running task attempts when the task set manager becomes a zombie.
  //僵尸 如果任务设置isZombie,我们也不需要执行推测任务
  var isZombie = false //true表示阶段的所有任务结束了

  // Set of pending tasks for each executor. These collections are actually
  // treated as stacks, in which new tasks are added to the end of the
  // ArrayBuffer and removed from the end.
  //
  // This makes it faster to detect
  // tasks that repeatedly fail because whenever a task failed, it is put
  // back at the head of the stack. They are also only cleaned up lazily;
  // when a task is launched, it remains in all the pending lists except
  // the one that it was launched from, but gets removed from them later.
  /**
   * 等待的任务集合,每一个executor一个小队列
   * 这个集合是真正意义上的栈数据结构,任务被添加到队列的尾部,从尾部最先pop弹出。
   *
   * key是executor,value是存储任务ID的集合
   */
  private val pendingTasksForExecutor = new HashMap[String, ArrayBuffer[Int]]

  // Set of pending tasks for each host. Similar to pendingTasksForExecutor,
  // but at host level.
  //每一个host一个小队列,也是内部实现栈数据结构
  private val pendingTasksForHost = new HashMap[String, ArrayBuffer[Int]]

  // Set of pending tasks for each rack -- similar to the above.
  //每一个rack一个小队列
  private val pendingTasksForRack = new HashMap[String, ArrayBuffer[Int]]

  // Set containing pending tasks with no locality preferences.
  //没有推荐位置信息的队列
  var pendingTasksWithNoPrefs = new ArrayBuffer[Int]

  // Set containing all pending tasks (also used as a stack, as above).
  //所有等待的任务集合
  val allPendingTasks = new ArrayBuffer[Int]

  // Tasks that can be speculated. Since these will be a small fraction of total
  // tasks, we'll just hold them in a HashSet.
  //正在推测执行的任务集合
  val speculatableTasks = new HashSet[Int]

  // Task index, start and finish time for each task attempt (indexed by task ID)
  //每一个taskId对应一个描述该task的内容的对象
  val taskInfos = new HashMap[Long, TaskInfo]

  // How frequently to reprint duplicate exceptions in full, in milliseconds
  val EXCEPTION_PRINT_INTERVAL =
    conf.getLong("spark.logging.exceptionPrintInterval", 10000)

  // Map of recent exceptions (identified by string representation and top stack frame) to
  // duplicate count (how many times the same exception has appeared) and time the full exception
  // was printed. This should ideally be an LRU map that can drop old exceptions automatically.
  val recentExceptions = HashMap[String, (Int, Long)]()

  // Figure out the current map output tracker epoch and set it on all tasks
  //获取当前输出的跟踪序号,并且给所有的任务分配相同的
  val epoch = sched.mapOutputTracker.getEpoch
  logDebug("Epoch for " + taskSet + ": " + epoch)
  for (t <- tasks) {
    t.epoch = epoch
  }

  // Add all our tasks to the pending lists. We do this in reverse order
  // of task index so that tasks with low indices get launched first.
  for (i <- (0 until numTasks).reverse) {//倒着顺序循环
    addPendingTask(i) //添加一个任务,到若干个队列去
  }

  // Figure out which locality levels we have in our TaskSet, so we can do delay scheduling
  var myLocalityLevels = computeValidLocalityLevels() //找到任务可以分配的级别
  var localityWaits = myLocalityLevels.map(getLocalityWait) // Time to wait at each level //找到每一个级别对应的等待时间

  // Delay scheduling variables: we keep track of our current locality level and the time we
  // last launched a task at that level, and move up a level when localityWaits[curLevel] expires.
  // We then move down if we manage to launch a "more local" task.
  var currentLocalityIndex = 0    // Index of our current locality level in validLocalityLevels
  var lastLaunchTime = clock.getTimeMillis()  // Time we last launched a task at this level 最新一个任务的启动时间

  override def schedulableQueue: ConcurrentLinkedQueue[Schedulable] = null

  override def schedulingMode: SchedulingMode = SchedulingMode.NONE

  var emittedTaskSizeWarning = false

  /**
   * Add a task to all the pending-task lists that it should be on. If readding is set, we are
   * re-adding the task so only include it in each list if it's not already there.
   *
   * 参数readding true表示只读的,不能修改,false表示可以修改
   *
   * 添加一个任务,到若干个队列去
   */
  private def addPendingTask(index: Int, readding: Boolean = false) {//readding=true表示只读
    // Utility method that adds `index` to a list only if readding=false or it's not already there
    //将任务的id添加到该队列中
    def addTo(list: ArrayBuffer[Int]) {
      if (!readding || !list.contains(index)) {//可以修改,或者list不包含index位置元素,则设置该位置元素
        list += index
      }
    }

    for (loc <- tasks(index).preferredLocations) {//循环该task的推荐运行节点
      //添加executor级别的任务,即该executor上可以优先执行哪些task的分区---因为数据块可能离这些execuor较近
      loc match {
        case e: ExecutorCacheTaskLocation =>
          addTo(pendingTasksForExecutor.getOrElseUpdate(e.executorId, new ArrayBuffer)) //说明该executorId可以执行该任务id
        case e: HDFSCacheTaskLocation => {
          val exe = sched.getExecutorsAliveOnHost(loc.host) //获取该host上有哪些executor集合
          exe match {
            case Some(set) => {
              for (e <- set) {//循环每一个executor,他们都在同一个host上
                addTo(pendingTasksForExecutor.getOrElseUpdate(e, new ArrayBuffer)) //说明该序号的任务可以分配给该host上所有的executor执行
              }
              logInfo(s"Pending task $index has a cached location at ${e.host} " +
                ", where there are executors " + set.mkString(","))
            }
            case None => logDebug(s"Pending task $index has a cached location at ${e.host} " +
                ", but there are no executors alive there.")//说明等候的任务在host上,但是该host上没有活着的executor
          }
        }
        case _ => Unit
      }
      addTo(pendingTasksForHost.getOrElseUpdate(loc.host, new ArrayBuffer)) //说明该分区可以在该host上运行
      for (rack <- sched.getRackForHost(loc.host)) { //找到该host所在的rack
        addTo(pendingTasksForRack.getOrElseUpdate(rack, new ArrayBuffer)) //说明该rack上可以运行哪些任务分区
      }
    }

    if (tasks(index).preferredLocations == Nil) {//说明该任务没有推荐的地方
      addTo(pendingTasksWithNoPrefs)
    }

    if (!readding) {//非只读的
      allPendingTasks += index  // No point scanning this whole list to find the old task there
    }
  }

  /**
   * Return the pending tasks list for a given executor ID, or an empty list if
   * there is no map entry for that host
   * 返回等候的任务中可以在该executor上推荐执行的分区ID集合
   */
  private def getPendingTasksForExecutor(executorId: String): ArrayBuffer[Int] = {
    pendingTasksForExecutor.getOrElse(executorId, ArrayBuffer())
  }

  /**
   * Return the pending tasks list for a given host, or an empty list if
   * there is no map entry for that host
   */
  private def getPendingTasksForHost(host: String): ArrayBuffer[Int] = {
    pendingTasksForHost.getOrElse(host, ArrayBuffer())
  }

  /**
   * Return the pending rack-local task list for a given rack, or an empty list if
   * there is no map entry for that rack
   */
  private def getPendingTasksForRack(rack: String): ArrayBuffer[Int] = {
    pendingTasksForRack.getOrElse(rack, ArrayBuffer())
  }

  /**
   * Dequeue a pending task from the given list and return its index.
   * Return None if the list is empty.
   * This method also cleans up any tasks in the list that have already
   * been launched, since we want that to happen lazily.
   * 参数是要在哪个executor上执行任务,以及推荐在该executor上执行哪些分区任务
   *
   * 返回值是找到了任务ID
   */
  private def dequeueTaskFromList(execId: String, list: ArrayBuffer[Int]): Option[Int] = {
    var indexOffset = list.size
    while (indexOffset > 0) {
      indexOffset -= 1
      val index = list(indexOffset) //获取第几个分区任务
      if (!executorIsBlacklisted(execId, index)) {//说明该分区在executor上不是黑名单
        // This should almost always be list.trimEnd(1) to remove tail
        list.remove(indexOffset)
        if (copiesRunning(index) == 0 && !successful(index)) {
          return Some(index)
        }
      }
    }
    None
  }

  /** Check whether a task is currently running an attempt on a given host */
  private def hasAttemptOnHost(taskIndex: Int, host: String): Boolean = {
    taskAttempts(taskIndex).exists(_.host == host)
  }

  /**
   * Is this re-execution of a failed task on an executor it already failed in before
   * EXECUTOR_TASK_BLACKLIST_TIMEOUT has elapsed ?
   * 说明是否该任务在该executor上是黑名单不能执行
   *
   * true表示不可以继续执行
   */
  private def executorIsBlacklisted(execId: String, taskId: Int): Boolean = {
    if (failedExecutors.contains(taskId)) {//确定该任务确实有失败的记录
      val failed = failedExecutors.get(taskId).get //找到该任务在哪些executor上失败过

      return failed.contains(execId) && //确定在executor上失败过
        clock.getTimeMillis() - failed.get(execId).get < EXECUTOR_TASK_BLACKLIST_TIMEOUT //还尚未过超时时间,因此是不允许在运行的
    }

    false
  }

  /**
   * Return a speculative task for a given executor if any are available. The task should not have
   * an attempt running on this host, in case the host is slow. In addition, the task should meet
   * the given locality constraint.
   * 如果所有的任务都调度完成了,则查找推测任务
   */
  // Labeled as protected to allow tests to override providing speculative tasks if necessary
  protected def dequeueSpeculativeTask(execId: String, host: String, locality: TaskLocality.Value)
    : Option[(Int, TaskLocality.Value)] =
  {
    speculatableTasks.retain(index => !successful(index)) // Remove finished tasks from set

    def canRunOnHost(index: Int): Boolean =
      !hasAttemptOnHost(index, host) && !executorIsBlacklisted(execId, index)

    if (!speculatableTasks.isEmpty) {
      // Check for process-local tasks; note that tasks can be process-local
      // on multiple nodes when we replicate cached blocks, as in Spark Streaming
      for (index <- speculatableTasks if canRunOnHost(index)) {
        val prefs = tasks(index).preferredLocations
        val executors = prefs.flatMap(_ match {
          case e: ExecutorCacheTaskLocation => Some(e.executorId)
          case _ => None
        });
        if (executors.contains(execId)) {
          speculatableTasks -= index
          return Some((index, TaskLocality.PROCESS_LOCAL))
        }
      }

      // Check for node-local tasks
      if (TaskLocality.isAllowed(locality, TaskLocality.NODE_LOCAL)) {
        for (index <- speculatableTasks if canRunOnHost(index)) {
          val locations = tasks(index).preferredLocations.map(_.host)
          if (locations.contains(host)) {
            speculatableTasks -= index
            return Some((index, TaskLocality.NODE_LOCAL))
          }
        }
      }

      // Check for no-preference tasks
      if (TaskLocality.isAllowed(locality, TaskLocality.NO_PREF)) {
        for (index <- speculatableTasks if canRunOnHost(index)) {
          val locations = tasks(index).preferredLocations
          if (locations.size == 0) {
            speculatableTasks -= index
            return Some((index, TaskLocality.PROCESS_LOCAL))
          }
        }
      }

      // Check for rack-local tasks
      if (TaskLocality.isAllowed(locality, TaskLocality.RACK_LOCAL)) {
        for (rack <- sched.getRackForHost(host)) {
          for (index <- speculatableTasks if canRunOnHost(index)) {
            val racks = tasks(index).preferredLocations.map(_.host).map(sched.getRackForHost)
            if (racks.contains(rack)) {
              speculatableTasks -= index
              return Some((index, TaskLocality.RACK_LOCAL))
            }
          }
        }
      }

      // Check for non-local tasks
      if (TaskLocality.isAllowed(locality, TaskLocality.ANY)) {
        for (index <- speculatableTasks if canRunOnHost(index)) {
          speculatableTasks -= index
          return Some((index, TaskLocality.ANY))
        }
      }
    }

    None
  }

  /**
   * Dequeue a pending task for a given node and return its index and locality level.
   * Only search for tasks matching the given locality constraint.
   *
   * @return An option containing (task index within the task set, locality, is speculative?)
   * 为该executor分配任务
   */
  private def dequeueTask(execId: String, host: String, maxLocality: TaskLocality.Value)
    : Option[(Int, TaskLocality.Value, Boolean)] = //返回值是任务ID,此时任务执行的级别,是否是推测执行
  {
    for (index <- dequeueTaskFromList(execId, getPendingTasksForExecutor(execId))) { //首先找到该executor上建议执行哪些分区任务
      return Some((index, TaskLocality.PROCESS_LOCAL, false))
    }

    if (TaskLocality.isAllowed(maxLocality, TaskLocality.NODE_LOCAL)) {
      for (index <- dequeueTaskFromList(execId, getPendingTasksForHost(host))) {
        return Some((index, TaskLocality.NODE_LOCAL, false))
      }
    }

    if (TaskLocality.isAllowed(maxLocality, TaskLocality.NO_PREF)) {
      // Look for noPref tasks after NODE_LOCAL for minimize cross-rack traffic
      for (index <- dequeueTaskFromList(execId, pendingTasksWithNoPrefs)) {
        return Some((index, TaskLocality.PROCESS_LOCAL, false))
      }
    }

    if (TaskLocality.isAllowed(maxLocality, TaskLocality.RACK_LOCAL)) {
      for {
        rack <- sched.getRackForHost(host) //找到host所在的rack
        index <- dequeueTaskFromList(execId, getPendingTasksForRack(rack))
      } {
        return Some((index, TaskLocality.RACK_LOCAL, false))
      }
    }

    if (TaskLocality.isAllowed(maxLocality, TaskLocality.ANY)) {
      for (index <- dequeueTaskFromList(execId, allPendingTasks)) {
        return Some((index, TaskLocality.ANY, false))
      }
    }

    // find a speculative task if all others tasks have been scheduled 如果所有的任务都调度完成了,则查找推测任务
    dequeueSpeculativeTask(execId, host, maxLocality).map {
      case (taskIndex, allowedLocality) => (taskIndex, allowedLocality, true)}
  }

  /**
   * Respond to an offer of a single executor from the scheduler by finding a task
   *
   * NOTE: this function is either called with a maxLocality which
   * would be adjusted by delay scheduling algorithm or it will be with a special
   * NO_PREF locality which will be not modified
   *
   * @param execId the executor Id of the offered resource
   * @param host  the host Id of the offered resource
   * @param maxLocality the maximum locality we want to schedule the tasks at
   * 为该executor分配一个task任务
   */
  @throws[TaskNotSerializableException]
  def resourceOffer(
      execId: String,
      host: String,
      maxLocality: TaskLocality.TaskLocality)
    : Option[TaskDescription] =
  {
    if (!isZombie) {
      val curTime = clock.getTimeMillis()

      var allowedLocality = maxLocality

      if (maxLocality != TaskLocality.NO_PREF) {//说明用户分配了优先在哪个节点上运行
        allowedLocality = getAllowedLocalityLevel(curTime) //获取此时应该从哪个节点先执行任务
        if (allowedLocality > maxLocality) {
          // We're not allowed to search for farther-away tasks
          allowedLocality = maxLocality //此时不允许超过最大级别范围
        }
      }

      dequeueTask(execId, host, allowedLocality) match {
        case Some((index, taskLocality, speculative)) => { //说明为该execitor匹配到了一个任务
          // Found a task; do some bookkeeping and return a task description
          val task = tasks(index) //任务对象
          val taskId = sched.newTaskId() //分配任务ID
          // Do various bookkeeping
          copiesRunning(index) += 1 //追加该任务被运行的次数
          val attemptNum = taskAttempts(index).size //有多少个尝试任务了
          val info = new TaskInfo(taskId, index, attemptNum, curTime,execId,
              host, taskLocality, speculative)//创建task的描述信息对象
            
          taskInfos(taskId) = info
          taskAttempts(index) = info :: taskAttempts(index)
          // Update our locality level for delay scheduling
          // NO_PREF will not affect the variables related to delay scheduling
          if (maxLocality != TaskLocality.NO_PREF) {
            currentLocalityIndex = getLocalityIndex(taskLocality)
            lastLaunchTime = curTime
          }
          // Serialize and return the task
          val startTime = clock.getTimeMillis()
          val serializedTask: ByteBuffer = try {
            Task.serializeWithDependencies(task, sched.sc.addedFiles, sched.sc.addedJars, ser) //将task任务以及依赖的文件和jar包序列化
          } catch {
            // If the task cannot be serialized, then there's no point to re-attempt the task,
            // as it will always fail. So just abort the whole task-set.
            case NonFatal(e) =>
              val msg = s"Failed to serialize task $taskId, not attempting to retry it."
              logError(msg, e)
              abort(s"$msg Exception during serialization: $e")
              throw new TaskNotSerializableException(e)
          }
          if (serializedTask.limit > TaskSetManager.TASK_SIZE_TO_WARN_KB * 1024 &&
              !emittedTaskSizeWarning) {//说明发送的内容过大
            emittedTaskSizeWarning = true
            logWarning(s"Stage ${task.stageId} contains a task of very large size " +
              s"(${serializedTask.limit / 1024} KB). The maximum recommended task size is " +
              s"${TaskSetManager.TASK_SIZE_TO_WARN_KB} KB.")
          }
          addRunningTask(taskId) //描述该队列已经增加了一个任务了

          // We used to log the time it takes to serialize the task, but task size is already
          // a good proxy to task serialization time.
          // val timeTaken = clock.getTime() - startTime
          val taskName = s"task ${info.id} in stage ${taskSet.id}"
          logInfo("Starting %s (TID %d, %s, %s, %d bytes)".format(
              taskName, taskId, host, taskLocality, serializedTask.limit))

          sched.dagScheduler.taskStarted(task, info) //通知调度器
          return Some(new TaskDescription(taskId = taskId, attemptNumber = attemptNum, execId,
            taskName, index, serializedTask)) //返回该任务的描述
        }
        case _ =>
      }
    }
    None
  }

  private def maybeFinishTaskSet() {
    if (isZombie && runningTasks == 0) {
      sched.taskSetFinished(this)
    }
  }

  /**
   * Get the level we can launch tasks according to delay scheduling, based on current wait time.
   * 获取从什么级别开始执行任务
   */
  private def getAllowedLocalityLevel(curTime: Long): TaskLocality.TaskLocality = {
    // Remove the scheduled or finished tasks lazily
    def tasksNeedToBeScheduledFrom(pendingTaskIds: ArrayBuffer[Int]): Boolean = {//参数是分区的任务id集合
      var indexOffset = pendingTaskIds.size
      while (indexOffset > 0) {
        indexOffset -= 1
        val index = pendingTaskIds(indexOffset)
        if (copiesRunning(index) == 0 && !successful(index)) {//说明任务还存在,还没有完成,则返回true
          return true
        } else {
          pendingTaskIds.remove(indexOffset)//说明任务完成了,因此从等待集合中删除---此时删除的是任务,即任务完成了
        }
      }
      false //返回false说明这些任务都已经完成了
    }
    // Walk through the list of tasks that can be scheduled at each location and returns true
    // if there are any tasks that still need to be scheduled. Lazily cleans up tasks that have
    // already been scheduled.
    //参数是某一个级别下应该要执行哪些分区任务,以及该任务在哪些executor、host、rack等上面执行
    def moreTasksToRunIn(pendingTasks: HashMap[String, ArrayBuffer[Int]]): Boolean = {
      val emptyKeys = new ArrayBuffer[String] //存储的是executor、host、rack之一的内容,说明该key上面的任务都执行完了
      val hasTasks = pendingTasks.exists {//过滤
        case (id: String, tasks: ArrayBuffer[Int]) => //id是executor、host、rack之一的内容,value是id上面要运行的任务的id
          if (tasksNeedToBeScheduledFrom(tasks)) {//true返回说明任务可以在上面去分配
            true
          } else {//说明任务都已经完成了
            emptyKeys += id
            false
          }
      }
      // The key could be executorId, host or rackId
      emptyKeys.foreach(id => pendingTasks.remove(id)) //因为任务都执行完成了,因此可以删除掉了,即此时删除的是key,即host上没有任何任务了
      hasTasks
    }

    while (currentLocalityIndex < myLocalityLevels.length - 1) {
      val moreTasks = myLocalityLevels(currentLocalityIndex) match {
        case TaskLocality.PROCESS_LOCAL => moreTasksToRunIn(pendingTasksForExecutor)
        case TaskLocality.NODE_LOCAL => moreTasksToRunIn(pendingTasksForHost)
        case TaskLocality.NO_PREF => pendingTasksWithNoPrefs.nonEmpty
        case TaskLocality.RACK_LOCAL => moreTasksToRunIn(pendingTasksForRack)
      }
      if (!moreTasks) {//说明没找到属于该级别的任务
        // This is a performance optimization: if there are no more tasks that can
        // be scheduled at a particular locality level, there is no point in waiting
        // for the locality wait timeout (SPARK-4939).
        //这个是一个优化,说明没有任务在该级别存在,则不需要等待超时,我们就切换下一个级别去查找任务
        lastLaunchTime = curTime
        logDebug(s"No tasks for locality level ${myLocalityLevels(currentLocalityIndex)}, " +
          s"so moving to locality level ${myLocalityLevels(currentLocalityIndex + 1)}")
        currentLocalityIndex += 1
      } else if (curTime - lastLaunchTime >= localityWaits(currentLocalityIndex)) { //说明超时了,跳跃到下一个级别去
        // Jump to the next locality level, and reset lastLaunchTime so that the next locality
        // wait timer doesn't immediately expire
        lastLaunchTime += localityWaits(currentLocalityIndex)
        currentLocalityIndex += 1
        logDebug(s"Moving to ${myLocalityLevels(currentLocalityIndex)} after waiting for " +
          s"${localityWaits(currentLocalityIndex)}ms") //说明本级别超时了
      } else {//说明该级别有任务
        return myLocalityLevels(currentLocalityIndex)
      }
    }
    myLocalityLevels(currentLocalityIndex)
  }

  /**
   * Find the index in myLocalityLevels for a given locality. This is also designed to work with
   * localities that are not in myLocalityLevels (in case we somehow get those) by returning the
   * next-biggest level we have. Uses the fact that the last value in myLocalityLevels is ANY.
   */
  def getLocalityIndex(locality: TaskLocality.TaskLocality): Int = {
    var index = 0
    while (locality > myLocalityLevels(index)) {
      index += 1
    }
    index
  }

  /**
   * Marks the task as getting result and notifies the DAG Scheduler
   */
  def handleTaskGettingResult(tid: Long): Unit = {
    val info = taskInfos(tid)
    info.markGettingResult()
    sched.dagScheduler.taskGettingResult(info)
  }

  /**
   * Check whether has enough quota to fetch the result with `size` bytes
   * true表示总结果大小+参数后还依然是合理范围内
   */
  def canFetchMoreResults(size: Long): Boolean = sched.synchronized {
    totalResultSize += size
    calculatedTasks += 1
    if (maxResultSize > 0 && totalResultSize > maxResultSize) {//超出最大结果字节范围,则抛异常
      val msg = s"Total size of serialized results of ${calculatedTasks} tasks " +
        s"(${Utils.bytesToString(totalResultSize)}) is bigger than spark.driver.maxResultSize " +
        s"(${Utils.bytesToString(maxResultSize)})"
      logError(msg)
      abort(msg)
      false
    } else {
      true
    }
  }

  /**
   * Marks the task as successful and notifies the DAGScheduler that a task has ended.
   * 标志任务成功完
   */
  def handleSuccessfulTask(tid: Long, result: DirectTaskResult[_]): Unit = {
    val info = taskInfos(tid)
    val index = info.index
    info.markSuccessful() //表示该任务已经完成
    removeRunningTask(tid)
    // This method is called by "TaskSchedulerImpl.handleSuccessfulTask" which holds the
    // "TaskSchedulerImpl" lock until exiting. To avoid the SPARK-7655 issue, we should not
    // "deserialize" the value when holding a lock to avoid blocking other threads. So we call
    // "result.value()" in "TaskResultGetter.enqueueSuccessfulTask" before reaching here.
    // Note: "result.value()" only deserializes the value when it's called at the first time, so
    // here "result.value()" just returns the value and won't block other threads.
    sched.dagScheduler.taskEnded(
      tasks(index), Success, result.value(), result.accumUpdates, info, result.metrics)
    if (!successful(index)) {
      tasksSuccessful += 1
      logInfo("Finished task %s in stage %s (TID %d) in %d ms on %s (%d/%d)".format(
        info.id, taskSet.id, info.taskId, info.duration, info.host, tasksSuccessful, numTasks))
      // Mark successful and stop if all the tasks have succeeded.
      successful(index) = true
      if (tasksSuccessful == numTasks) {
        isZombie = true
      }
    } else {//因为已经完成过了,因此忽略这次结果
      logInfo("Ignoring task-finished event for " + info.id + " in stage " + taskSet.id +
        " because task " + index + " has already completed successfully")
    }
    failedExecutors.remove(index) //删除曾经失败过的任务结果
    maybeFinishTaskSet() //看看是否完成了整个阶段的所有任务
  }

  /**
   * Marks the task as failed, re-adds it to the list of pending tasks, and notifies the
   * DAG Scheduler.
   * 说明一个task任务失败了
   */
  def handleFailedTask(tid: Long, state: TaskState, reason: TaskEndReason) {
    val info = taskInfos(tid)
    if (info.failed) {
      return
    }
    removeRunningTask(tid)
    info.markFailed() //标记该任务失败了
    val index = info.index
    copiesRunning(index) -= 1
    var taskMetrics : TaskMetrics = null

    val failureReason = s"Lost task ${info.id} in stage ${taskSet.id} (TID $tid, ${info.host}): " +
      reason.asInstanceOf[TaskFailedReason].toErrorString
    val failureException: Option[Throwable] = reason match {
      case fetchFailed: FetchFailed =>
        logWarning(failureReason)
        if (!successful(index)) {
          successful(index) = true
          tasksSuccessful += 1
        }
        // Not adding to failed executors for FetchFailed.
        isZombie = true
        None

      case ef: ExceptionFailure =>
        taskMetrics = ef.metrics.orNull
        if (ef.className == classOf[NotSerializableException].getName) {
          // If the task result wasn't serializable, there's no point in trying to re-execute it.
          logError("Task %s in stage %s (TID %d) had a not serializable result: %s; not retrying"
            .format(info.id, taskSet.id, tid, ef.description))
          abort("Task %s in stage %s (TID %d) had a not serializable result: %s".format(
            info.id, taskSet.id, tid, ef.description))
          return
        }
        val key = ef.description
        val now = clock.getTimeMillis()
        val (printFull, dupCount) = {
          if (recentExceptions.contains(key)) {
            val (dupCount, printTime) = recentExceptions(key)
            if (now - printTime > EXCEPTION_PRINT_INTERVAL) {
              recentExceptions(key) = (0, now)
              (true, 0)
            } else {
              recentExceptions(key) = (dupCount + 1, printTime)
              (false, dupCount + 1)
            }
          } else {
            recentExceptions(key) = (0, now)
            (true, 0)
          }
        }
        if (printFull) {
          logWarning(failureReason)
        } else {
          logInfo(
            s"Lost task ${info.id} in stage ${taskSet.id} (TID $tid) on executor ${info.host}: " +
            s"${ef.className} (${ef.description}) [duplicate $dupCount]")
        }
        ef.exception

      case e: TaskFailedReason =>  // TaskResultLost, TaskKilled, and others
        logWarning(failureReason)
        None

      case e: TaskEndReason =>
        logError("Unknown TaskEndReason: " + e)
        None
    }
    // always add to failed executors
    failedExecutors.getOrElseUpdate(index, new HashMap[String, Long]()).
      put(info.executorId, clock.getTimeMillis())
    sched.dagScheduler.taskEnded(tasks(index), reason, null, null, info, taskMetrics)
    addPendingTask(index) //因为任务失败了,因此还要添加到队列中去重新计算该任务
    if (!isZombie && state != TaskState.KILLED && !reason.isInstanceOf[TaskCommitDenied]) {
      // If a task failed because its attempt to commit was denied, do not count this failure
      // towards failing the stage. This is intended to prevent spurious stage failures in cases
      // where many speculative tasks are launched and denied to commit.
      assert (null != failureReason)
      numFailures(index) += 1
      if (numFailures(index) >= maxTaskFailures) {//是否大于最大失败次数
        logError("Task %d in stage %s failed %d times; aborting job".format(
          index, taskSet.id, maxTaskFailures))
        abort("Task %d in stage %s failed %d times, most recent failure: %s\nDriver stacktrace:"
          .format(index, taskSet.id, maxTaskFailures, failureReason), failureException)
        return
      }
    }
    maybeFinishTaskSet()
  }

  //使该尝试阶段的TaskSetManager流产
  def abort(message: String, exception: Option[Throwable] = None): Unit = sched.synchronized {
    // TODO: Kill running tasks if we were not terminated due to a Mesos error
    sched.dagScheduler.taskSetFailed(taskSet, message, exception)
    isZombie = true
    maybeFinishTaskSet()
  }

  /** If the given task ID is not in the set of running tasks, adds it.
   *
   * Used to keep track of the number of running tasks, for enforcing scheduling policies.
   */
  def addRunningTask(tid: Long) {
    if (runningTasksSet.add(tid) && parent != null) {
      parent.increaseRunningTasks(1)
    }
  }

  /** If the given task ID is in the set of running tasks, removes it. */
  def removeRunningTask(tid: Long) {
    if (runningTasksSet.remove(tid) && parent != null) {
      parent.decreaseRunningTasks(1)
    }
  }

  //没有子队列,因为他是叶子节点
  override def getSchedulableByName(name: String): Schedulable = {
    null
  }

  override def addSchedulable(schedulable: Schedulable) {}

  override def removeSchedulable(schedulable: Schedulable) {}

  //仅包含该类本身
  override def getSortedTaskSetQueue(): ArrayBuffer[TaskSetManager] = {
    var sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]()
    sortedTaskSetQueue += this
    sortedTaskSetQueue
  }

  /** Called by TaskScheduler when an executor is lost so we can re-enqueue our tasks */
  override def executorLost(execId: String, host: String) {
    logInfo("Re-queueing tasks for " + execId + " from TaskSet " + taskSet.id)

    // Re-enqueue pending tasks for this host based on the status of the cluster. Note
    // that it's okay if we add a task to the same queue twice (if it had multiple preferred
    // locations), because dequeueTaskFromList will skip already-running tasks.
    for (index <- getPendingTasksForExecutor(execId)) {
      addPendingTask(index, readding = true)
    }
    for (index <- getPendingTasksForHost(host)) {
      addPendingTask(index, readding = true)
    }

    // Re-enqueue any tasks that ran on the failed executor if this is a shuffle map stage,
    // and we are not using an external shuffle server which could serve the shuffle outputs.
    // The reason is the next stage wouldn't be able to fetch the data from this dead executor
    // so we would need to rerun these tasks on other executors.
    if (tasks(0).isInstanceOf[ShuffleMapTask] && !env.blockManager.externalShuffleServiceEnabled) {
      for ((tid, info) <- taskInfos if info.executorId == execId) {
        val index = taskInfos(tid).index
        if (successful(index)) {
          successful(index) = false
          copiesRunning(index) -= 1
          tasksSuccessful -= 1
          addPendingTask(index)
          // Tell the DAGScheduler that this task was resubmitted so that it doesn't think our
          // stage finishes when a total of tasks.size tasks finish.
          sched.dagScheduler.taskEnded(tasks(index), Resubmitted, null, null, info, null)
        }
      }
    }
    // Also re-enqueue any tasks that were running on the node
    for ((tid, info) <- taskInfos if info.running && info.executorId == execId) {
      handleFailedTask(tid, TaskState.FAILED, ExecutorLostFailure(execId))
    }
    // recalculate valid locality levels and waits when executor is lost
    recomputeLocality()
  }

  /**
   * Check for tasks to be speculated and return true if there are any. This is called periodically
   * by the TaskScheduler.
   * 如果有符合推测的任务,则返回true
   * TODO: To make this scale to large jobs, we need to maintain a list of running tasks, so that
   * we don't scan the whole task set. It might also help to make this sorted by launch time.
   */
  override def checkSpeculatableTasks(): Boolean = {
    // Can't speculate if we only have one task, and no need to speculate if the task set is a
    // zombie.如果我们仅仅执行一个任务,则不需要推测执行
    //如果任务设置isZombie,我们也不需要执行推测任务
    if (isZombie || numTasks == 1) {
      return false
    }
    var foundTasks = false
    val minFinishedForSpeculation = (SPECULATION_QUANTILE * numTasks).floor.toInt //计算最小完成数量(达到该数量,才能启动推测执行)
    logDebug("Checking for speculative tasks: minFinished = " + minFinishedForSpeculation)
    if (tasksSuccessful >= minFinishedForSpeculation && tasksSuccessful > 0) { //已经完成了若干个任务了
      val time = clock.getTimeMillis()
      val durations = taskInfos.values.filter(_.successful).map(_.duration).toArray //获取每一个成功完成的任务,每一个任务的处理时间集合
      Arrays.sort(durations) //按照处理时间排序
      val medianDuration = durations(min((0.5 * tasksSuccessful).round.toInt, durations.size - 1)) //获取中位数,因为durations已经是数组了,因此获取0.5 * tasksSuccessful的位置,durations(int) 表示 durations[int]
      val threshold = max(SPECULATION_MULTIPLIER * medianDuration, 100) //最小是100,运行时间超过多少平均值的倍数,才可以对其进行推测执行
      // TODO: Threshold should also look at standard deviation of task durations and have a lower
      // bound based on that.
      logDebug("Task length threshold for speculation: " + threshold)
      for ((tid, info) <- taskInfos) {
        val index = info.index
        //该任务没成功,并且运行时间超过平均时间,并且没有推测任务,则将其添加到推测任务集合中
        if (!successful(index) && copiesRunning(index) == 1 && info.timeRunning(time) > threshold &&
          !speculatableTasks.contains(index)) {
          logInfo(
            "Marking task %d in stage %s (on %s) as speculatable because it ran more than %.0f ms"
              .format(index, taskSet.id, info.host, threshold))
          speculatableTasks += index //加入推测执行任务集合中
          foundTasks = true
        }
      }
    }
    foundTasks
  }

  //获取conf对应的等待时间
  private def getLocalityWait(level: TaskLocality.TaskLocality): Long = {
    val defaultWait = conf.get("spark.locality.wait", "3s")
    val localityWaitKey = level match {//根据级别获取对应的配置文件key
      case TaskLocality.PROCESS_LOCAL => "spark.locality.wait.process"
      case TaskLocality.NODE_LOCAL => "spark.locality.wait.node"
      case TaskLocality.RACK_LOCAL => "spark.locality.wait.rack"
      case _ => null
    }

    if (localityWaitKey != null) {
      conf.getTimeAsMs(localityWaitKey, defaultWait) //获取等待时间
    } else {
      0L
    }
  }

  /**
   * Compute the locality levels used in this TaskSet. Assumes that all tasks have already been
   * added to queues using addPendingTask.
   * 计算任务可以运行在哪些级别上
   */
  private def computeValidLocalityLevels(): Array[TaskLocality.TaskLocality] = {
    import TaskLocality.{PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY} //总级别
    val levels = new ArrayBuffer[TaskLocality.TaskLocality]
    //在executor上可以执行哪些任务
    if (!pendingTasksForExecutor.isEmpty && getLocalityWait(PROCESS_LOCAL) != 0 && //getLocalityWait(PROCESS_LOCAL)>0 说明可以在该executor上等待一下,看看是否任务会分配过来
        pendingTasksForExecutor.keySet.exists(sched.isExecutorAlive(_))) { //是否存在该调度器
      levels += PROCESS_LOCAL
    }
    if (!pendingTasksForHost.isEmpty && getLocalityWait(NODE_LOCAL) != 0 &&
        pendingTasksForHost.keySet.exists(sched.hasExecutorsAliveOnHost(_))) {//说明该host上有该executor
      levels += NODE_LOCAL
    }
    if (!pendingTasksWithNoPrefs.isEmpty) {
      levels += NO_PREF
    }
    if (!pendingTasksForRack.isEmpty && getLocalityWait(RACK_LOCAL) != 0 &&
        pendingTasksForRack.keySet.exists(sched.hasHostAliveOnRack(_))) {//说明该rack上有该executor
      levels += RACK_LOCAL
    }
    levels += ANY //一定任何任务都可以在任意一个executor上执行的
    logDebug("Valid locality levels for " + taskSet + ": " + levels.mkString(", "))
    levels.toArray
  }

  //重新计算本地级别
  def recomputeLocality() {
    val previousLocalityLevel = myLocalityLevels(currentLocalityIndex)
    myLocalityLevels = computeValidLocalityLevels()
    
    localityWaits = myLocalityLevels.map(getLocalityWait) //每一个级别的时间
    currentLocalityIndex = getLocalityIndex(previousLocalityLevel)
  }

  def executorAdded() {
    recomputeLocality()
  }
}

private[spark] object TaskSetManager {
  // The user will be warned if any stages contain a task that has a serialized size greater than
  // this.如果任何阶段包含一个序列化后大小超过100K的任务,则需要发出警告
  val TASK_SIZE_TO_WARN_KB = 100
}
