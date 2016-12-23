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

import java.util.Properties

import scala.collection.Map
import scala.language.existentials

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CallSite

/**
 * Types of events that can be handled by the DAGScheduler.
 * 调度器能够处理的事件类型
 * The DAGScheduler uses an event queue architecture where any thread can post an event (e.g. a task finishing or a new job being
 * submitted) but there is a single "logic" thread that reads these events and takes decisions.
 * 调度器使用事件队列去建筑,任何线程都能够发送一个事件(例如,task完成或者一个新的job正在提交中),但是有一个单独的逻辑线程能够读取这些事件和任务
 * This greatly simplifies synchronization.
 * 这就是简单的同步方式
 */
private[scheduler] sealed trait DAGSchedulerEvent //基本类,表示调度事件

//一个job已经被提交到调度了
private[scheduler] case class JobSubmitted(
    jobId: Int,//jobId
    finalRDD: RDD[_],//要处理的RDD
    func: (TaskContext, Iterator[_]) => _,//如何处理RDD的每一个分区
    partitions: Array[Int],//多少个分区
    callSite: CallSite,
    listener: JobListener,
    properties: Properties = null)
  extends DAGSchedulerEvent

//取消一个stage阶段调度
private[scheduler] case class StageCancelled(stageId: Int) extends DAGSchedulerEvent

//取消一个job调度
private[scheduler] case class JobCancelled(jobId: Int) extends DAGSchedulerEvent

//取消全部job的调度
private[scheduler] case object AllJobsCancelled extends DAGSchedulerEvent

//取消一个组内全部job
private[scheduler] case class JobGroupCancelled(groupId: String) extends DAGSchedulerEvent


private[scheduler]
case class BeginEvent(task: Task[_], taskInfo: TaskInfo) extends DAGSchedulerEvent

private[scheduler]
case class GettingResultEvent(taskInfo: TaskInfo) extends DAGSchedulerEvent

private[scheduler] case class CompletionEvent(
    task: Task[_],
    reason: TaskEndReason,
    result: Any,
    accumUpdates: Map[Long, Any],
    taskInfo: TaskInfo,
    taskMetrics: TaskMetrics)
  extends DAGSchedulerEvent

private[scheduler] case class ExecutorAdded(execId: String, host: String) extends DAGSchedulerEvent

private[scheduler] case class ExecutorLost(execId: String) extends DAGSchedulerEvent

private[scheduler]
case class TaskSetFailed(taskSet: TaskSet, reason: String, exception: Option[Throwable])
  extends DAGSchedulerEvent

private[scheduler] case object ResubmitFailedStages extends DAGSchedulerEvent
