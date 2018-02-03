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

import scala.collection.mutable.{ArrayBuffer, HashMap}

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.metrics.source.Source
import org.apache.spark.unsafe.memory.TaskMemoryManager
import org.apache.spark.util.{TaskCompletionListener, TaskCompletionListenerException}

//封装该task的基本属性,即所属阶段ID,所属阶段的分区,第几个尝试任务
private[spark] class TaskContextImpl(
    val stageId: Int,
    val partitionId: Int,
    override val taskAttemptId: Long,
    override val attemptNumber: Int,
    override val taskMemoryManager: TaskMemoryManager,
    @transient private val metricsSystem: MetricsSystem,
    internalAccumulators: Seq[Accumulator[Long]],
    val runningLocally: Boolean = false,
    val taskMetrics: TaskMetrics = TaskMetrics.empty)
  extends TaskContext
  with Logging {

  // For backwards-compatibility; this method is now deprecated as of 1.3.0.
  override def attemptId(): Long = taskAttemptId

  // List of callback functions to execute when the task completes.当完成的时候产生的回调函数集合
  @transient private val onCompleteCallbacks = new ArrayBuffer[TaskCompletionListener]

  // Whether the corresponding task has been killed.
  @volatile private var interrupted: Boolean = false //true表示被kill了

  // Whether the task has completed.
  @volatile private var completed: Boolean = false //true表示完成task

  override def addTaskCompletionListener(listener: TaskCompletionListener): this.type = {
    onCompleteCallbacks += listener
    this
  }

  override def addTaskCompletionListener(f: TaskContext => Unit): this.type = {
    onCompleteCallbacks += new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = f(context)
    }
    this
  }

  //即使参数是TaskContext,但是回调函数中没有用到该参数,因此没必要含有该参数
  @deprecated("use addTaskCompletionListener", "1.1.0")
  override def addOnCompleteCallback(f: () => Unit) {
    onCompleteCallbacks += new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = f()
    }
  }

  /** Marks the task as completed and triggers the listeners.
    * 当任务完成的时候,调用回调函数,并且将上下文传递给回调函数
    **/
  private[spark] def markTaskCompleted(): Unit = {
    completed = true
    val errorMsgs = new ArrayBuffer[String](2)
    // Process complete callbacks in the reverse order of registration
    onCompleteCallbacks.reverse.foreach { listener =>
      try {
        listener.onTaskCompletion(this) //上下文this传递给回调函数
      } catch {
        case e: Throwable =>
          errorMsgs += e.getMessage
          logError("Error in TaskCompletionListener", e)
      }
    }
    if (errorMsgs.nonEmpty) {
      throw new TaskCompletionListenerException(errorMsgs)
    }
  }

  /** Marks the task for interruption, i.e. cancellation. */
  private[spark] def markInterrupted(): Unit = {
    interrupted = true
  }

  override def isCompleted(): Boolean = completed

  override def isRunningLocally(): Boolean = runningLocally

  override def isInterrupted(): Boolean = interrupted

  override def getMetricsSources(sourceName: String): Seq[Source] =
    metricsSystem.getSourcesByName(sourceName)

  @transient private val accumulators = new HashMap[Long, Accumulable[_, _]]

  private[spark] override def registerAccumulator(a: Accumulable[_, _]): Unit = synchronized {
    accumulators(a.id) = a
  }

  private[spark] override def collectInternalAccumulators(): Map[Long, Any] = synchronized {
    accumulators.filter(_._2.isInternal).mapValues(_.localValue).toMap
  }

  private[spark] override def collectAccumulators(): Map[Long, Any] = synchronized {
    accumulators.mapValues(_.localValue).toMap
  }

  private[spark] override val internalMetricsToAccumulators: Map[String, Accumulator[Long]] = {
    // Explicitly register internal accumulators here because these are
    // not captured in the task closure and are already deserialized
    internalAccumulators.foreach(registerAccumulator)
    internalAccumulators.map { a => (a.name.get, a) }.toMap
  }
}
