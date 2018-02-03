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

import scala.collection.mutable.ListBuffer

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * Information about a running task attempt inside a TaskSet.
 */
@DeveloperApi
class TaskInfo(
    val taskId: Long,//为task自增长一个唯一ID
    val index: Int,//第几个task任务
    val attemptNumber: Int,//尝试任务编号
    val launchTime: Long,//启动时间
    val executorId: String,//在哪个executor执行该任务
    val host: String,//exeutor所在的host节点
    val taskLocality: TaskLocality.TaskLocality,//启动该task的级别
    val speculative: Boolean) {//是否是推测执行启动的任务

  /**
   * The time when the task started remotely getting the result. Will not be set if the
   * task result was sent immediately when the task finished (as opposed to sending an
   * IndirectTaskResult and later fetching the result from the block manager).
   */
  var gettingResultTime: Long = 0

  /**
   * Intermediate updates to accumulables during this task. Note that it is valid for the same
   * accumulable to be updated multiple times in a single task or for two accumulables with the
   * same name but different IDs to exist in a task.
   */
  val accumulables = ListBuffer[AccumulableInfo]()

  /**
   * The time when the task has completed successfully (including the time to remotely fetch
   * results, if necessary).
   */
  var finishTime: Long = 0 //完成时间

  var failed = false //是否失败

  //表示结果已经存在的时间
  private[spark] def markGettingResult(time: Long = System.currentTimeMillis) {
    gettingResultTime = time
  }

  //表示成功的时间
  private[spark] def markSuccessful(time: Long = System.currentTimeMillis) {
    finishTime = time
  }

  //表示失败的时间
  private[spark] def markFailed(time: Long = System.currentTimeMillis) {
    finishTime = time
    failed = true
  }

  def gettingResult: Boolean = gettingResultTime != 0

  def finished: Boolean = finishTime != 0 //完成了  可能成功  可能失败

  def successful: Boolean = finished && !failed //成功完成

  def running: Boolean = !finished //是否运行中

  def status: String = {
    if (running) {
      if (gettingResult) {
        "GET RESULT"
      } else {
        "RUNNING"
      }
    } else if (failed) {
      "FAILED"
    } else if (successful) {
      "SUCCESS"
    } else {
      "UNKNOWN"
    }
  }

  @deprecated("Use attemptNumber", "1.6.0")
  def attempt: Int = attemptNumber

  def id: String = s"$index.$attemptNumber"

  //任务启动到完成需要的时间
  def duration: Long = {
    if (!finished) {
      throw new UnsupportedOperationException("duration() called on unfinished task")
    } else {
      finishTime - launchTime
    }
  }

  //运行时间
  private[spark] def timeRunning(currentTime: Long): Long = currentTime - launchTime
}
