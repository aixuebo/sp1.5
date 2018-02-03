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

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Logging
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

/**
 * An Schedulable entity that represent collection of Pools or TaskSetManagers
调度器队列的具体实现类Pool
该类表示的就是一个具体的调度器队列的非叶子节点,即承担了队列的容器工作

调度器队列的具体实现类TaskSetManager
主要承担了叶子节点的功能
 */
private[spark] class Pool(
    val poolName: String,//调度的name
    val schedulingMode: SchedulingMode,//调度模式是先进先出,还是公平模式
    initMinShare: Int,
    initWeight: Int)
  extends Schedulable
  with Logging {

  //调度的队列---因为调度是有父子关系的
  val schedulableQueue = new ConcurrentLinkedQueue[Schedulable]
  //调度的name与调度的映射关系
  val schedulableNameToSchedulable = new ConcurrentHashMap[String, Schedulable]

  var weight = initWeight
  var minShare = initMinShare
  var runningTasks = 0 //该队列的子子孙孙中正在运行的任务数量
  var priority = 0

  // A pool's stage id is used to break the tie in scheduling.
  var stageId = -1

  var name = poolName //队列名字
  var parent: Pool = null //父队列

  //调度的算法
  var taskSetSchedulingAlgorithm: SchedulingAlgorithm = {
    schedulingMode match {
      case SchedulingMode.FAIR =>
        new FairSchedulingAlgorithm()
      case SchedulingMode.FIFO =>
        new FIFOSchedulingAlgorithm()
    }
  }

  //设置父子关系
  override def addSchedulable(schedulable: Schedulable) {
    require(schedulable != null)
    schedulableQueue.add(schedulable)
    schedulableNameToSchedulable.put(schedulable.name, schedulable)
    schedulable.parent = this
  }

  //移除一个子队列
  override def removeSchedulable(schedulable: Schedulable) {
    schedulableQueue.remove(schedulable)
    schedulableNameToSchedulable.remove(schedulable.name)
  }

  //子子孙孙中查找该name的调度
  override def getSchedulableByName(schedulableName: String): Schedulable = {
    if (schedulableNameToSchedulable.containsKey(schedulableName)) {
      return schedulableNameToSchedulable.get(schedulableName)
    }
    for (schedulable <- schedulableQueue) {//在每一个子队列中继续查找
      val sched = schedulable.getSchedulableByName(schedulableName)
      if (sched != null) {
        return sched
      }
    }
    null
  }

  override def executorLost(executorId: String, host: String) {
    schedulableQueue.foreach(_.executorLost(executorId, host)) //丢失了一个节点
  }

  //校验是否有延迟加载的任务
  override def checkSpeculatableTasks(): Boolean = {
    var shouldRevive = false
    for (schedulable <- schedulableQueue) {
      shouldRevive |= schedulable.checkSpeculatableTasks() //递归操作
    }
    shouldRevive
  }

  //获取排序后的TaskSetManager队列
  //注意:只要第一轮判断某个节点优先级高,那么就对该优先级高的队列的所有任务都一定比 优先级底的任务要高,这是一个递归过程
  override def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] = {
    var sortedTaskSetQueue = new ArrayBuffer[TaskSetManager] //排序后的任务集合
    val sortedSchedulableQueue =
      schedulableQueue.toSeq.sortWith(taskSetSchedulingAlgorithm.comparator) //所有调度任务进行排序
    for (schedulable <- sortedSchedulableQueue) {
      sortedTaskSetQueue ++= schedulable.getSortedTaskSetQueue
    }
    sortedTaskSetQueue
  }

  //增加队列中包含任务数量
  def increaseRunningTasks(taskNum: Int) {
    runningTasks += taskNum
    if (parent != null) {
      parent.increaseRunningTasks(taskNum)
    }
  }

  //减少队列中包含任务数量
  def decreaseRunningTasks(taskNum: Int) {
    runningTasks -= taskNum
    if (parent != null) {
      parent.decreaseRunningTasks(taskNum)
    }
  }
}
