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

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

/**
 * An interface for schedulable entities.
 * there are two type of Schedulable entities(Pools and TaskSetManagers)
 * 一个调度的接口
 * 一共有两个实现类,分别是Pools和TaskSetManagers
 */
private[spark] trait Schedulable {
  var parent: Pool //该调度器的父亲--通过该对象可以得到一个调度树对象
  // child queues
  def schedulableQueue: ConcurrentLinkedQueue[Schedulable]//调度队列集合,属于该调度的子队列


  //该队列的属性
  def schedulingMode: SchedulingMode //该调度器的模式,公平的还是先进先出的
  def name: String //调度的名字
  def runningTasks: Int //该调度的子子孙孙正在运行的任务数量

  def weight: Int //资源权重
  def minShare: Int //最小CPU的数量
  def priority: Int //优先级
  def stageId: Int //阶段

  //调度队列可以嵌套
  def addSchedulable(schedulable: Schedulable): Unit
  def removeSchedulable(schedulable: Schedulable): Unit
  def getSchedulableByName(name: String): Schedulable//获取属于该调度队列下的一个调度


  def executorLost(executorId: String, host: String): Unit
  def checkSpeculatableTasks(): Boolean //如果有符合推测的任务,则返回true

  def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager]//对子队列进行重新排序,按照队列模式
}
