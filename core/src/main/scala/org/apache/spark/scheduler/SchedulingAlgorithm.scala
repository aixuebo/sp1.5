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

/**
 * An interface for sort algorithm
 * FIFO: FIFO algorithm between TaskSetManagers
 * FS: FS algorithm between Pools, and FIFO or FS within Pools
 * 调度的算法
 */
private[spark] trait SchedulingAlgorithm {
  def comparator(s1: Schedulable, s2: Schedulable): Boolean
}

//先比较优先级,再比较阶段---一般这种队列都用于叶子节点的倒数第二个节点上,即该节点管理的都是叶子节点
private[spark] class FIFOSchedulingAlgorithm extends SchedulingAlgorithm {
  override def comparator(s1: Schedulable, s2: Schedulable): Boolean = {
    val priority1 = s1.priority
    val priority2 = s2.priority
    var res = math.signum(priority1 - priority2)
    if (res == 0) {
      val stageId1 = s1.stageId
      val stageId2 = s2.stageId
      res = math.signum(stageId1 - stageId2)
    }
    if (res < 0) {
      true
    } else {
      false
    }
  }
}

private[spark] class FairSchedulingAlgorithm extends SchedulingAlgorithm {
  override def comparator(s1: Schedulable, s2: Schedulable): Boolean = {
    //该队列的最小资源
    val minShare1 = s1.minShare
    val minShare2 = s2.minShare
    //该队列上存在运行中的任务数量
    val runningTasks1 = s1.runningTasks
    val runningTasks2 = s2.runningTasks

    //即该队列上最少也要有这些个task在运行
    val s1Needy = runningTasks1 < minShare1 //true表示缺乏,即运行的task没有达到最小值,可以继续运行
    val s2Needy = runningTasks2 < minShare2

    //当前任务数量是最小资源的多少倍,该比值越大,说明当前队列任务越重
    val minShareRatio1 = runningTasks1.toDouble / math.max(minShare1, 1.0).toDouble //math.max(minShare1, 1.0)说明队列至少也要有1个任务在运行
    val minShareRatio2 = runningTasks2.toDouble / math.max(minShare2, 1.0).toDouble

    //表示单位权重下的任务数量,越大,表示任务越重---用于在队列未满的时候,让队列均匀按照权重分配任务
    val taskToWeightRatio1 = runningTasks1.toDouble / s1.weight.toDouble
    val taskToWeightRatio2 = runningTasks2.toDouble / s2.weight.toDouble

    //比较最终结果
    var compare: Int = 0

    if (s1Needy && !s2Needy) {//如果s1缺乏,而s2满足了,则就让给s1运行
      return true
    } else if (!s1Needy && s2Needy) {//说明给队列2
      return false
    } else if (s1Needy && s2Needy) {//如果两个队列都缺乏,则需要竞争,看谁的占比更小,谁更合适
      compare = minShareRatio1.compareTo(minShareRatio2)
    } else {//如果两个队列都已经达到了最小值,则需要竞争,看权重---谁小就放哪个队列中,看谁的占比更小,谁更合适
      compare = taskToWeightRatio1.compareTo(taskToWeightRatio2)
    }

    //最终什么数据都相同,则比较name
    if (compare < 0) {
      true
    } else if (compare > 0) {
      false
    } else {
      s1.name < s2.name //name一定可以排序出来的
    }
  }
}

