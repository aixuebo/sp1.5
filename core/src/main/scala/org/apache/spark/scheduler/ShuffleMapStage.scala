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

import org.apache.spark.ShuffleDependency
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.CallSite

/**
 * The ShuffleMapStage represents the intermediate stages in a job.
 * ShuffleMapStage代表job中的一个中间阶段
 */
private[spark] class ShuffleMapStage(
    id: Int,
    rdd: RDD[_],
    numTasks: Int,
    parents: List[Stage],
    firstJobId: Int,
    callSite: CallSite,
    val shuffleDep: ShuffleDependency[_, _, _])
  extends Stage(id, rdd, numTasks, parents, firstJobId, callSite) {

  override def toString: String = "ShuffleMapStage " + id

  var numAvailableOutputs: Long = 0 //可用的分区输出数量

  def isAvailable: Boolean = numAvailableOutputs == numPartitions

  //每一个partition是一个List[MapStatus]作为数组的元素
  val outputLocs = Array.fill[List[MapStatus]](numPartitions)(Nil)

  //为该partition添加一个输出
  def addOutputLoc(partition: Int, status: MapStatus): Unit = {
    val prevList = outputLocs(partition)//返回该partition对应的List[MapStatus]元素
    outputLocs(partition) = status :: prevList //将status追加到数组的前面
    if (prevList == Nil) {
      numAvailableOutputs += 1 //可用的输出数量累加1
    }
  }

  //移除该partition的某一个输出
  def removeOutputLoc(partition: Int, bmAddress: BlockManagerId): Unit = {
    val prevList = outputLocs(partition) //找到该partiiton对应的输出集合
    val newList = prevList.filterNot(_.location == bmAddress) //过滤---删除bmAddress节点的数据块信息
    outputLocs(partition) = newList //重新设置
    if (prevList != Nil && newList == Nil) {
      numAvailableOutputs -= 1 //减少一个有效可用的输出数量
    }
  }

  /**
   * Removes all shuffle outputs associated with this executor. Note that this will also remove
   * outputs which are served by an external shuffle server (if one exists), as they are still
   * registered with this execId.
   * 移除在execId上的结果集
   */
  def removeOutputsOnExecutor(execId: String): Unit = {
    var becameUnavailable = false //默认变成不可用为false
    for (partition <- 0 until numPartitions) {//循环每一个partition
      val prevList = outputLocs(partition)//找到该partition对应的集合
      val newList = prevList.filterNot(_.location.executorId == execId) //过滤在execId上的结果集
      outputLocs(partition) = newList //重新设置集合
      if (prevList != Nil && newList == Nil) {
        becameUnavailable = true //说明不可用了
        numAvailableOutputs -= 1 //减少一个有效可用的输出数量
      }
    }
    if (becameUnavailable) { //打印日志,当不可用的时候
      logInfo("%s is now unavailable on executor %s (%d/%d, %s)".format(
        this, execId, numAvailableOutputs, numPartitions, isAvailable))
    }
  }
}
