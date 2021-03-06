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

import scala.collection.mutable.HashMap

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.storage.RDDInfo

/**
 * :: DeveloperApi ::
 * Stores information about a stage to pass from the scheduler to SparkListeners.
 * 关于记录调度任务的一个阶段信息
 */
@DeveloperApi
class StageInfo(
    val stageId: Int,//阶段ID
    val attemptId: Int,//第几次尝试执行该阶段
    val name: String,
    val numTasks: Int,//该阶段的任务数量
    val rddInfos: Seq[RDDInfo],//依赖哪些RDD
    val parentIds: Seq[Int],//依赖的哪些stageId集合
    val details: String,//callSite.longForm
    private[spark] val taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty) { //推荐每一个partition任务在哪个节点上执行
  /** When this stage was submitted from the DAGScheduler to a TaskScheduler.
    * 该阶段被提交的时间
    **/
  var submissionTime: Option[Long] = None
  /** Time when all tasks in the stage completed or when the stage was cancelled.
    * 该阶段完成的时间
    **/
  var completionTime: Option[Long] = None
  /** If the stage failed, the reason why. 失败原因*/
  var failureReason: Option[String] = None
  /** Terminal values of accumulables updated during this stage. */
  val accumulables = HashMap[Long, AccumulableInfo]()

  //该阶段最终失败
  def stageFailed(reason: String) {
    failureReason = Some(reason)
    completionTime = Some(System.currentTimeMillis)//完成时间
  }

  private[spark] def getStatusString: String = {
    if (completionTime.isDefined) {
      if (failureReason.isDefined) {
        "failed"
      } else {
        "succeeded"
      }
    } else {
      "running"
    }
  }
}

private[spark] object StageInfo {
  /**
   * Construct a StageInfo from a Stage.
   *
   * Each Stage is associated with one or many RDDs, with the boundary of a Stage marked by
   * shuffle dependencies. Therefore, all ancestor RDDs related to this Stage's RDD through a
   * sequence of narrow dependencies should also be associated with this Stage.
   */
  def fromStage(
      stage: Stage,
      attemptId: Int,
      numTasks: Option[Int] = None,//需要执行多少个parititon
      taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty //推荐每一个partition任务在哪个节点上执行
    ): StageInfo = {
    val ancestorRddInfos = stage.rdd.getNarrowAncestors.map(RDDInfo.fromRdd)
    val rddInfos = Seq(RDDInfo.fromRdd(stage.rdd)) ++ ancestorRddInfos
    new StageInfo(
      stage.id,
      attemptId,
      stage.name,
      numTasks.getOrElse(stage.numTasks),
      rddInfos,
      stage.parents.map(_.id),
      stage.details,
      taskLocalityPreferences)
  }
}
