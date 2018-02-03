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

import scala.collection.mutable

import org.apache.spark._
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, RpcEndpoint}

private sealed trait OutputCommitCoordinationMessage extends Serializable

private case object StopCoordinator extends OutputCommitCoordinationMessage
private case class AskPermissionToCommitOutput(stage: Int, partition: Int, attemptNumber: Int)

/**
 * Authority that decides whether tasks can commit output to HDFS. Uses a "first committer wins"
 * policy.
 * 使用第一个提交者机制,决定任务能否被提交到HDFS上
 *
 * OutputCommitCoordinator is instantiated in both the drivers and executors. On executors, it is
 * configured with a reference to the driver's OutputCommitCoordinatorEndpoint, so requests to
 * commit output will be forwarded to the driver's OutputCommitCoordinator.
 * 该实例是要在driver和executor都要实例的,只是executor上持有的是driver的一个引用.因此所有的请求最终还是访问的driver
 *
 * This class was introduced in SPARK-4879; see that JIRA issue (and the associated pull requests)
 * for an extensive design discussion.
 */
private[spark] class OutputCommitCoordinator(conf: SparkConf, isDriver: Boolean) extends Logging {

  // Initialized by SparkEnv
  var coordinatorRef: Option[RpcEndpointRef] = None

  private type StageId = Int
  private type PartitionId = Int
  private type TaskAttemptNumber = Int

  /**
   * Map from active stages's id => partition id => task attempt with exclusive lock on committing
   * output for that partition.
   *
   * Entries are added to the top-level map when stages start and are removed they finish
   * (either successfully or unsuccessfully).
   *
   * Access to this map should be guarded by synchronizing on the OutputCommitCoordinator instance.
   *
   * key是StageId,表示一个阶段,value是该阶段的每一个分区在执行的尝试任务
   */
  private val authorizedCommittersByStage: CommittersByStageMap = mutable.Map()
  private type CommittersByStageMap =
    mutable.Map[StageId, mutable.Map[PartitionId, TaskAttemptNumber]]

  /**
   * Returns whether the OutputCommitCoordinator's internal data structures are all empty.
   */
  def isEmpty: Boolean = {
    authorizedCommittersByStage.isEmpty
  }

  /**
   * Called by tasks to ask whether they can commit their output to HDFS.
   *
   * If a task attempt has been authorized to commit, then all other attempts to commit the same
   * task will be denied.  If the authorized task attempt fails (e.g. due to its executor being
   * lost), then a subsequent task attempt may be authorized to commit its output.
   *
   * @param stage the stage number
   * @param partition the partition number
   * @param attemptNumber how many times this task has been attempted
   *                      (see [[TaskContext.attemptNumber()]])
   * @return true if this task is authorized to commit, false otherwise
   */
  def canCommit(
      stage: StageId,
      partition: PartitionId,
      attemptNumber: TaskAttemptNumber): Boolean = {
    val msg = AskPermissionToCommitOutput(stage, partition, attemptNumber)
    coordinatorRef match {
      case Some(endpointRef) =>
        endpointRef.askWithRetry[Boolean](msg)
      case None =>
        logError(
          "canCommit called after coordinator was stopped (is SparkEnv shutdown in progress)?")
        false
    }
  }

  // Called by DAGScheduler 说明某一个stage开始了
  private[scheduler] def stageStart(stage: StageId): Unit = synchronized {
    authorizedCommittersByStage(stage) = mutable.HashMap[PartitionId, TaskAttemptNumber]()
  }

  // Called by DAGScheduler 说明该阶段已经完成了
  private[scheduler] def stageEnd(stage: StageId): Unit = synchronized {
    authorizedCommittersByStage.remove(stage)
  }

  // Called by DAGScheduler
  private[scheduler] def taskCompleted(
      stage: StageId,
      partition: PartitionId,
      attemptNumber: TaskAttemptNumber,
      reason: TaskEndReason): Unit = synchronized {
    //找到该stage对应的数据Map结构
    val authorizedCommitters = authorizedCommittersByStage.getOrElse(stage, {
      logDebug(s"Ignoring task completion for completed stage")
      return
    })
    reason match {
      case Success =>
      // The task output has been committed successfully
      case denied: TaskCommitDenied =>
        logInfo(s"Task was denied committing, stage: $stage, partition: $partition, " +
        s"attempt: $attemptNumber")
      case otherReason =>
        if (authorizedCommitters.get(partition).exists(_ == attemptNumber)) {
          logDebug(s"Authorized committer (attemptNumber=$attemptNumber, stage=$stage, " +
            s"partition=$partition) failed; clearing lock")
          authorizedCommitters.remove(partition)
        }
    }
  }

  def stop(): Unit = synchronized {
    if (isDriver) {
      coordinatorRef.foreach(_ send StopCoordinator)
      coordinatorRef = None
      authorizedCommittersByStage.clear()
    }
  }

  // Marked private[scheduler] instead of private so this can be mocked in tests
  //测试该分区的尝试任务是否可以提交
  private[scheduler] def handleAskPermissionToCommit(
      stage: StageId,
      partition: PartitionId,
      attemptNumber: TaskAttemptNumber): Boolean = synchronized {
    authorizedCommittersByStage.get(stage) match {
      case Some(authorizedCommitters) =>
        authorizedCommitters.get(partition) match {
          case Some(existingCommitter) => //说明已经存在该分区的任务了
            logDebug(s"Denying attemptNumber=$attemptNumber to commit for stage=$stage, " +
              s"partition=$partition; existingCommitter = $existingCommitter") //打印日志说 拒绝该分区的任务,因为该分区已经存在尝试任务了
            false
          case None =>
            logDebug(s"Authorizing attemptNumber=$attemptNumber to commit for stage=$stage, " +
              s"partition=$partition")
            authorizedCommitters(partition) = attemptNumber //添加该分区的尝试任务
            true
        }
      case None =>
        logDebug(s"Stage $stage has completed, so not allowing attempt number $attemptNumber of" +
          s"partition $partition to commit") //说明该阶段都没开始呢
        false
    }
  }
}

private[spark] object OutputCommitCoordinator {

  // This endpoint is used only for RPC
  private[spark] class OutputCommitCoordinatorEndpoint(
      override val rpcEnv: RpcEnv, outputCommitCoordinator: OutputCommitCoordinator)
    extends RpcEndpoint with Logging {

    override def receive: PartialFunction[Any, Unit] = {
      case StopCoordinator =>
        logInfo("OutputCommitCoordinator stopped!")
        stop()
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case AskPermissionToCommitOutput(stage, partition, attemptNumber) =>
        context.reply(
          outputCommitCoordinator.handleAskPermissionToCommit(stage, partition, attemptNumber))
    }
  }
}
