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

import scala.collection.mutable.HashSet

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CallSite

/**
 * A stage is a set of independent tasks all computing the same function that need to run as part
 * of a Spark job, where all the tasks have the same shuffle dependencies. Each DAG of tasks run
 * by the scheduler is split up into stages at the boundaries where shuffle occurs, and then the
 * DAGScheduler runs these stages in topological order.
 *
 * Each Stage can either be a shuffle map stage, in which case its tasks' results are input for
 * another stage, or a result stage, in which case its tasks directly compute the action that
 * initiated a job (e.g. count(), save(), etc). For shuffle map stages, we also track the nodes
 * that each output partition is on.
 *
 * Each Stage also has a firstJobId, identifying the job that first submitted the stage.  When FIFO
 * scheduling is used, this allows Stages from earlier jobs to be computed first or recovered
 * faster on failure.
 *
 * The callSite provides a location in user code which relates to the stage. For a shuffle map
 * stage, the callSite gives the user code that created the RDD being shuffled. For a result
 * stage, the callSite gives the user code that executes the associated action (e.g. count()).
 *
 * A single stage can consist of multiple attempts. In that case, the latestInfo field will
 * be updated for each attempt.
 *
 Stage的意义(网上的文章):
 对RDD的操作分为transformation和action两类，真正的作业提交运行发生在action之后，调用action之后会将对原始输入数据的所有transformation操作封装成作业并向集群提交运行。这个过程大致可以如下描述：
由DAGScheduler对RDD之间的依赖性进行分析，通过DAG来分析各个RDD之间的转换依赖关系
根据DAGScheduler分析得到的RDD依赖关系将Job划分成多个stage
每个stage会生成一个TaskSet并提交给TaskScheduler，调度权转交给TaskScheduler，由它来负责分发task到worker执行

job : A job is triggered by an action, like count() or saveAsTextFile(). Click on a job to see information about the stages of tasks inside it. 理解了吗，所谓一个 job，就是由一个 rdd 的 action 触发的动作，可以简单的理解为，当你需要执行一个 rdd 的 action 的时候，会生成一个 job。
stage : stage 是一个 job 的组成单位，就是说，一个 job 会被切分成 1 个或 1 个以上的 stage，然后各个 stage 会按照执行顺序依次执行。至于 job 根据什么标准来切分 stage，可以回顾第二篇博文：『 Spark 』2. spark 基本概念解析
task : A unit of work within a stage, corresponding to one RDD partition。即 stage 下的一个任务执行单元，一般来说，一个 rdd 有多少个 partition，就会有多少个 task，因为每一个 task 只是处理一个 partition 上的数据。
从 web ui 截图上我们可以看到，这个 job 一共有 2 个 stage，66 个 task，平均下来每个 stage 有 33 个 task，相当于每个 stage 的数据都有 33 个 partition
[注意：这里是平均下来的哦，并不都是每个 stage 有 33 个 task，有时候也会有一个 stage 多，另外一个 stage 少的情况，就看你有没有在不同的 stage 进行 repartition 类似的操作了。]
 *
 */
private[spark] abstract class Stage(
    val id: Int,//阶段ID
    val rdd: RDD[_],//该阶段的RDD输入源
    val numTasks: Int,//该阶段的任务数量,即RDD的partition数量
    val parents: List[Stage],//该阶段依赖哪些阶段
    val firstJobId: Int,//该阶段属于哪个jobid
    val callSite: CallSite)
  extends Logging {

  val numPartitions = rdd.partitions.size //该阶段需要多少个partition

  /** Set of jobs that this stage belongs to.
    * 可能该阶段被多个job共同使用,因此这里面是使用该阶段的job集合
    **/
  val jobIds = new HashSet[Int]

  var pendingTasks = new HashSet[Task[_]] //等待提交到执行队列的任务集合

  /** The ID to use for the next new attempt for this stage.最后一次尝试阶段的ID */
  private var nextAttemptId: Int = 0

  val name = callSite.shortForm
  val details = callSite.longForm

  private var _internalAccumulators: Seq[Accumulator[Long]] = Seq.empty

  /** Internal accumulators shared across all tasks in this stage. */
  def internalAccumulators: Seq[Accumulator[Long]] = _internalAccumulators

  /**
   * Re-initialize the internal accumulators associated with this stage.
   *
   * This is called every time the stage is submitted, *except* when a subset of tasks
   * belonging to this stage has already finished. Otherwise, reinitializing the internal
   * accumulators here again will override partial values from the finished tasks.
   */
  def resetInternalAccumulators(): Unit = {
    _internalAccumulators = InternalAccumulator.create(rdd.sparkContext)
  }

  /**
   * Pointer to the [StageInfo] object for the most recent attempt.
   * 指示最后一个阶段的尝试任务对应的状态信息StageInfo
   * This needs to be initialized here, before any attempts have actually been created, because the DAGScheduler uses this
   * StageInfo to tell SparkListeners when a job starts (which happens before any stage attempts
   * have been created).
   * 任何尝试任务被创建之前,初始化在这里进行, 因为当job开始的时候,调度器使用StageInfo这个对象去通知监听器
   */
  private var _latestInfo: StageInfo = StageInfo.fromStage(this, nextAttemptId)

  /** Creates a new attempt for this stage by creating a new StageInfo with a new attempt ID. */
  def makeNewStageAttempt(
      numPartitionsToCompute: Int,//要去重新计算的partition数量
      taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty): Unit = {//每一个partition推荐在哪个节点执行
    _latestInfo = StageInfo.fromStage(
      this, nextAttemptId, Some(numPartitionsToCompute), taskLocalityPreferences)
    nextAttemptId += 1
  }

  /** Returns the StageInfo for the most recent attempt for this stage. */
  def latestInfo: StageInfo = _latestInfo

  override final def hashCode(): Int = id
  override final def equals(other: Any): Boolean = other match {
    case stage: Stage => stage != null && stage.id == id
    case _ => false
  }
}
