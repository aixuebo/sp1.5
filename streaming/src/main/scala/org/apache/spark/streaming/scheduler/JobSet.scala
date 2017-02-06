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

package org.apache.spark.streaming.scheduler

import scala.collection.mutable.HashSet

import org.apache.spark.streaming.Time

/** Class representing a set of Jobs
  * belong to the same batch.
  * driver需要从多个streaming中获取数据源,因此对应多个streaming,每一个streaming的一次任务就是一个job,
  * 因此该对象表示所有的streaming中job集合
  */
private[streaming]
case class JobSet(
    time: Time,//什么时间点产生的job集合
    jobs: Seq[Job],//每一个streaming对应一个job对象
    streamIdToInputInfo: Map[Int, StreamInputInfo] = Map.empty) {

  private val incompleteJobs = new HashSet[Job]() //未完成的job集合
  private val submissionTime = System.currentTimeMillis() // when this jobset was submitted 提交JobSet时候的时间
  private var processingStartTime = -1L // when the first job of this jobset started processing 第一个job开始处理的时间
  private var processingEndTime = -1L // when the last job of this jobset finished processing 最后一个job处理完成的时间

  jobs.zipWithIndex.foreach { case (job, i) => job.setOutputOpId(i) } //为该jobSet中的job分配唯一的ID
  incompleteJobs ++= jobs

  def handleJobStart(job: Job) {//每个job启动的时候都处理该方法,但是只有第一个job才会真正意义上修改时间
    if (processingStartTime < 0) processingStartTime = System.currentTimeMillis()
  }

  def handleJobCompletion(job: Job) { //每个job完成的时候都处理该方法
    incompleteJobs -= job //因为该job已经执行完了,因此从未完成的job中移除该job
    if (hasCompleted) processingEndTime = System.currentTimeMillis() //只有最后一个job才会真正意义上修改时间
  }

  def hasStarted: Boolean = processingStartTime > 0 //已经开始执行job了

  def hasCompleted: Boolean = incompleteJobs.isEmpty //true表示已经没有未执行的job了,即所有job都完成了

  // Time taken to process all the jobs from the time they started processing
  // (i.e. not including the time they wait in the streaming scheduler queue)
  //处理所有job花费的总时间
  def processingDelay: Long = processingEndTime - processingStartTime

  // Time taken to process all the jobs from the time they were submitted
  // (i.e. including the time they wait in the streaming scheduler queue)
  //总花费时间
  def totalDelay: Long = {
    processingEndTime - time.milliseconds
  }

  def toBatchInfo: BatchInfo = {
    new BatchInfo(
      time,
      streamIdToInputInfo,
      submissionTime,
      if (processingStartTime >= 0 ) Some(processingStartTime) else None,
      if (processingEndTime >= 0 ) Some(processingEndTime) else None
    )
  }
}
