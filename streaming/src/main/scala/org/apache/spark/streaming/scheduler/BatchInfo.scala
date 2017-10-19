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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.streaming.Time

/**
 * :: DeveloperApi ::
 * Class having information on completed batches.
  * 关于一个jobSet进行批处理batch的信息,表示一组批处理
 * @param batchTime   Time of the batch 批处理JobSet的创建时间
 * @param streamIdToInputInfo A map of input stream id to its input info 记录每一个任务的记录数
 * @param submissionTime  Clock time of when jobs of this batch was submitted to
 *                        the streaming scheduler queue 提交时间
 * @param processingStartTime Clock time of when the first job of this batch started processing 第一个job开始处理的时间
 * @param processingEndTime Clock time of when the last job of this batch finished processing 最后一个job处理完成的时间
 */
@DeveloperApi
case class BatchInfo(
    batchTime: Time,
    streamIdToInputInfo: Map[Int, StreamInputInfo],//stream集合,key是streamId
    submissionTime: Long,//提交时间
    processingStartTime: Option[Long],//开始处理时间
    processingEndTime: Option[Long] //完成时间
  ) {

  @deprecated("Use streamIdToInputInfo instead", "1.5.0")
  def streamIdToNumRecords: Map[Int, Long] = streamIdToInputInfo.mapValues(_.numRecords) //每一个stream中有多少条记录数据

  /**
   * Time taken for the first job of this batch to start processing from the time this batch
   * was submitted to the streaming scheduler. Essentially, it is
   * `processingStartTime` - `submissionTime`.
    * 调度等待时间
   */
  def schedulingDelay: Option[Long] = processingStartTime.map(_ - submissionTime)

  /**
   * Time taken for the all jobs of this batch to finish processing from the time they started
   * processing. Essentially, it is `processingEndTime` - `processingStartTime`.
    * 处理总时间
   */
  def processingDelay: Option[Long] = processingEndTime.zip(processingStartTime)
    .map(x => x._1 - x._2).headOption

  /**
   * Time taken for all the jobs of this batch to finish processing from the time they
   * were submitted.  Essentially, it is `processingDelay` + `schedulingDelay`.
    * 等待调度+处理时间 = 总时间
   */
  def totalDelay: Option[Long] = schedulingDelay.zip(processingDelay)
    .map(x => x._1 + x._2).headOption

  /**
   * The number of recorders received by the receivers in this batch.
    * 一个batch的总记录数量
   */
  def numRecords: Long = streamIdToInputInfo.values.map(_.numRecords).sum
}
