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

import scala.collection.mutable.Queue

import org.apache.spark.util.Distribution
import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * Base trait for events related to StreamingListener
 * 定义事件
 */
@DeveloperApi
sealed trait StreamingListenerEvent

@DeveloperApi
case class StreamingListenerBatchSubmitted(batchInfo: BatchInfo) extends StreamingListenerEvent //说明jobGenerator每隔一段时间就产生的一组要执行的任务,即产生的JobSet已经给调度器了

@DeveloperApi
case class StreamingListenerBatchCompleted(batchInfo: BatchInfo) extends StreamingListenerEvent //当一个jobset中全部job都完成的时候调用该事件

@DeveloperApi
case class StreamingListenerBatchStarted(batchInfo: BatchInfo) extends StreamingListenerEvent //说明jobSet中第一个job开始执行了

@DeveloperApi
case class StreamingListenerReceiverStarted(receiverInfo: ReceiverInfo)
  extends StreamingListenerEvent //说明一个streaming的receiver已经被注册了

@DeveloperApi
case class StreamingListenerReceiverStopped(receiverInfo: ReceiverInfo)
  extends StreamingListenerEvent  //说明一个streaming的receiver已经被取消注册了

@DeveloperApi
case class StreamingListenerReceiverError(receiverInfo: ReceiverInfo)
  extends StreamingListenerEvent //说明一个streaming的receiver接收到了错误信息

/**
 * :: DeveloperApi ::
 * A listener interface for receiving information about an ongoing streaming
 * computation.
 * 定义监听过程,当特定事件发生的时候,调用不同的过程
 */
@DeveloperApi
trait StreamingListener {

  /** Called when a receiver has been started */
  def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted) { }

  /** Called when a receiver has reported an error */
  def onReceiverError(receiverError: StreamingListenerReceiverError) { }

  /** Called when a receiver has been stopped */
  def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped) { }

  /** Called when a batch of jobs has been submitted for processing. */
  def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted) { }

  /** Called when processing of a batch of jobs has started.  */
  def onBatchStarted(batchStarted: StreamingListenerBatchStarted) { }

  /** Called when processing of a batch of jobs has completed. */
  def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) { }
}


/**
 * :: DeveloperApi ::
 * A simple StreamingListener that logs summary statistics across Spark Streaming batches
 * @param numBatchInfos Number of last batches to consider for generating statistics (default: 10)
 * 简单的实现,记录一些统计信息
 */
@DeveloperApi
class StatsReportListener(numBatchInfos: Int = 10) extends StreamingListener {
  // Queue containing latest completed batches
  val batchInfos = new Queue[BatchInfo]()//包含最近完成的批处理任务的统计信息

  //完成的时候调用
  override def onBatchCompleted(batchStarted: StreamingListenerBatchCompleted) {
    batchInfos.enqueue(batchStarted.batchInfo)
    if (batchInfos.size > numBatchInfos) batchInfos.dequeue() //说明超过范围了,让一些数据出队列
    printStats()
  }

  //打印总消耗时间和deley消耗时间
  def printStats() {
    showMillisDistribution("Total delay: ", _.totalDelay)
    showMillisDistribution("Processing time: ", _.processingDelay)
  }

  def showMillisDistribution(heading: String, getMetric: BatchInfo => Option[Long]) {
    org.apache.spark.scheduler.StatsReportListener.showMillisDistribution(
      heading, extractDistribution(getMetric))
  }

  def extractDistribution(getMetric: BatchInfo => Option[Long]): Option[Distribution] = {
    Distribution(batchInfos.flatMap(getMetric(_)).map(_.toDouble))
  }
}
