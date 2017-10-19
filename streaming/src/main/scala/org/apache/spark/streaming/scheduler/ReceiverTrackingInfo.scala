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

import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.streaming.scheduler.ReceiverState._

//receiver发送过来的错误信息
private[streaming] case class ReceiverErrorInfo(
    lastErrorMessage: String = "",//message正常信息
    lastError: String = "", //error打印的异常信息堆栈
    lastErrorTime: Long = -1L)//最后更新时间

/**
 * Class having information about a receiver.
 * 在driver端 对一个receriver的描述
 * @param receiverId the unique receiver id
 * @param state the current Receiver state
 * @param scheduledExecutors the scheduled executors provided by ReceiverSchedulingPolicy
 * @param runningExecutor the running executor if the receiver is active
 * @param name the receiver name
 * @param endpoint the receiver endpoint. It can be used to send messages to the receiver
 * @param errorInfo the receiver error information if it fails
 * 因为该driver上可以启动好多个receiver去接收数据,因此每一个receiver对应一个streamid
  而该receiver是在其他executor上执行的,因此driver上要为每一个receiver保持一个对象在内存中,该对象即ReceiverTrackingInfo
 */
private[streaming] case class ReceiverTrackingInfo(
    receiverId: Int,//表示哪一个receiver
    state: ReceiverState,//receiver此时的状态
    scheduledExecutors: Option[Seq[String]],//该receiver在哪些host上可以去被调度
    runningExecutor: Option[String],//真正在哪个host上运行的该receiver,去接收streamid对应的流信息
    name: Option[String] = None,
    endpoint: Option[RpcEndpointRef] = None,//如果与receiver与交互的socket,即receiver端的服务器地址
    errorInfo: Option[ReceiverErrorInfo] = None) { //错误信息对象

  def toReceiverInfo: ReceiverInfo = ReceiverInfo(
    receiverId,
    name.getOrElse(""),
    state == ReceiverState.ACTIVE,
    location = runningExecutor.getOrElse(""),
    lastErrorMessage = errorInfo.map(_.lastErrorMessage).getOrElse(""),
    lastError = errorInfo.map(_.lastError).getOrElse(""),
    lastErrorTime = errorInfo.map(_.lastErrorTime).getOrElse(-1L)
  )
}
