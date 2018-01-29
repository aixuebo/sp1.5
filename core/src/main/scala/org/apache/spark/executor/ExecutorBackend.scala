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

package org.apache.spark.executor

import java.nio.ByteBuffer

import org.apache.spark.TaskState.TaskState

/**
 * A pluggable interface used by the Executor to send updates to the cluster scheduler.
一、deploy包的ExecutorRunner产生命令文件,单独一个进程去启动CoarseGrainedExecutorBackend
二、CoarseGrainedExecutorBackend有两个意义
1.创建executor去执行具体的task任务
2.与driver连接通信,传递一些统计信息,统计信息在statusUpdate方法中实现。
 */
private[spark] trait ExecutorBackend {

  //每一个任务会自动上报给driver端该任务上的统计信息,因此在executorBackend上做了一个公共方法
  def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer)
}

