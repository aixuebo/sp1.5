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

package org.apache.spark.scheduler.cluster

import org.apache.spark.rpc.{RpcEndpointRef, RpcAddress}

/**
 * Grouping of data for an executor used by CoarseGrainedSchedulerBackend.
 *
 * @param executorEndpoint The RpcEndpointRef representing this executor
 * @param executorAddress The network address of this executor
 * @param executorHost The hostname that this executor is running on
 * @param freeCores  The current number of cores available for work on the executor
 * @param totalCores The total number of cores available to the executor
 * driver上描述一个executor的对象
 */
private[cluster] class ExecutorData(
   val executorEndpoint: RpcEndpointRef,//executor的通信对象
   val executorAddress: RpcAddress,//executor的通信地址host:port
   override val executorHost: String,//仅仅包含executor的host,即知道该executor运行在哪个节点上
   var freeCores: Int,//目前空闲多少个cpu
   override val totalCores: Int,//总共该executor分配了多少个cpu
   override val logUrlMap: Map[String, String]//executor提供的属性集合
) extends ExecutorInfo(executorHost, totalCores, logUrlMap)
