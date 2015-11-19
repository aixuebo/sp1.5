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

package org.apache.spark.deploy

/**
 * Used to send state on-the-wire about Executors from Worker to Master.
 * This state is sufficient for the Master to reconstruct its internal data structures during
 * failover.
 * 从Worker发送给master,关于该Executor的状态
 * 在失败过程中,这些状态可以满足master去重建内部数据结构
 * 
 * 包含:哪个apii使用多少cpu,执行到什么状态了
 */
private[deploy] class ExecutorDescription(
    val appId: String,
    val execId: Int,
    val cores: Int,
    val state: ExecutorState.Value)
  extends Serializable {

  override def toString: String =
    "ExecutorState(appId=%s, execId=%d, cores=%d, state=%s)".format(appId, execId, cores, state)
}
