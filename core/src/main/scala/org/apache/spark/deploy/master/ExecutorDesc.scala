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

package org.apache.spark.deploy.master

import org.apache.spark.deploy.{ExecutorDescription, ExecutorState}

//表示一个应用众多执行者中的一个执行者
private[master] class ExecutorDesc(
    val id: Int,//执行者唯一ID
    val application: ApplicationInfo,//该执行者所属的应用
    val worker: WorkerInfo,//该执行者在哪个节点执行
    val cores: Int,//该执行者所需要的cpu
    val memory: Int) {//该执行者所需要的内存

  var state = ExecutorState.LAUNCHING //该执行者的状态

  /** Copy all state (non-val) variables from the given on-the-wire ExecutorDescription. */
  def copyState(execDesc: ExecutorDescription) {
    state = execDesc.state
  }

  //该执行者的全ID,即由应用id+该执行者唯一id组成
  def fullId: String = application.id + "/" + id

  override def equals(other: Any): Boolean = {
    other match {
      case info: ExecutorDesc =>
        fullId == info.fullId &&
        worker.id == info.worker.id &&
        cores == info.cores &&
        memory == info.memory
      case _ => false
    }
  }

  override def toString: String = fullId

  override def hashCode: Int = toString.hashCode()
}
