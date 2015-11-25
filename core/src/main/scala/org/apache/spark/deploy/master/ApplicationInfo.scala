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

import java.util.Date

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.deploy.ApplicationDescription
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.Utils

//该对象表示一个应用,一个driver可以产生多个应用,但是一个应用会产生多个执行者执行任务
private[spark] class ApplicationInfo(
    val startTime: Long,//任务开启时间
    val id: String,//任务ID
    val desc: ApplicationDescription,//任务描述
    val submitDate: Date,//任务提交时间
    val driver: RpcEndpointRef,//与该应用交互的流,即master服务器可以通过该driver对应的application进行通信
    defaultCores: Int)//默认该应用最多可以获取多少CPU
  extends Serializable {

  @transient var state: ApplicationState.Value = _//该应用当时的状态
  //当前应用存在的执行者集合,key是执行者ID,value是对应的执行者对象
  @transient var executors: mutable.HashMap[Int, ExecutorDesc] = _
  //刚刚被移除掉的执行者集合
  @transient var removedExecutors: ArrayBuffer[ExecutorDesc] = _
  @transient var coresGranted: Int = _ //当前应用授予所有执行者对应的总cpu数量
  @transient var endTime: Long = _//应用完成的时间
  @transient var appSource: ApplicationSource = _

  // A cap on the number of executors this application can have at any given time.
  // By default, this is infinite. Only after the first allocation request is issued by the
  // application will this be set to a finite value. This is used for dynamic allocation.
  //仅仅测试的时候使用,可忽略
  @transient private[master] var executorLimit: Int = _

  @transient private var nextExecutorId: Int = _ //该应用下一个执行者的ID

  init()

  private def readObject(in: java.io.ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    init()
  }

  private def init() {
    state = ApplicationState.WAITING
    executors = new mutable.HashMap[Int, ExecutorDesc]
    coresGranted = 0
    endTime = -1L
    appSource = new ApplicationSource(this)
    nextExecutorId = 0
    removedExecutors = new ArrayBuffer[ExecutorDesc]
    executorLimit = Integer.MAX_VALUE
  }

  //设置执行者ID
  //如果参数存在,则本次执行者ID就是参数值,如果不存在,则id是自增长的
  private def newExecutorId(useID: Option[Int] = None): Int = {
    useID match {
      case Some(id) =>
        nextExecutorId = math.max(nextExecutorId, id + 1) //重新设置nextExecutorId为id+1,即本次使用的是id,下一次使用的就是id+1
        id
      case None =>
        val id = nextExecutorId //自增长获取执行者ID
        nextExecutorId += 1
        id
    }
  }

  //添加一个执行者
  private[master] def addExecutor(
      worker: WorkerInfo,//该执行者在哪个节点执行
      cores: Int,//该执行者所需要的cpu
      useID: Option[Int] = None): ExecutorDesc = {//该执行者默认的唯一标示执行者ID
    val exec = new ExecutorDesc(newExecutorId(useID), this, worker, cores, desc.memoryPerExecutorMB) //创建执行者对象
    executors(exec.id) = exec
    coresGranted += cores
    exec
  }

  //移除该application上的一个进程
  private[master] def removeExecutor(exec: ExecutorDesc) {
    if (executors.contains(exec.id)) {
      removedExecutors += executors(exec.id)
      executors -= exec.id
      coresGranted -= exec.cores
    }
  }

  //该应用最大允许请求多少个cpu
  private val requestedCores = desc.maxCores.getOrElse(defaultCores)

  //该应用还可以请求多少个cpu
  private[master] def coresLeft: Int = requestedCores - coresGranted

  //尝试次数
  private var _retryCount = 0
  private[master] def retryCount = _retryCount
  private[master] def incrementRetryCount() = {//增加尝试次数
    _retryCount += 1
    _retryCount
  }
  private[master] def resetRetryCount() = _retryCount = 0 //重置尝试次数

  //标记完成状态以及完成时间
  private[master] def markFinished(endState: ApplicationState.Value) {
    state = endState
    endTime = System.currentTimeMillis()
  }

  //true表示已经完成
  private[master] def isFinished: Boolean = {
    state != ApplicationState.WAITING && state != ApplicationState.RUNNING
  }

  /**
   * Return the limit on the number of executors this application can have.
   * For testing only.
   * 仅仅测试的时候使用
   */
  private[deploy] def getExecutorLimit: Int = executorLimit

  //任务花费时间
  def duration: Long = {
    if (endTime != -1) {
      endTime - startTime
    } else {
      System.currentTimeMillis() - startTime
    }
  }

}
