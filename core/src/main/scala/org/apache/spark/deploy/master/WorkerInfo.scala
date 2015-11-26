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

import scala.collection.mutable

import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.Utils

private[spark] class WorkerInfo(
    val id: String,//worker唯一标示
    val host: String,//workder所在节点host
    val port: Int,//worker所在节点port
    val cores: Int,//worker提供多少cpu可以被使用
    val memory: Int,//worker提供多少内存可以被使用
    val endpoint: RpcEndpointRef,//与该workder交互的流,即master服务器可以通过该对象与workder进行通信
    val webUiPort: Int,//worker提供的webui所在端口
    val publicAddress: String)//该worker对外提供的host,通过host:webUiPort可以访问该workder的webUi
  extends Serializable {

  Utils.checkHost(host, "Expected hostname")
  assert (port > 0)

  //key是该worker上执行的执行者ExecutorDesc.fullId,value是执行者对象
  @transient var executors: mutable.HashMap[String, ExecutorDesc] = _ // executorId => info
  
  //key是DriverInfo.id,value是DriverInfo对象,表示在该worker上要运行的driver集合
  @transient var drivers: mutable.HashMap[String, DriverInfo] = _ // driverId => info
  
  @transient var state: WorkerState.Value = _ ////该worker服务器的状态
  @transient var coresUsed: Int = _//该worker服务器已经使用了多少个cpu
  @transient var memoryUsed: Int = _//该worker服务器已经使用了多少内存

  @transient var lastHeartbeat: Long = _ //该worker最后一次心跳时间

  init()

  def coresFree: Int = cores - coresUsed //该worker目前还剩余多少cpu可用
  def memoryFree: Int = memory - memoryUsed //该worker目前还剩余多少内存可用

  private def readObject(in: java.io.ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    init()
  }

  private def init() {
    executors = new mutable.HashMap
    drivers = new mutable.HashMap
    state = WorkerState.ALIVE
    coresUsed = 0
    memoryUsed = 0
    lastHeartbeat = System.currentTimeMillis()
  }

  //该worker对应的host:port
  def hostPort: String = {
    assert (port > 0)
    host + ":" + port
  }

  //在该worker上执行该执行者
  def addExecutor(exec: ExecutorDesc) {
    executors(exec.fullId) = exec
    coresUsed += exec.cores
    memoryUsed += exec.memory
  }

  //移除该worker上的一个执行者
  def removeExecutor(exec: ExecutorDesc) {
    if (executors.contains(exec.fullId)) {
      executors -= exec.fullId
      coresUsed -= exec.cores
      memoryUsed -= exec.memory
    }
  }

  //true表示该workder上执行该app
  def hasExecutor(app: ApplicationInfo): Boolean = {
    executors.values.exists(_.application == app)
  }

  /**
   * Master的launchDriver方法调用该函数
   * 表示在schedule方法里面,当一个worker的内存和cpu满足一个driver,则在该worker上启动该driver
   */
  def addDriver(driver: DriverInfo) {
    drivers(driver.id) = driver
    //累加被使用的资源
    memoryUsed += driver.desc.mem
    coresUsed += driver.desc.cores
  }

  /**
   * master的removeDriver方法调用该函数
   * 回收该内存和cpu
   */
  def removeDriver(driver: DriverInfo) {
    drivers -= driver.id
    memoryUsed -= driver.desc.mem
    coresUsed -= driver.desc.cores
  }

  //该worker上的ui地址
  def webUiAddress : String = {
    "http://" + this.publicAddress + ":" + this.webUiPort
  }

  def setState(state: WorkerState.Value): Unit = {
    this.state = state
  }

  //true表示该workder现在是活着的
  def isAlive(): Boolean = this.state == WorkerState.ALIVE
}
