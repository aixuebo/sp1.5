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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rpc.RpcEnv

import scala.reflect.ClassTag

/**
 * Allows Master to persist any state that is necessary in order to recover from a failure.
 * The following semantics are required:
 *   - addApplication and addWorker are called before completing registration of a new app/worker.
 *   - removeApplication and removeWorker are called at any time.
 * Given these two requirements, we will have all apps and workers persisted, but
 * we might not have yet deleted apps or workers that finished (so their liveness must be verified
 * during recovery).
 *
 * The implementation of this trait defines how name-object pairs are stored or retrieved.
 */
@DeveloperApi
abstract class PersistenceEngine {

  /**
   * Defines how the object is serialized and persisted. Implementation will
   * depend on the store used.
   * 存储object对象内容,存储的标示是name
   */
  def persist(name: String, obj: Object)

  /**
   * Defines how the object referred by its name is removed from the store.
   * 删除key为name对应的存储信息
   */
  def unpersist(name: String)

  /**
   * Gives all objects, matching a prefix. This defines how objects are
   * read/deserialized back.
   * 将key以参数prefix开头的key,反序列化成对象集合
   */
  def read[T: ClassTag](prefix: String): Seq[T]

  //添加一个应用,应用的名字以app_开头
  final def addApplication(app: ApplicationInfo): Unit = {
    persist("app_" + app.id, app)
  }

  //移除一个应用,应用的名字以app_开头
  final def removeApplication(app: ApplicationInfo): Unit = {
    unpersist("app_" + app.id)
  }

  //添加一个workder,workder的名字以worker_开头
  final def addWorker(worker: WorkerInfo): Unit = {
    persist("worker_" + worker.id, worker)
  }

  //移除一个worker,workder的名字以worker_开头
  final def removeWorker(worker: WorkerInfo): Unit = {
    unpersist("worker_" + worker.id)
  }

  //添加一个driver,driver的名字以driver开头
  final def addDriver(driver: DriverInfo): Unit = {
    persist("driver_" + driver.id, driver)
  }

  //移除一个driver,driver的名字以driver开头
  final def removeDriver(driver: DriverInfo): Unit = {
    unpersist("driver_" + driver.id)
  }

  /**
   * Returns the persisted data sorted by their respective ids (which implies that they're
   * sorted by time of creation).
   * 读取所有数据,分别返回三组集合
   */
  final def readPersistedData(
      rpcEnv: RpcEnv): (Seq[ApplicationInfo], Seq[DriverInfo], Seq[WorkerInfo]) = {
    rpcEnv.deserialize { () =>
      (read[ApplicationInfo]("app_"), read[DriverInfo]("driver_"), read[WorkerInfo]("worker_"))
    }
  }

  def close() {}
}

//空实现的方案
private[master] class BlackHolePersistenceEngine extends PersistenceEngine {

  override def persist(name: String, obj: Object): Unit = {}

  override def unpersist(name: String): Unit = {}

  override def read[T: ClassTag](name: String): Seq[T] = Nil

}
