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

package org.apache.spark.broadcast

import scala.reflect.ClassTag

import org.apache.spark.SecurityManager
import org.apache.spark.SparkConf
import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * An interface for all the broadcast implementations in Spark (to allow
 * multiple broadcast implementations). SparkContext uses a user-specified
 * BroadcastFactory implementation to instantiate a particular broadcast for the
 * entire Spark job.
 */
@DeveloperApi
trait BroadcastFactory {

  //初始化工厂
  def initialize(isDriver: Boolean, conf: SparkConf, securityMgr: SecurityManager): Unit

  /**
   * Creates a new broadcast variable.
   *
   * @param value value to broadcast 要广播的对象
   * @param isLocal whether we are in local mode (single JVM process)
   * @param id unique id representing this broadcast variable 为广播生成一个唯一编号
   * 产生一个新的广播对象
   */
  def newBroadcast[T: ClassTag](value: T, isLocal: Boolean, id: Long): Broadcast[T]

  /**
   * 对已经存在的广播对象进行销毁或者暂时删除
   * @id 准备要处理哪个广播对象
   * @removeFromDriver 是销毁还是暂时删除
   * @blocking 是阻塞的还是非阻塞的
   */
  def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit

  //停止工厂
  def stop(): Unit
}
