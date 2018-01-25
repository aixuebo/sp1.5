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

package org.apache.spark.shuffle

import org.apache.spark.{TaskContext, ShuffleDependency}

/**
 * Pluggable interface for shuffle systems. A ShuffleManager is created in SparkEnv on the driver
 * and on each executor, based on the spark.shuffle.manager setting. The driver registers shuffles
 * with it, and executors (or tasks running locally in the driver) can ask to read and write data.
 *
 * NOTE: this will be instantiated by SparkEnv so its constructor can take a SparkConf and
 * boolean isDriver as parameters.
 * 该对象是在一个节点上只初始化一次,即在SparkEnv里面进行初始化的
 */
private[spark] trait ShuffleManager {
  /**
   * Register a shuffle with the manager and obtain a handle for it to pass to tasks.
   * 注册一个shuffle信息,产生一个注册对象ShuffleHandle
   */
  def registerShuffle[K, V, C](
      shuffleId: Int,//shuffle的唯一ID
      numMaps: Int,//RDD的partiton数量
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle //该rdd最终被分发成多少个reduce等操作记录

  /** Get a writer for a given partition. Called on executors by map tasks.
    *   map任务写入数据到多个reduce文件中
    **/
  def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext): ShuffleWriter[K, V]

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
   * reduce要获取一组map区间的输出
   */
  def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C]

  /**
    * Remove a shuffle's metadata from the ShuffleManager.
    * @return true if the metadata removed successfully, otherwise false.
    * 删除一个shuffle,即该shuffle完成了
    */
  def unregisterShuffle(shuffleId: Int): Boolean

  /**
   * Return a resolver capable of retrieving shuffle block data based on block coordinates.
   * 如何获取shuffle的结果
   */
  def shuffleBlockResolver: ShuffleBlockResolver

  /** Shut down this ShuffleManager.
    * 停止该shuffle过程
    **/
  def stop(): Unit
}
