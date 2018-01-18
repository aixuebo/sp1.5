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

package org.apache.spark.shuffle.hash

import org.apache.spark._
import org.apache.spark.shuffle._

/**
 * A ShuffleManager using hashing, that creates one output file per reduce partition on each
 * mapper (possibly reusing these across waves of tasks).
 * 在一个mapper上,为每一个reduce都创建一个输出文件
 */
private[spark] class HashShuffleManager(conf: SparkConf) extends ShuffleManager {

  //使用文件的方式存储map的输出内容
  private val fileShuffleBlockResolver = new FileShuffleBlockResolver(conf)

  /* Register a shuffle with the manager and obtain a handle for it to pass to tasks.
   * 注册一个shuffle信息,产生一个注册对象ShuffleHandle
    **/
  override def registerShuffle[K, V, C](
      shuffleId: Int,//shuffle唯一的ID
      numMaps: Int,//父RDD一共多少个partition
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {//ShuffleDependency表示子类依赖的对象
    new BaseShuffleHandle(shuffleId, numMaps, dependency)
  }

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
   * reduce要获取一组map区间的输出
   */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C] = {
    new HashShuffleReader(
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]], startPartition, endPartition, context)
  }

  /** Get a writer for a given partition. Called on executors by map tasks.
    * map任务写入数据到多个reduce文件中
    * mapId 表示原始RDD的第几个partition
    **/
  override def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext)
      : ShuffleWriter[K, V] = {
    new HashShuffleWriter(
      shuffleBlockResolver, handle.asInstanceOf[BaseShuffleHandle[K, V, _]], mapId, context)
  }

  /** Remove a shuffle's metadata from the ShuffleManager.
    * 删除一个shuffle,即该shuffle完成了
    **/
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    shuffleBlockResolver.removeShuffle(shuffleId)
  }

  override def shuffleBlockResolver: FileShuffleBlockResolver = {
    fileShuffleBlockResolver
  }

  /** Shut down this ShuffleManager.
    * 停止该shuffle过程
    **/
  override def stop(): Unit = {
    shuffleBlockResolver.stop()
  }
}
