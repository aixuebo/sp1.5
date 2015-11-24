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

package org.apache.spark.network

import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.storage.{BlockId, StorageLevel}

//该类可以帮助我们抓取本地数据块以及存储数据块到本地
private[spark]
trait BlockDataManager {

  /**
   * Interface to get local block data. Throws an exception if the block cannot be found or
   * cannot be read successfully.
   * 通过数据块ID,获取本地的数据块对象
   */
  def getBlockData(blockId: BlockId): ManagedBuffer

  /**
   * Put the block locally, using the given storage level.
   * 将数据块ID与数据块内容存储到本地
   */
  def putBlockData(blockId: BlockId, data: ManagedBuffer, level: StorageLevel): Unit
}
