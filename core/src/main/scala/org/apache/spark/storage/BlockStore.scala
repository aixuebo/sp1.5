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

package org.apache.spark.storage

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Logging

/**
 * Abstract class to store blocks.
 * 抽象类,专门用于定义存储接口,实现类有内存存储和磁盘存储
 */
private[spark] abstract class BlockStore(val blockManager: BlockManager) extends Logging {

  //存储一个数据块,包括数据块ID,数据块的内容,以及存储的级别
  def putBytes(blockId: BlockId, bytes: ByteBuffer, level: StorageLevel): PutResult

  /**
   * Put in a block and, possibly, also return its content as either bytes or another Iterator.
   * This is used to efficiently write the values to multiple locations (e.g. for replication).
   *
   * @return a PutResult that contains the size of the data, as well as the values put if
   *         returnValues is true (if not, the result's data field can be null)
   *         
   * 存储一个数据块,该数据块内容是一组迭代的数据
   * returnValues = true,表示是否将文件内容返回到PutResult中
   */
  def putIterator(
    blockId: BlockId,
    values: Iterator[Any],
    level: StorageLevel,
    returnValues: Boolean): PutResult

  def putArray(
    blockId: BlockId,
    values: Array[Any],
    level: StorageLevel,
    returnValues: Boolean): PutResult

  /**
   * Return the size of a block in bytes.
   * 返回制定数据块的大小
   */
  def getSize(blockId: BlockId): Long

  //获取该数据块的内容
  def getBytes(blockId: BlockId): Option[ByteBuffer]

  //获取该数据块的内容,该内容是可以迭代的value值
  def getValues(blockId: BlockId): Option[Iterator[Any]]

  /**
   * Remove a block, if it exists.
   * @param blockId the block to remove.
   * @return True if the block was found and removed, False otherwise.
   * 删除一个数据块
   */
  def remove(blockId: BlockId): Boolean

  //判断是否包含该数据块
  def contains(blockId: BlockId): Boolean

  //清空所有数据块上
  def clear() { }
}
