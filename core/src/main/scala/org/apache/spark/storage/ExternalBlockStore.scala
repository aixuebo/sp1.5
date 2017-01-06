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

import scala.util.control.NonFatal

import org.apache.spark.Logging
import org.apache.spark.util.Utils


/**
 * Stores BlockManager blocks on ExternalBlockStore.
 * We capture any potential exception from underlying implementation
 * and return with the expected failure value
 */
private[spark] class ExternalBlockStore(blockManager: BlockManager, executorId: String)
  extends BlockStore(blockManager: BlockManager) with Logging {

  lazy val externalBlockManager: Option[ExternalBlockManager] = createBlkManager()

  logInfo("ExternalBlockStore started")

  override def getSize(blockId: BlockId): Long = {
    try {
      externalBlockManager.map(_.getSize(blockId)).getOrElse(0)
    } catch {
      case NonFatal(t) =>
        logError(s"Error in getSize($blockId)", t)
        0L
    }
  }

  override def putBytes(blockId: BlockId, bytes: ByteBuffer, level: StorageLevel): PutResult = {
    putIntoExternalBlockStore(blockId, bytes, returnValues = true)
  }

  override def putArray(
      blockId: BlockId,
      values: Array[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {
    putIntoExternalBlockStore(blockId, values.toIterator, returnValues)
  }

  override def putIterator(
      blockId: BlockId,
      values: Iterator[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {
    putIntoExternalBlockStore(blockId, values, returnValues)
  }

  //存储一组对象到外部存储中
  private def putIntoExternalBlockStore(
      blockId: BlockId,
      values: Iterator[_],
      returnValues: Boolean): PutResult = {
    logTrace(s"Attempting to put block $blockId into ExternalBlockStore")
    // we should never hit here if externalBlockManager is None. Handle it anyway for safety.
    try {
      val startTime = System.currentTimeMillis
      if (externalBlockManager.isDefined) {//定义了外部存储
        externalBlockManager.get.putValues(blockId, values) //将对象集合序列化成字节数组,然后存储
        val size = getSize(blockId)//返回存储后的字节大小
        val data = if (returnValues) {
          Left(getValues(blockId).get)//存储后的字节数组内容,要被返回
        } else {
          null
        }
        val finishTime = System.currentTimeMillis
        logDebug("Block %s stored as %s file in ExternalBlockStore in %d ms".format(
          blockId, Utils.bytesToString(size), finishTime - startTime))
        PutResult(size, data)
      } else {
        logError(s"Error in putValues($blockId): no ExternalBlockManager has been configured")
        PutResult(-1, null, Seq((blockId, BlockStatus.empty)))
      }
    } catch {
      case NonFatal(t) =>
        logError(s"Error in putValues($blockId)", t)
        PutResult(-1, null, Seq((blockId, BlockStatus.empty)))
    }
  }

  //存储一个数据块ID对应的数据块字节数组到外部存储中
  private def putIntoExternalBlockStore(
      blockId: BlockId,
      bytes: ByteBuffer,
      returnValues: Boolean): PutResult = {
    logTrace(s"Attempting to put block $blockId into ExternalBlockStore")
    // we should never hit here if externalBlockManager is None. Handle it anyway for safety.
    try {
      val startTime = System.currentTimeMillis
      if (externalBlockManager.isDefined) {//已经定义了外部存储
        val byteBuffer = bytes.duplicate()//数据块内容
        byteBuffer.rewind()
        externalBlockManager.get.putBytes(blockId, byteBuffer)//存储数据块
        val size = bytes.limit() //数据块内容所占用的字节大小
        val data = if (returnValues) {
          Right(bytes)//保存字节数组内容,作为返回值
        } else {
          null
        }
        val finishTime = System.currentTimeMillis
        logDebug("Block %s stored as %s file in ExternalBlockStore in %d ms".format(
          blockId, Utils.bytesToString(size), finishTime - startTime))
        PutResult(size, data)
      } else {
        logError(s"Error in putBytes($blockId): no ExternalBlockManager has been configured")
        PutResult(-1, null, Seq((blockId, BlockStatus.empty)))
      }
    } catch {
      case NonFatal(t) =>
        logError(s"Error in putBytes($blockId)", t)
        PutResult(-1, null, Seq((blockId, BlockStatus.empty)))
    }
  }

  // We assume the block is removed even if exception thrown
  //删除一个数据块,如果出现异常,我们假设我们成功删除了数据块
  override def remove(blockId: BlockId): Boolean = {
    try {
      externalBlockManager.map(_.removeBlock(blockId)).getOrElse(true)
    } catch {
      case NonFatal(t) =>
        logError(s"Error in removeBlock($blockId)", t)
        true
    }
  }

  //将该数据块对应的字节数组反序列化成对象集合
  override def getValues(blockId: BlockId): Option[Iterator[Any]] = {
    try {
      externalBlockManager.flatMap(_.getValues(blockId))//将该数据块对应的字节数组反序列化成对象集合
    } catch {
      case NonFatal(t) =>
        logError(s"Error in getValues($blockId)", t)
        None
    }
  }

  //获取该数据块对应的字节内容
  override def getBytes(blockId: BlockId): Option[ByteBuffer] = {
    try {
      externalBlockManager.flatMap(_.getBytes(blockId))
    } catch {
      case NonFatal(t) =>
        logError(s"Error in getBytes($blockId)", t)
        None
    }
  }

  override def contains(blockId: BlockId): Boolean = {
    try {
      val ret = externalBlockManager.map(_.blockExists(blockId)).getOrElse(false)//是否包含该数据块
      if (!ret) {
        logInfo(s"Remove block $blockId")
        blockManager.removeBlock(blockId, true) //不包含,则删除该数据块
      }
      ret
    } catch {
      case NonFatal(t) =>
        logError(s"Error in getBytes($blockId)", t)
        false
    }
  }

  private def addShutdownHook() {
    Runtime.getRuntime.addShutdownHook(new Thread("ExternalBlockStore shutdown hook") {
      override def run(): Unit = Utils.logUncaughtExceptions {
        logDebug("Shutdown hook called")
        externalBlockManager.map(_.shutdown())
      }
    })
  }

  // Create concrete block manager and fall back to Tachyon by default for backward compatibility.
  //创建外部存储实例
  private def createBlkManager(): Option[ExternalBlockManager] = {
    val clsName = blockManager.conf.getOption(ExternalBlockStore.BLOCK_MANAGER_NAME)
      .getOrElse(ExternalBlockStore.DEFAULT_BLOCK_MANAGER_NAME)//找到外部存储的class全路径

    try {
      val instance = Utils.classForName(clsName)
        .newInstance()
        .asInstanceOf[ExternalBlockManager]//实例化该外部存储
      instance.init(blockManager, executorId)
      addShutdownHook();
      Some(instance)
    } catch {
      case NonFatal(t) =>
        logError("Cannot initialize external block store", t)
        None
    }
  }
}

private[spark] object ExternalBlockStore extends Logging {
  val MAX_DIR_CREATION_ATTEMPTS = 10
  val SUB_DIRS_PER_DIR = "64"
  val BASE_DIR = "spark.externalBlockStore.baseDir"
  val FOLD_NAME = "spark.externalBlockStore.folderName"
  val MASTER_URL = "spark.externalBlockStore.url"
  val BLOCK_MANAGER_NAME = "spark.externalBlockStore.blockManager"//可以自己实现外部存储对象,这个是key,value就是具体的实现类的全路径
  val DEFAULT_BLOCK_MANAGER_NAME = "org.apache.spark.storage.TachyonBlockManager" //默认外部存储对象
}
