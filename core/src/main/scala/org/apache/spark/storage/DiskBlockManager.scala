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

import java.util.UUID
import java.io.{IOException, File}

import org.apache.spark.{SparkConf, Logging}
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.util.{ShutdownHookManager, Utils}

/**
 * Creates and maintains the logical mapping between logical blocks and physical on-disk
 * locations. By default, one block is mapped to one file with a name given by its BlockId.
 * However, it is also possible to have a block map to only a segment of a file, by calling
 * mapBlockToFileSegment().
 *
 * Block files are hashed among the directories listed in spark.local.dir (or in
 * SPARK_LOCAL_DIRS, if it's set).
 * 磁盘数据块管理,该对象仅仅是管理数据块存储的File之间的关系,并不会真的产生写入文件操作
 */
private[spark] class DiskBlockManager(blockManager: BlockManager, conf: SparkConf)
  extends Logging {

  private[spark]
  val subDirsPerLocalDir = blockManager.conf.getInt("spark.diskStore.subDirectories", 64) //每一个目录可以存储多少个子目录

  /* Create one local directory for each path mentioned in spark.local.dir; then, inside this
   * directory, create multiple subdirectories that we will hash files into, in order to avoid
   * having really large inodes at the top level. 
   *path/blockmgr目录数组 
   **/
  private[spark] val localDirs: Array[File] = createLocalDirs(conf)

  /**
   * Create local directories for storing block data. These directories are
   * located inside configured local directories and won't
   * be deleted on JVM exit when using the external shuffle service.
   * 创建path/blockmgr目录数组,可以多个磁盘下创建该目录
   */
  private def createLocalDirs(conf: SparkConf): Array[File] = {
    Utils.getConfiguredLocalDirs(conf).flatMap { rootDir =>
      try {
        val localDir = Utils.createDirectory(rootDir, "blockmgr")
        logInfo(s"Created local directory at $localDir")
        Some(localDir)
      } catch {
        case e: IOException =>
          logError(s"Failed to create local dir in $rootDir. Ignoring this directory.", e)
          None
      }
    }
  }

  //如果没有存储的本地目录,则打印日志后,停止程序运行
  if (localDirs.isEmpty) {
    logError("Failed to create any local dir.")
    System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR)
  }

  // The content of subDirs is immutable but the content of subDirs(i) is mutable. And the content
  // of subDirs(i) is protected by the lock of subDirs(i)
  //每一个root目录下有64个子目录,Array[Array[File]] 第一级数组表示根磁盘,第二级目录是真正存储目录
  private val subDirs = Array.fill(localDirs.length)(new Array[File](subDirsPerLocalDir))

  //添加钩子
  private val shutdownHook = addShutdownHook()

  /** Looks up a file by hashing it into one of our local subdirectories. */
  // This method should be kept in sync with
  // org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getFile().
  //获取文件name应该在哪个磁盘上存储,返回path/filename
  def getFile(filename: String): File = {
    // Figure out which local directory it hashes to, and which subdirectory in that
    val hash = Utils.nonNegativeHash(filename) //根据name获取hash
    val dirId = hash % localDirs.length //根据hash可以获取该name存储在哪个磁盘上
    val subDirId = (hash / localDirs.length) % subDirsPerLocalDir //存储在哪个目录下

    // Create the subdirectory if it doesn't already exist 获取或者创建子目录
    val subDir = subDirs(dirId).synchronized {
      val old = subDirs(dirId)(subDirId)//先看看该目录是否有内容
      if (old != null) {//说明有内容
        old//返回老的内容
      } else {
        val newDir = new File(localDirs(dirId), "%02x".format(subDirId)) //%02x 表示将整数转换成16进制数
        if (!newDir.exists() && !newDir.mkdir()) {//创建新目录
          throw new IOException(s"Failed to create local dir in $newDir.")
        }
        subDirs(dirId)(subDirId) = newDir
        newDir
      }
    }

    //在目录下创建文件名的文件
    new File(subDir, filename)
  }

  //返回该数据块应该存储的路径
  def getFile(blockId: BlockId): File = getFile(blockId.name)

  /** Check if disk block manager has a block.判断文件是否真的存在*/
  def containsBlock(blockId: BlockId): Boolean = {
    getFile(blockId.name).exists()
  }

  /** List all the files currently stored on disk by the disk manager. */
  def getAllFiles(): Seq[File] = {
    // Get all the files inside the array of array of directories
    subDirs.flatMap { dir =>
      dir.synchronized {
        // Copy the content of dir because it may be modified in other threads
        dir.clone()
      }
    }.filter(_ != null).flatMap { dir =>
      val files = dir.listFiles()
      if (files != null) files else Seq.empty
    }
  }

  /** List all the blocks currently stored on disk by the disk manager. */
  def getAllBlocks(): Seq[BlockId] = {
    getAllFiles().map(f => BlockId(f.getName))
  }

  /** Produces a unique block id and File suitable for storing local intermediate results.
   *  创建临时数据块,返回被创建的临时数据块对象以及该对象要存储在什么路径下
   **/
  def createTempLocalBlock(): (TempLocalBlockId, File) = {
    var blockId = new TempLocalBlockId(UUID.randomUUID())
    while (getFile(blockId).exists()) {//直到找到不存在的临时数据块ID
      blockId = new TempLocalBlockId(UUID.randomUUID())
    }
    (blockId, getFile(blockId))
  }

  /** Produces a unique block id and File suitable for storing shuffled intermediate results.
   *  创建临时数据块,返回被创建的临时数据块对象以及该对象要存储在什么路径下
   **/
  def createTempShuffleBlock(): (TempShuffleBlockId, File) = {
    var blockId = new TempShuffleBlockId(UUID.randomUUID())
    while (getFile(blockId).exists()) {//直到找到不存在的临时数据块ID
      blockId = new TempShuffleBlockId(UUID.randomUUID())
    }
    (blockId, getFile(blockId))
  }

  //设置钩子
  private def addShutdownHook(): AnyRef = {
    ShutdownHookManager.addShutdownHook(ShutdownHookManager.TEMP_DIR_SHUTDOWN_PRIORITY + 1) { () =>
      logInfo("Shutdown hook called")
      DiskBlockManager.this.doStop()
    }
  }

  /** Cleanup local dirs and stop shuffle sender. */
  private[spark] def stop() {
    // Remove the shutdown hook.  It causes memory leaks if we leave it around.
    try {
      ShutdownHookManager.removeShutdownHook(shutdownHook)
    } catch {
      case e: Exception =>
        logError(s"Exception while removing shutdown hook.", e)
    }
    doStop()
  }

  private def doStop(): Unit = {
    // Only perform cleanup if an external service is not serving our shuffle files.
    // Also blockManagerId could be null if block manager is not initialized properly.
    if (!blockManager.externalShuffleServiceEnabled ||
      (blockManager.blockManagerId != null && blockManager.blockManagerId.isDriver)) {
      localDirs.foreach { localDir =>
        if (localDir.isDirectory() && localDir.exists()) {
          try {
            if (!ShutdownHookManager.hasRootAsShutdownDeleteDir(localDir)) {
              Utils.deleteRecursively(localDir)
            }
          } catch {
            case e: Exception =>
              logError(s"Exception while deleting local spark dir: $localDir", e)
          }
        }
      }
    }
  }
}
