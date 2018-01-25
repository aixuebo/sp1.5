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

import java.io.File
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._

import org.apache.spark.{Logging, SparkConf, SparkEnv}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.FileShuffleBlockResolver.ShuffleFileGroup
import org.apache.spark.storage._
import org.apache.spark.util.{MetadataCleaner, MetadataCleanerType, TimeStampedHashMap}
import org.apache.spark.util.collection.{PrimitiveKeyOpenHashMap, PrimitiveVector}

/** A group of writers for a ShuffleMapTask, one writer per reducer. */
private[spark] trait ShuffleWriterGroup {
  val writers: Array[DiskBlockObjectWriter]//每一个reduce对应一个DiskBlockObjectWriter对象

  /** @param success Indicates all writes were successful. If false, no blocks will be recorded. */
  def releaseWriters(success: Boolean)
}

/**
 * Manages assigning disk-based block writers to shuffle tasks. Each shuffle task gets one file
 * per reducer (this set of files is called a ShuffleFileGroup).
 *
 * As an optimization to reduce the number of physical shuffle files produced, multiple shuffle
 * blocks are aggregated into the same file. There is one "combined shuffle file" per reducer
 * per concurrently executing shuffle task. As soon as a task finishes writing to its shuffle
 * files, it releases them for another task.
 * Regarding the implementation of this feature, shuffle files are identified by a 3-tuple:
 *   - shuffleId: The unique id given to the entire shuffle stage.
 *   - bucketId: The id of the output partition (i.e., reducer id)
 *   - fileId: The unique id identifying a group of "combined shuffle files." Only one task at a
 *       time owns a particular fileId, and this id is returned to a pool when the task finishes.
 * Each shuffle file is then mapped to a FileSegment, which is a 3-tuple (file, offset, length)
 * that specifies where in a given file the actual block data is located.
 *
 * Shuffle file metadata is stored in a space-efficient manner. Rather than simply mapping
 * ShuffleBlockIds directly to FileSegments, each ShuffleFileGroup maintains a list of offsets for
 * each block stored in each file. In order to find the location of a shuffle block, we search the
 * files within a ShuffleFileGroups associated with the block's reducer.
 *
 * 用于管理mapshuffle时候,每一个map给每一个reduce分配一个文件,或者每一个reduce公用同一个文件
 *
 * 难点:
FileShuffleBlockResolver 优化的时候不是所有的mapper在一个节点上都只会产生N个reduce文件。而是如果多个mapper是非同一时间在节点上执行时候,那么会公用同一套N个reduce文件


 */
// Note: Changes to the format in this file should be kept in sync with
// org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getHashBasedShuffleBlockData().
private[spark] class FileShuffleBlockResolver(conf: SparkConf)
  extends ShuffleBlockResolver with Logging {

  private val transportConf = SparkTransportConf.fromSparkConf(conf) //获取传输相关的配置信息

  private lazy val blockManager = SparkEnv.get.blockManager

  // Turning off shuffle file consolidation causes all shuffle Blocks to get their own file.
  // TODO: Remove this once the shuffle file consolidation feature is stable.
  private val consolidateShuffleFiles =
    conf.getBoolean("spark.shuffle.consolidateFiles", false)//true表示每一个map对应的reduce都要产生一个文件,false表示reduce可以公用同一个文件

  // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
  private val bufferSize = conf.getSizeAsKb("spark.shuffle.file.buffer", "32k").toInt * 1024

  /**
   * Contains all the state related to a particular shuffle. This includes a pool of unused
   * ShuffleFileGroups, as well as all ShuffleFileGroups that have been created for the shuffle.
   * 参数numBuckets表示结果一共多少个reduce
   */
  private class ShuffleState(val numBuckets: Int) {
    val nextFileId = new AtomicInteger(0)
    val unusedFileGroups = new ConcurrentLinkedQueue[ShuffleFileGroup]()//回收的文件组集合---即该文件内容是可以参与共享的reduce
    val allFileGroups = new ConcurrentLinkedQueue[ShuffleFileGroup]()//所有的文件组

    /**
     * The mapIds of all map tasks completed on this Executor for this shuffle.
     * NB: This is only populated if consolidateShuffleFiles is FALSE. We don't need it otherwise.
     * 表示已经完成的map集合
     */
    val completedMapTasks = new ConcurrentLinkedQueue[Int]()
  }

  //每一个shuffleId对应一个该shuffle的状态对象
  private val shuffleStates = new TimeStampedHashMap[ShuffleId, ShuffleState]

  private val metadataCleaner =
    new MetadataCleaner(MetadataCleanerType.SHUFFLE_BLOCK_MANAGER, this.cleanup, conf)

  /**
   * Get a ShuffleWriterGroup for the given map task, which will register it as complete
   * when the writers are closed successfully
   * shuffleId 表示是哪个shuffler
   * mapId 表示原始RDD的第几个partition
   * numBuckets 表示一共多少个partition,此时partition是目标reduce的数量
   * serializer 表示如何序列化数据
   */
  def forMapTask(shuffleId: Int, mapId: Int, numBuckets: Int, serializer: Serializer,
      writeMetrics: ShuffleWriteMetrics): ShuffleWriterGroup = {
    new ShuffleWriterGroup {
      shuffleStates.putIfAbsent(shuffleId, new ShuffleState(numBuckets)) //如果不存在,则添加
      private val shuffleState = shuffleStates(shuffleId) //获取状态对象
      private var fileGroup: ShuffleFileGroup = null

      val openStartTime = System.nanoTime
      val serializerInstance = serializer.newInstance()
      val writers: Array[DiskBlockObjectWriter] = if (consolidateShuffleFiles) {//共用同一个文件
        fileGroup = getUnusedFileGroup()
        Array.tabulate[DiskBlockObjectWriter](numBuckets) { bucketId => //第几个reduce文件
          val blockId = ShuffleBlockId(shuffleId, mapId, bucketId)//创建一个blockId
          blockManager.getDiskWriter(blockId, fileGroup(bucketId),//每一个reduce文件固定输出到一个文件中
            serializerInstance, bufferSize,
            writeMetrics)
        }
      } else {//为每一个reduce产生一个文件
        Array.tabulate[DiskBlockObjectWriter](numBuckets) { bucketId =>
          val blockId = ShuffleBlockId(shuffleId, mapId, bucketId) //创建一个blockId
          val blockFile = blockManager.diskBlockManager.getFile(blockId) //获取该数据块存储的本地路径
          // Because of previous failures, the shuffle file may already exist on this machine.
          // If so, remove it.
          if (blockFile.exists) {
            if (blockFile.delete()) {
              logInfo(s"Removed existing shuffle file $blockFile")
            } else {
              logWarning(s"Failed to remove existing shuffle file $blockFile")
            }
          }
          blockManager.getDiskWriter(blockId, blockFile, serializerInstance, bufferSize,
            writeMetrics)//为该文件创建了一个DiskBlockObjectWriter对象,该对象可以向文件内容中写入数据
        }
      }
      // Creating the file to write to and creating a disk writer both involve interacting with
      // the disk, so should be included in the shuffle write time.
      writeMetrics.incShuffleWriteTime(System.nanoTime - openStartTime) //统计shuffle时间

      override def releaseWriters(success: Boolean) {
        if (consolidateShuffleFiles) {//公用一个文件
          if (success) {
            //属于在该map中属于该reduce的输出开始位置以及总长度
            val offsets = writers.map(_.fileSegment().offset)//这是一组集合,size大小就是reduce的数量
            val lengths = writers.map(_.fileSegment().length)//这是一组集合,size大小就是reduce的数量
            fileGroup.recordMapOutput(mapId, offsets, lengths)
          }
          recycleFileGroup(fileGroup)
        } else {
          shuffleState.completedMapTasks.add(mapId)
        }
      }

      private def getUnusedFileGroup(): ShuffleFileGroup = {
        val fileGroup = shuffleState.unusedFileGroups.poll()//从已经被回收的队列中获取一个,如果该队列为空,则创建一个
        if (fileGroup != null) fileGroup else newFileGroup()
      }

      //说明此时没有共享的reduce文件,因此要创建一个
      private def newFileGroup(): ShuffleFileGroup = {
        val fileId = shuffleState.nextFileId.getAndIncrement()
        //产生reduce个文件
        val files = Array.tabulate[File](numBuckets) { bucketId => //多少个reduce,就产生多少个物理真实的文件
          val filename = physicalFileName(shuffleId, bucketId, fileId) //文件名字
          blockManager.diskBlockManager.getFile(filename)//创建该文件对应的存储位置
        }
        val fileGroup = new ShuffleFileGroup(shuffleId, fileId, files)
        shuffleState.allFileGroups.add(fileGroup)
        fileGroup
      }

      //回收一个文件组
      private def recycleFileGroup(group: ShuffleFileGroup) {
        shuffleState.unusedFileGroups.add(group)
      }
    }
  }

  //获取一个map-reduce的输出,即获取某一个reduce需要的内容,并且传输
  //参数已经给定了是哪个shuffle,要获取某个mapperId对应的reduceId的数据块
  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
    if (consolidateShuffleFiles) {
      // Search all file groups associated with this shuffle.
      val shuffleState = shuffleStates(blockId.shuffleId)
      val iter = shuffleState.allFileGroups.iterator
      while (iter.hasNext) {
        val segmentOpt = iter.next.getFileSegmentFor(blockId.mapId, blockId.reduceId) //获取属于该map/reduce的数据块文件所在的offset以及length
        if (segmentOpt.isDefined) {
          val segment = segmentOpt.get
          return new FileSegmentManagedBuffer(
            transportConf, segment.file, segment.offset, segment.length)
        }
      }
      throw new IllegalStateException("Failed to find shuffle block: " + blockId)
    } else {
      val file = blockManager.diskBlockManager.getFile(blockId)//找到该数据块对应的文件,直接返回即可
      new FileSegmentManagedBuffer(transportConf, file, 0, file.length)
    }
  }

  /** Remove all the blocks / files and metadata related to a particular shuffle. */
  def removeShuffle(shuffleId: ShuffleId): Boolean = {
    // Do not change the ordering of this, if shuffleStates should be removed only
    // after the corresponding shuffle blocks have been removed
    val cleaned = removeShuffleBlocks(shuffleId)
    shuffleStates.remove(shuffleId)//移除一个shuffle
    cleaned
  }

  /** Remove all the blocks / files related to a particular shuffle.
    * 删除所有的文件内容---即master要求删除一个shuffled的时候才会真的删除这些数据块文件
    **/
  private def removeShuffleBlocks(shuffleId: ShuffleId): Boolean = {
    shuffleStates.get(shuffleId) match {
      case Some(state) =>
        if (consolidateShuffleFiles) {
          for (fileGroup <- state.allFileGroups; file <- fileGroup.files) {//删除该shuffle对应的文件内容
            file.delete()
          }
        } else {
          for (mapId <- state.completedMapTasks; reduceId <- 0 until state.numBuckets) {
            val blockId = new ShuffleBlockId(shuffleId, mapId, reduceId)
            blockManager.diskBlockManager.getFile(blockId).delete()
          }
        }
        logInfo("Deleted all files for shuffle " + shuffleId)
        true
      case None =>
        logInfo("Could not find files for shuffle " + shuffleId + " for deleting")
        false
    }
  }

  //物理文件名字 merged_shuffle_${shuffleId}_${bucketId}_${fileId} 即shuffleid+第几个reduce+文件ID
  private def physicalFileName(shuffleId: Int, bucketId: Int, fileId: Int) = {
    "merged_shuffle_%d_%d_%d".format(shuffleId, bucketId, fileId)
  }

  private def cleanup(cleanupTime: Long) {
    shuffleStates.clearOldValues(cleanupTime, (shuffleId, state) => removeShuffleBlocks(shuffleId))
  }

  override def stop() {
    metadataCleaner.cancel()
  }
}

private[spark] object FileShuffleBlockResolver {
  /**
   * A group of shuffle files, one per reducer.
   * A particular mapper will be assigned a single ShuffleFileGroup to write its output to.
   * 创建一个文件组,该文件组的所有文件都属于一个shuffle,每一个reduce对应一个文件
   */
  private class ShuffleFileGroup(val shuffleId: Int, val fileId: Int,
                                 val files: Array[File]) {//一个reduce一个真实的文件,但是该节点的所有map都要向同一个文件写入数据
    private var numBlocks: Int = 0//该节点一共处理了多少个map

    /**
     * Stores the absolute index of each mapId in the files of this group. For instance,
     * if mapId 5 is the first block in each file, mapIdToIndex(5) = 0.
     * key是第几个map,value是该map是第几个处理的
     */
    private val mapIdToIndex = new PrimitiveKeyOpenHashMap[Int, Int]()

    /**
     * Stores consecutive offsets and lengths of blocks into each reducer file, ordered by
     * position in the file.
     * Note: mapIdToIndex(mapId) returns the index of the mapper into the vector for every
     * reducer.
     * 每一个reduce作为一个元素,即有10个reduce,则有是个元素组成的数组
     * 每一个元素又是PrimitiveVector类型的向量,向量的每一个元素表示对应每一个map
     *
     * 比如PrimitiveVector内容为1，204，308,表示第一个数据块对应的map输出关于该reduce的信息在文件的哪个开始位置开始的
     */
    private val blockOffsetsByReducer = Array.fill[PrimitiveVector[Long]](files.length) {
      new PrimitiveVector[Long]()
    }

    //与blockOffsetsByReducer记录的信息相同,只是记录的是字节长度
    private val blockLengthsByReducer = Array.fill[PrimitiveVector[Long]](files.length) {
      new PrimitiveVector[Long]()
    }

    //获取某一个reduce的文件
    def apply(bucketId: Int): File = files(bucketId) //返回存储该reduce结果的文件

    //记录该map的输出,每一个reduce的开始位置以及字节长度
    def recordMapOutput(mapId: Int, offsets: Array[Long], lengths: Array[Long]) {
      assert(offsets.length == lengths.length)//必须数组大小是相同的
      mapIdToIndex(mapId) = numBlocks //存储该map是第几个数据块存储
      numBlocks += 1 //数据块增加
      for (i <- 0 until offsets.length) {//记录每一个reduce的开始位置以及长度
        blockOffsetsByReducer(i) += offsets(i)
        blockLengthsByReducer(i) += lengths(i)
      }
    }

    /** Returns the FileSegment associated with the given map task, or None if no entry exists.
      * 返回一个reduce需要的某一个map的结果集
      *
      * 因为一台节点上,即可以执行多个map,比如执行3个map,每一个map产生4个文件,那么此时该节点会有12个文件
      * 但是可以优化,成无论该节点有多少个map,都是只有4个reduce文件即可,即同一个reduce的文件无论哪个map都输出到同一个文件中
      **/
    def getFileSegmentFor(mapId: Int, reducerId: Int): Option[FileSegment] = {
      val file = files(reducerId) //找到属于该reduce的文件
      val blockOffsets = blockOffsetsByReducer(reducerId)//找到reduce在该文件的开始位置,返回是PrimitiveVector向量
      val blockLengths = blockLengthsByReducer(reducerId)//找到reduce在该文件的长度,返回是PrimitiveVector向量
      val index = mapIdToIndex.getOrElse(mapId, -1)//获取mapId是第几个map
      if (index >= 0) {
        val offset = blockOffsets(index)//获取该map对应的开始位置
        val length = blockLengths(index)//获取该map对应的字节长度
        Some(new FileSegment(file, offset, length))
      } else {
        None
      }
    }
  }
}
