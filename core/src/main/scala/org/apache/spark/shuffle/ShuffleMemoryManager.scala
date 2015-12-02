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

import scala.collection.mutable

import com.google.common.annotations.VisibleForTesting

import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.{Logging, SparkException, SparkConf, TaskContext}

/**
 * Allocates a pool of memory to tasks for use in shuffle operations. 
 * 为任务分配一个内存池,用于任务的shuffle操作
 * Each disk-spilling collection (ExternalAppendOnlyMap or ExternalSorter) used by these tasks can acquire memory
 * from this pool and release it as it spills data out. When a task ends, all its memory will be
 * released by the Executor.
 *
 * This class tries to ensure that each task gets a reasonable share of memory, instead of some
 * task ramping up to a large amount first and then causing others to spill to disk repeatedly.
 * If there are N tasks, it ensures that each tasks can acquire at least 1 / 2N of the memory
 * before it has to spill, and at most 1 / N. Because N varies dynamically, we keep track of the
 * set of active tasks and redo the calculations of 1 / 2N and 1 / N in waiting tasks whenever
 * this set changes. This is all done by synchronizing access on "this" to mutate state and using
 * wait() and notifyAll() to signal changes.
 * 至少保证为每一个任务的1/2N内存是可以分配出去的
 * Use `ShuffleMemoryManager.create()` factory method to create a new instance.
 *
 * @param maxMemory total amount of memory available for execution, in bytes.
 * @param pageSizeBytes number of bytes for each page, by default.
 */
private[spark]
class ShuffleMemoryManager protected (
    val maxMemory: Long,//该shuffle管理器管理的最多允许内存数量
    val pageSizeBytes: Long) //内存管理单位是pageSize
  extends Logging {

  //当前线程下的任务task所持有的内存使用数量  ,key是taskId,value是该task对应的内存使用量
  private val taskMemory = new mutable.HashMap[Long, Long]()  // taskAttemptId -> memory bytes

  //获取当前线程绑定的任务ID
  private def currentTaskAttemptId(): Long = {
    // In case this is called on the driver, return an invalid task attempt id.
    Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(-1L)
  }

  /**
   * Try to acquire up to numBytes memory for the current task, and return the number of bytes
   * obtained, or 0 if none can be allocated. This call may block until there is enough free memory
   * in some situations, to make sure each task has a chance to ramp up to at least 1 / 2N of the
   * total memory pool (where N is the # of active tasks) before it is forced to spill. This can
   * happen if the number of tasks increases but an older task had a lot of memory already.
   * 为当前线程对应的任务申请内存,返回值是真正申请到的内存数量
   */
  def tryToAcquire(numBytes: Long): Long = synchronized {
    val taskAttemptId = currentTaskAttemptId()
    assert(numBytes > 0, "invalid number of bytes requested: " + numBytes)

    // Add this task to the taskMemory map just so we can keep an accurate count of the number
    // of active tasks, to let other tasks ramp down their memory in calls to tryToAcquire
    //向内存映射中存储该task与内存使用情况,初始化为0
    if (!taskMemory.contains(taskAttemptId)) {
      taskMemory(taskAttemptId) = 0L
      notifyAll()  // Will later cause waiting tasks to wake up and check numThreads again
    }

    // Keep looping until we're either sure that we don't want to grant this request (because this
    // task would have more than 1 / numActiveTasks of the memory) or we have enough free
    // memory to give it (we always let each task get at least 1 / (2 * numActiveTasks)).
    while (true) {
      val numActiveTasks = taskMemory.keys.size //当前多少个task任务
      val curMem = taskMemory(taskAttemptId) //该任务当前分配了多少内存
      val freeMemory = maxMemory - taskMemory.values.sum //剩余内存数量

      // How much we can grant this task; don't let it grow to more than 1 / numActiveTasks;
      // don't let it be negative
      //可能等待被分配的numBytes字节数,内存不够,无法分配,因此该方法返回能分配多少字节
      //maxMemory / numActiveTasks 表示可以平均分配每一个任务多少空间  - curMem 表示对于该任务还能分配多少内存空间
      val maxToGrant = math.min(numBytes, math.max(0, (maxMemory / numActiveTasks) - curMem))

      if (curMem < maxMemory / (2 * numActiveTasks)) { //如果当前该任务分配的空间小于 每个任务平均分配内存的理论值的一半
        // We want to let each task get at least 1 / (2 * numActiveTasks) before blocking;
        // if we can't give it this much now, wait for other tasks to free up memory
        // (this happens if older tasks allocated lots of memory before N grew)
        if (freeMemory >= math.min(maxToGrant, maxMemory / (2 * numActiveTasks) - curMem)) {//空闲内存够每个任务平均的1/2N内存,则进行分配
          val toGrant = math.min(maxToGrant, freeMemory)
          taskMemory(taskAttemptId) += toGrant
          return toGrant
        } else {//说明空闲内存不够分,要等待内存释放后再进行分配,即说明内存空间不足平均每个任务的1/2N
          logInfo(
            s"TID $taskAttemptId waiting for at least 1/2N of shuffle memory pool to be free")
          wait()
        }
      } else {//说明当前该任务分配的空间大于 每个任务平均分配内存的理论值的一半
        // Only give it as much memory as is free, which might be none if it reached 1 / numThreads
        val toGrant = math.min(maxToGrant, freeMemory)//要么全部都分配给该任务,要么都满足该任务
        taskMemory(taskAttemptId) += toGrant
        return toGrant
      }
    }
    0L  // Never reached
  }

  /** Release numBytes bytes for the current task. 
   * 为当前的任务释放参数 numBytes个字节数
   **/
  def release(numBytes: Long): Unit = synchronized {
    //获取当前线程绑定的任务ID
    val taskAttemptId = currentTaskAttemptId()
    val curMem = taskMemory.getOrElse(taskAttemptId, 0L)
    
    //校验准备释放的字节数一定要小于该task任务拥有的线程数
    if (curMem < numBytes) {
      throw new SparkException(
        s"Internal error: release called on ${numBytes} bytes but task only has ${curMem}")
    }
    //释放字节
    taskMemory(taskAttemptId) -= numBytes
    notifyAll()  // Notify waiters who locked "this" in tryToAcquire that memory has been freed
  }

  /** Release all memory for the current task and mark it as inactive (e.g. when a task ends). 
   * 将当前线程下的任务task,全部内存都释放掉  
   **/
  def releaseMemoryForThisTask(): Unit = synchronized {
    val taskAttemptId = currentTaskAttemptId()
    taskMemory.remove(taskAttemptId)
    notifyAll()  // Notify waiters who locked "this" in tryToAcquire that memory has been freed
  }

  /** Returns the memory consumption, in bytes, for the current task 
   * 获取当前线程下的任务task所持有的内存使用数量  
   **/
  def getMemoryConsumptionForThisTask(): Long = synchronized {
    val taskAttemptId = currentTaskAttemptId()
    taskMemory.getOrElse(taskAttemptId, 0L)
  }
}


private[spark] object ShuffleMemoryManager {

  def create(conf: SparkConf, numCores: Int): ShuffleMemoryManager = {
    val maxMemory = ShuffleMemoryManager.getMaxMemory(conf)
    val pageSize = ShuffleMemoryManager.getPageSize(conf, maxMemory, numCores)
    new ShuffleMemoryManager(maxMemory, pageSize)
  }

  def create(maxMemory: Long, pageSizeBytes: Long): ShuffleMemoryManager = {
    new ShuffleMemoryManager(maxMemory, pageSizeBytes)
  }

  @VisibleForTesting
  def createForTesting(maxMemory: Long): ShuffleMemoryManager = {
    new ShuffleMemoryManager(maxMemory, 4 * 1024 * 1024) //默认每个pageSize使用4M内存
  }

  /**
   * Figure out the shuffle memory limit from a SparkConf. We currently have both a fraction
   * of the memory pool and a safety factor since collections can sometimes grow bigger than
   * the size we target before we estimate their sizes again.
   * 获取shuffle需要的内存
   */
  private def getMaxMemory(conf: SparkConf): Long = {
    val memoryFraction = conf.getDouble("spark.shuffle.memoryFraction", 0.2)
    val safetyFraction = conf.getDouble("spark.shuffle.safetyFraction", 0.8)
    (Runtime.getRuntime.maxMemory * memoryFraction * safetyFraction).toLong
  }

  /**
   * Sets the page size, in bytes.
   *
   * If user didn't explicitly set "spark.buffer.pageSize", we figure out the default value
   * by looking at the number of cores available to the process, and the total amount of memory,
   * and then divide it by a factor of safety.
   * 
   * @maxMemory shuffle阶段使用的总内存量,而不是机器所包含的内存量
   */
  private def getPageSize(conf: SparkConf, maxMemory: Long, numCores: Int): Long = {
    val minPageSize = 1L * 1024 * 1024   // 1MB
    val maxPageSize = 64L * minPageSize  // 64MB
    val cores = if (numCores > 0) numCores else Runtime.getRuntime.availableProcessors() //获取cpu数量
    // Because of rounding to next power of 2, we may have safetyFactor as 8 in worst case
    val safetyFactor = 16
    // TODO(davies): don't round to next power of 2
    val size = ByteArrayMethods.nextPowerOf2(maxMemory / cores / safetyFactor)
    val default = math.min(maxPageSize, math.max(minPageSize, size))
    conf.getSizeAsBytes("spark.buffer.pageSize", default)
  }
}
