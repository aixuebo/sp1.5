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

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.Utils

/**
 * :: DeveloperApi ::
 * Flags for controlling the storage of an RDD. Each StorageLevel records whether to use memory,
 * 标记,用于控制RDD的存储,每一个记录是使用内存还是外部存储,
 * or ExternalBlockStore, whether to drop the RDD to disk if it falls out of memory or
 * ExternalBlockStore, whether to keep the data in memory in a serialized format, and whether
 * to replicate the RDD partitions on multiple nodes.
 * 如果内存溢出的时候是写入磁盘还是写入额外的存储
 * 是否需要压缩后的数据存储在内存中
 * 是否需要将RDD的partition存储在多个节点上
 * The [[org.apache.spark.storage.StorageLevel$]] singleton object contains some static constants
 * for commonly useful storage levels. To create your own storage level object, use the
 * factory method of the singleton object (`StorageLevel(...)`).
 * 存储级别与方式
 */
@DeveloperApi
class StorageLevel private(
    private var _useDisk: Boolean,//使用磁盘存储
    private var _useMemory: Boolean,//使用内存存储
    private var _useOffHeap: Boolean,//使用额外的存储
    private var _deserialized: Boolean,
    private var _replication: Int = 1)//备份到多少个节点上
  extends Externalizable {

  // TODO: Also add fields for caching priority, dataset ID, and flushing.
  //使用flag标示四个boolean类型的
  private def this(flags: Int, replication: Int) {
    this((flags & 8) != 0, (flags & 4) != 0, (flags & 2) != 0, (flags & 1) != 0, replication)
  }

  //默认使用内存做存储
  def this() = this(false, true, false, false)  // For deserialization

  def useDisk: Boolean = _useDisk
  def useMemory: Boolean = _useMemory
  def useOffHeap: Boolean = _useOffHeap
  def deserialized: Boolean = _deserialized
  def replication: Int = _replication

  //约束数据的备份数不能多于40个
  assert(replication < 40, "Replication restricted to be less than 40 for calculating hash codes")

  //校验如果useOffHeap=true,必须不能使用内存，也不能使用磁盘,也不能支持序列化,也不能支持多个备份
  if (useOffHeap) {
    require(!useDisk, "Off-heap storage level does not support using disk")
    require(!useMemory, "Off-heap storage level does not support using heap memory")
    require(!deserialized, "Off-heap storage level does not support deserialized storage")
    require(replication == 1, "Off-heap storage level does not support multiple replication")
  }

  //复制一个存储对象
  override def clone(): StorageLevel = {
    new StorageLevel(useDisk, useMemory, useOffHeap, deserialized, replication)
  }

  override def equals(other: Any): Boolean = other match {
    case s: StorageLevel =>
      s.useDisk == useDisk &&
      s.useMemory == useMemory &&
      s.useOffHeap == useOffHeap &&
      s.deserialized == deserialized &&
      s.replication == replication
    case _ =>
      false
  }

  //true表示可用,即有存储的方式 以及 有备份数量即可
  def isValid: Boolean = (useMemory || useDisk || useOffHeap) && (replication > 0)

  //将四个boolean转换成int
  def toInt: Int = {
    var ret = 0
    if (_useDisk) {
      ret |= 8
    }
    if (_useMemory) {
      ret |= 4
    }
    if (_useOffHeap) {
      ret |= 2
    }
    if (_deserialized) {
      ret |= 1
    }
    ret
  }

  //序列化
  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    out.writeByte(toInt)
    out.writeByte(_replication)
  }

  //反序列化对象
  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    val flags = in.readByte()
    _useDisk = (flags & 8) != 0
    _useMemory = (flags & 4) != 0
    _useOffHeap = (flags & 2) != 0
    _deserialized = (flags & 1) != 0
    _replication = in.readByte()
  }

  @throws(classOf[IOException])
  private def readResolve(): Object = StorageLevel.getCachedStorageLevel(this)

  override def toString: String = {
    s"StorageLevel($useDisk, $useMemory, $useOffHeap, $deserialized, $replication)"
  }

  override def hashCode(): Int = toInt * 41 + replication

  def description: String = {
    var result = ""
    result += (if (useDisk) "Disk " else "")
    result += (if (useMemory) "Memory " else "")
    result += (if (useOffHeap) "ExternalBlockStore " else "")
    result += (if (deserialized) "Deserialized " else "Serialized ")
    result += s"${replication}x Replicated"
    result
  }
}


/**
 * Various [[org.apache.spark.storage.StorageLevel]] defined and utility functions for creating
 * new storage levels.
 */
object StorageLevel {
  val NONE = new StorageLevel(false, false, false, false) //没有存储,没有序列化,1个备份
  val DISK_ONLY = new StorageLevel(true, false, false, false) //仅仅存储在磁盘,1个备份
  val DISK_ONLY_2 = new StorageLevel(true, false, false, false, 2) //仅仅存储在磁盘,2个备份
  val MEMORY_ONLY = new StorageLevel(false, true, false, true) //仅仅内存存储,序列化,1个备份
  val MEMORY_ONLY_2 = new StorageLevel(false, true, false, true, 2) //仅仅内存存储,序列化,2个备份
  val MEMORY_ONLY_SER = new StorageLevel(false, true, false, false)
  val MEMORY_ONLY_SER_2 = new StorageLevel(false, true, false, false, 2)
  val MEMORY_AND_DISK = new StorageLevel(true, true, false, true)
  val MEMORY_AND_DISK_2 = new StorageLevel(true, true, false, true, 2)
  val MEMORY_AND_DISK_SER = new StorageLevel(true, true, false, false)
  val MEMORY_AND_DISK_SER_2 = new StorageLevel(true, true, false, false, 2)
  val OFF_HEAP = new StorageLevel(false, false, true, false) //使用外部存储

  /**
   * :: DeveloperApi ::
   * Return the StorageLevel object with the specified name.
   */
  @DeveloperApi
  def fromString(s: String): StorageLevel = s match {
    case "NONE" => NONE
    case "DISK_ONLY" => DISK_ONLY
    case "DISK_ONLY_2" => DISK_ONLY_2
    case "MEMORY_ONLY" => MEMORY_ONLY
    case "MEMORY_ONLY_2" => MEMORY_ONLY_2
    case "MEMORY_ONLY_SER" => MEMORY_ONLY_SER
    case "MEMORY_ONLY_SER_2" => MEMORY_ONLY_SER_2
    case "MEMORY_AND_DISK" => MEMORY_AND_DISK
    case "MEMORY_AND_DISK_2" => MEMORY_AND_DISK_2
    case "MEMORY_AND_DISK_SER" => MEMORY_AND_DISK_SER
    case "MEMORY_AND_DISK_SER_2" => MEMORY_AND_DISK_SER_2
    case "OFF_HEAP" => OFF_HEAP
    case _ => throw new IllegalArgumentException(s"Invalid StorageLevel: $s")
  }

  /**
   * :: DeveloperApi ::
   * Create a new StorageLevel object without setting useOffHeap.
   */
  @DeveloperApi
  def apply(
      useDisk: Boolean,
      useMemory: Boolean,
      useOffHeap: Boolean,
      deserialized: Boolean,
      replication: Int): StorageLevel = {
    getCachedStorageLevel(
      new StorageLevel(useDisk, useMemory, useOffHeap, deserialized, replication))
  }

  /**
   * :: DeveloperApi ::
   * Create a new StorageLevel object.
   */
  @DeveloperApi
  def apply(
      useDisk: Boolean,
      useMemory: Boolean,
      deserialized: Boolean,
      replication: Int = 1): StorageLevel = {
    getCachedStorageLevel(new StorageLevel(useDisk, useMemory, false, deserialized, replication))
  }

  /**
   * :: DeveloperApi ::
   * Create a new StorageLevel object from its integer representation.
   */
  @DeveloperApi
  def apply(flags: Int, replication: Int): StorageLevel = {
    getCachedStorageLevel(new StorageLevel(flags, replication))
  }

  /**
   * :: DeveloperApi ::
   * Read StorageLevel object from ObjectInput stream.
   */
  @DeveloperApi
  def apply(in: ObjectInput): StorageLevel = {
    val obj = new StorageLevel()
    obj.readExternal(in)
    getCachedStorageLevel(obj)
  }

  //单例方法
  private[spark] val storageLevelCache = new ConcurrentHashMap[StorageLevel, StorageLevel]()

  private[spark] def getCachedStorageLevel(level: StorageLevel): StorageLevel = {
    storageLevelCache.putIfAbsent(level, level)
    storageLevelCache.get(level)
  }
}
