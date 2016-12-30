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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.{RDDOperationScope, RDD}
import org.apache.spark.util.Utils

/**
 * 描述一个RDD内容
 */
@DeveloperApi
class RDDInfo(
    val id: Int,//rdd的id
    val name: String,//rdd的名字
    val numPartitions: Int,//rdd有多少个map
    var storageLevel: StorageLevel,//rdd的存储级别
    val parentIds: Seq[Int],//RDD是哪些父RDD创建的,这个集合是父RDD的ID集合
    val scope: Option[RDDOperationScope] = None)
  extends Ordered[RDDInfo] {//rdd支持排序,按照rdd的id进行排序

  var numCachedPartitions = 0 //多少个缓存的partition

  //该RDD使用内存、磁盘、额外存储的字节数量
  var memSize = 0L
  var diskSize = 0L
  var externalBlockStoreSize = 0L

  //是否被缓存了,前提是该RDD已经有字节了,并且已经产生缓存了,则说明该数据已经缓存了
  def isCached: Boolean =
    (memSize + diskSize + externalBlockStoreSize > 0) && numCachedPartitions > 0

  override def toString: String = {
    import Utils.bytesToString
    ("RDD \"%s\" (%d) StorageLevel: %s; CachedPartitions: %d; TotalPartitions: %d; " +
      "MemorySize: %s; ExternalBlockStoreSize: %s; DiskSize: %s").format(
        name, id, storageLevel.toString, numCachedPartitions, numPartitions,
        bytesToString(memSize), bytesToString(externalBlockStoreSize), bytesToString(diskSize))
  }

  //rdd支持排序,按照rdd的id进行排序
  override def compare(that: RDDInfo): Int = {
    this.id - that.id
  }
}

private[spark] object RDDInfo {
  def fromRdd(rdd: RDD[_]): RDDInfo = {
    val rddName = Option(rdd.name).getOrElse(Utils.getFormattedClassName(rdd))
    val parentIds = rdd.dependencies.map(_.rdd.id)
    new RDDInfo(rdd.id, rddName, rdd.partitions.length, rdd.getStorageLevel, parentIds, rdd.scope)
  }
}
