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

package org.apache.spark.scheduler

import collection.mutable.ArrayBuffer

import org.apache.spark.annotation.DeveloperApi

// information about a specific split instance : handles both split instances.
// So that we do not need to worry about the differences.
/**
 * 表示一个HDFS上的数据块在一个节点的对象,
 * 因为HDFS上的数据块备份在多个节点上,则他们存在多个该对象,只是hostLocation参数不一样而已
 */
@DeveloperApi
class SplitInfo(
    val inputFormatClazz: Class[_],//org.apache.hadoop.mapreduce.InputFormat实例,读取该数据块
    val hostLocation: String,//该数据块对应所在节点host,该数据块在N个节点上有备份,这个hostLocation就是其中一个节点host
    val path: String,//该数据块对应的path路径,即该数据块对应文件所属的HDFS的路径
    val length: Long,//该数据块的字节大小
    val underlyingSplit: Any) {//该数据块文件 org.apache.hadoop.mapreduce.InputSplit
  override def toString(): String = {
    "SplitInfo " + super.toString + " .. inputFormatClazz " + inputFormatClazz +
      ", hostLocation : " + hostLocation + ", path : " + path +
      ", length : " + length + ", underlyingSplit " + underlyingSplit
  }

  override def hashCode(): Int = {
    var hashCode = inputFormatClazz.hashCode
    hashCode = hashCode * 31 + hostLocation.hashCode
    hashCode = hashCode * 31 + path.hashCode
    // ignore overflow ? It is hashcode anyway !
    hashCode = hashCode * 31 + (length & 0x7fffffff).toInt
    hashCode
  }

  // This is practically useless since most of the Split impl's dont seem to implement equals :-(
  // So unless there is identity equality between underlyingSplits, it will always fail even if it
  // is pointing to same block.
  override def equals(other: Any): Boolean = other match {
    case that: SplitInfo => {
      this.hostLocation == that.hostLocation &&
        this.inputFormatClazz == that.inputFormatClazz &&
        this.path == that.path &&
        this.length == that.length &&
        // other split specific checks (like start for FileSplit)
        this.underlyingSplit == that.underlyingSplit
    }
    case _ => false
  }
}

object SplitInfo {

  def toSplitInfo(inputFormatClazz: Class[_], path: String,
                  mapredSplit: org.apache.hadoop.mapred.InputSplit): Seq[SplitInfo] = {
    val retval = new ArrayBuffer[SplitInfo]()
    val length = mapredSplit.getLength
    for (host <- mapredSplit.getLocations) {
      retval += new SplitInfo(inputFormatClazz, host, path, length, mapredSplit)
    }
    retval
  }

  /**
   *
   * @param inputFormatClazz org.apache.hadoop.mapreduce.InputFormat实例,读取该数据块的
   * @param path 该数据源文件的输入路径,可能是包含通配符的路径path
   * @param mapreduceSplit 一个拆分后的数据块
   * @return 该数据块在多台节点上存在备份,因此返回一个集合,每一个节点一个SplitInfo对象
   */
  def toSplitInfo(inputFormatClazz: Class[_], path: String,
                  mapreduceSplit: org.apache.hadoop.mapreduce.InputSplit): Seq[SplitInfo] = {
    val retval = new ArrayBuffer[SplitInfo]()
    val length = mapreduceSplit.getLength //数据块所对应的字节数
    for (host <- mapreduceSplit.getLocations) {
      retval += new SplitInfo(inputFormatClazz, host, path, length, mapreduceSplit)
    }
    retval
  }
}
