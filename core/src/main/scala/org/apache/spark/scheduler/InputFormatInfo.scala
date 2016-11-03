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

import scala.collection.JavaConversions._
import scala.collection.immutable.Set
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.util.ReflectionUtils

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.deploy.SparkHadoopUtil

/**
 * :: DeveloperApi ::
 * Parses and holds information about inputFormat (and files) specified as a parameter.
 *
 * InputFormatInfo表示一个数据源与拆分数据源的org.apache.hadoop.mapred.InputFormat类
 */
@DeveloperApi
class InputFormatInfo(val configuration: Configuration, val inputFormatClazz: Class[_],
    val path: String) extends Logging {

  //org.apache.hadoop.mapred.InputFormat---该类里面有InputSplit[] getSplits和RecordReader<K,V> getRecordReader方法
  var mapreduceInputFormat: Boolean = false //是hadoop new api接口
  var mapredInputFormat: Boolean = false //是hadoop old api接口

  validate()

  override def toString: String = {
    "InputFormatInfo " + super.toString + " .. inputFormatClazz " + inputFormatClazz + ", " +
      "path : " + path
  }

  override def hashCode(): Int = {
    var hashCode = inputFormatClazz.hashCode
    hashCode = hashCode * 31 + path.hashCode
    hashCode
  }

  // Since we are not doing canonicalization of path, this can be wrong : like relative vs
  // absolute path .. which is fine, this is best case effort to remove duplicates - right ?
  override def equals(other: Any): Boolean = other match {
    case that: InputFormatInfo => {
      // not checking config - that should be fine, right ?
      this.inputFormatClazz == that.inputFormatClazz &&
        this.path == that.path
    }
    case _ => false
  }

  //必须inputFormatClazz是hadoop的InputFormat子类
  private def validate() {
    logDebug("validate InputFormatInfo : " + inputFormatClazz + ", path  " + path)

    try {
      if (classOf[org.apache.hadoop.mapreduce.InputFormat[_, _]].isAssignableFrom(
        inputFormatClazz)) {
        logDebug("inputformat is from mapreduce package")
        mapreduceInputFormat = true
      }
      else if (classOf[org.apache.hadoop.mapred.InputFormat[_, _]].isAssignableFrom(
        inputFormatClazz)) {
        logDebug("inputformat is from mapred package")
        mapredInputFormat = true
      }
      else {
        throw new IllegalArgumentException("Specified inputformat " + inputFormatClazz +
          " is NOT a supported input format ? does not implement either of the supported hadoop " +
            "api's")
      }
    }
    catch {
      case e: ClassNotFoundException => {
        throw new IllegalArgumentException("Specified inputformat " + inputFormatClazz +
          " cannot be found ?", e)
      }
    }
  }


  // This method does not expect failures, since validate has already passed ...
  //根据输入源生成数据块文件集合--针对hadoop new api
  private def prefLocsFromMapreduceInputFormat(): Set[SplitInfo] = {
    val conf = new JobConf(configuration)
    SparkHadoopUtil.get.addCredentials(conf)
    FileInputFormat.setInputPaths(conf, path)//设置该job的输入源路径

    //创建InputFormat实例
    val instance: org.apache.hadoop.mapreduce.InputFormat[_, _] =
      ReflectionUtils.newInstance(inputFormatClazz.asInstanceOf[Class[_]], conf).asInstanceOf[
        org.apache.hadoop.mapreduce.InputFormat[_, _]]
    val job = new Job(conf)

    //每一个数据块InputSplit对应N个SplitInfo对象,因为InputSplit数据块在N个节点有备份,因此有N个SplitInfo对象
    val retval = new ArrayBuffer[SplitInfo]()

    //读取输入源,根据InputFormat实例生成不同的task任务输入源InputSplit集合
    val list = instance.getSplits(job) //List<org.apache.hadoop.mapreduce.InputSplit>
    for (split <- list) {
      retval ++= SplitInfo.toSplitInfo(inputFormatClazz, path, split)
    }

    retval.toSet
  }

  // This method does not expect failures, since validate has already passed ...
  //根据输入源生成数据块文件集合--针对hadoop old api
  private def prefLocsFromMapredInputFormat(): Set[SplitInfo] = {
    val jobConf = new JobConf(configuration)
    SparkHadoopUtil.get.addCredentials(jobConf)
    FileInputFormat.setInputPaths(jobConf, path)

    val instance: org.apache.hadoop.mapred.InputFormat[_, _] =
      ReflectionUtils.newInstance(inputFormatClazz.asInstanceOf[Class[_]], jobConf).asInstanceOf[
        org.apache.hadoop.mapred.InputFormat[_, _]]

    val retval = new ArrayBuffer[SplitInfo]()
    instance.getSplits(jobConf, jobConf.getNumMapTasks()).foreach(
        elem => retval ++= SplitInfo.toSplitInfo(inputFormatClazz, path, elem)
    )

    retval.toSet
   }

  //根据输入源生成数据块文件集合
  private def findPreferredLocations(): Set[SplitInfo] = {
    logDebug("mapreduceInputFormat : " + mapreduceInputFormat + ", mapredInputFormat : " +
      mapredInputFormat + ", inputFormatClazz : " + inputFormatClazz)
    if (mapreduceInputFormat) {
      prefLocsFromMapreduceInputFormat()
    }
    else {
      assert(mapredInputFormat)
      prefLocsFromMapredInputFormat()
    }
  }
}




object InputFormatInfo {
  /**
    Computes the preferred locations based on input(s) and returned a location to block map.
    Typical use of this method for allocation would follow some algo like this:

    a) For each host, count number of splits hosted on that host.
    b) Decrement the currently allocated containers on that host.
    c) Compute rack info for each host and update rack -> count map based on (b).
    d) Allocate nodes based on (c)
    e) On the allocation result, ensure that we dont allocate "too many" jobs on a single node
       (even if data locality on that is very high) : this is to prevent fragility of job if a
       single (or small set of) hosts go down.

    go to (a) until required nodes are allocated.

    If a node 'dies', follow same procedure.

    PS: I know the wording here is weird, hopefully it makes some sense !
    参数是一组数据源与拆分数据源的org.apache.hadoop.mapred.InputFormat类

    返回值 key是每一台物理节点host,value是该节点上存储的数据块信息集合
  */
  def computePreferredLocations(formats: Seq[InputFormatInfo]): Map[String, Set[SplitInfo]] = {

    //key是每一台物理节点host,value是该节点上存储的数据块信息集合
    val nodeToSplit = new HashMap[String, HashSet[SplitInfo]]
    for (inputSplit <- formats) {//循环每一个  数据源与拆分数据源的org.apache.hadoop.mapred.InputFormat类
      val splits = inputSplit.findPreferredLocations() //获取该数据源对应的SplitInfo集合

      for (split <- splits){
        val location = split.hostLocation//该数据块所在的节点host
        val set = nodeToSplit.getOrElseUpdate(location, new HashSet[SplitInfo]) //设置该节点上存储的数据块集合
        set += split
      }
    }

    nodeToSplit.mapValues(_.toSet).toMap
  }
}
