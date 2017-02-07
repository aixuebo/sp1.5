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

package org.apache.spark.streaming.dstream

import org.apache.spark.rdd.{PartitionerAwareUnionRDD, RDD, UnionRDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.Duration

import scala.reflect.ClassTag

/**
 * windowDuration=3*batchInterval，
slideDuration=10*batchInterval,
表示的含义是每个10个时间间隔对之前的3个RDD进行统计计算，也意味着有7个RDD没在window窗口的统计范围内。slideDuration的默认值是batchInterval

即每一次对前N次的处理结果进一步进行处理,因为默认非WindowedDStream,都是一个时间片段下只能处理该时间点的聚合.不能让全局聚合

 * 背景描述：在社交网络（例如微博），电子商务（例如京东），热搜词（例如百度）等人们核心关注的内容之一就是我所关注的内容中，大家正在最关注什么或者说当前的热点是什么，
 * 这在市级企业级应用中是非常有价值，例如我们关心过去30分钟大家正在热搜什么，并且每5分钟更新一次，这就使得热点内容是动态更新的，当然更有价值
 */
private[streaming]
class WindowedDStream[T: ClassTag](
    parent: DStream[T],
    _windowDuration: Duration,//parent.slideDuration = 5s   _slideDuration = 10s   _windowDuration = 60s 表示每隔5s进行一次数据收集,但是现在每隔10s进行一次,操作的是最近60s的数据
    _slideDuration: Duration)
  extends DStream[T](parent.ssc) {

  if (!_windowDuration.isMultipleOf(parent.slideDuration)) {//_windowDuration 必须是 时间间隔slideDuration的倍数
    throw new Exception("The window duration of windowed DStream (" + _windowDuration + ") " +
    "must be a multiple of the slide duration of parent DStream (" + parent.slideDuration + ")")
  }

  if (!_slideDuration.isMultipleOf(parent.slideDuration)) { //_slideDuration 必须是 时间间隔slideDuration的倍数
    throw new Exception("The slide duration of windowed DStream (" + _slideDuration + ") " +
    "must be a multiple of the slide duration of parent DStream (" + parent.slideDuration + ")")
  }

  // Persist parent level by default, as those RDDs are going to be obviously reused.
  parent.persist(StorageLevel.MEMORY_ONLY_SER) //默认将parentRDD设置persist,因为parent RDD会在window slide中被反复读到

  def windowDuration: Duration = _windowDuration //window length - The duration of the window (3 in the figure)，滑动窗口的时间跨度，指本次window操作所包含的过去的时间间隔（图中包含3个batch interval，可以理解时间单位）

  override def dependencies: List[DStream[_]] = List(parent)

  override def slideDuration: Duration = _slideDuration //sliding interval - The interval at which the window operation is performed (2 in the figure).（窗口操作执行的频率，即每隔多少时间计算一次）

  override def parentRememberDuration: Duration = rememberDuration + windowDuration

  override def persist(level: StorageLevel): DStream[T] = {
    // Do not let this windowed DStream be persisted as windowed (union-ed) RDDs share underlying
    // RDDs and persisting the windowed RDDs would store numerous copies of the underlying data.
    // Instead control the persistence of the parent DStream.
    // 不要直接persist windowed RDDS，而是去persist parent RDD，原因是各个windows RDDs之间有大量的重复数据，直接persist浪费空间
    parent.persist(level)
    this
  }

  override def compute(validTime: Time): Option[RDD[T]] = {
    val currentWindow = new Interval(validTime - windowDuration + parent.slideDuration, validTime)
    val rddsInWindow = parent.slice(currentWindow) //返回一组Seq[RDD[T]]集合,后续对一组RDD进行处理
    val windowRDD = if (rddsInWindow.flatMap(_.partitioner).distinct.length == 1) {
      logDebug("Using partition aware union for windowing at " + validTime)
      new PartitionerAwareUnionRDD(ssc.sc, rddsInWindow) //多个RDD拥有相同的partitioner,将他们合并成一个RDD
    } else {
      logDebug("Using normal union for windowing at " + validTime)
      new UnionRDD(ssc.sc, rddsInWindow) //合并多个RDD
    }
    Some(windowRDD)
  }
}
