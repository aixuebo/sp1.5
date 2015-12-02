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

package org.apache.spark

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.collection.{AppendOnlyMap, ExternalAppendOnlyMap}

/**
 * :: DeveloperApi ::
 * A set of functions used to aggregate data.
 *
 * @param createCombiner function to create the initial value of the aggregation.
 * @param mergeValue function to merge a new value into the aggregation result.
 * @param mergeCombiners function to merge outputs from multiple mergeValue function.
 */
@DeveloperApi
case class Aggregator[K, V, C] (
    createCombiner: V => C,//将value转换成一个C对象,当key不存在的时候,则创建key对应的value转换成C,根key-value管理
    mergeValue: (C, V) => C, //如果key存在对应的value,则将存在的value即c,与新的value v进行合并,产生新的c
    mergeCombiners: (C, C) => C) {

  // When spilling is enabled sorting will happen externally, but not necessarily with an
  // ExternalSorter.
  private val isSpillEnabled = SparkEnv.get.conf.getBoolean("spark.shuffle.spill", true)

  @deprecated("use combineValuesByKey with TaskContext argument", "0.9.0")
  def combineValuesByKey(iter: Iterator[_ <: Product2[K, V]]): Iterator[(K, C)] =
    combineValuesByKey(iter, null)

    //循环每一个key-value,返回key,c的迭代器,c是相同的key对应的value的合并后的值
    //通过key进行合并所有的value
  def combineValuesByKey(iter: Iterator[_ <: Product2[K, V]],
                         context: TaskContext): Iterator[(K, C)] = {
    if (!isSpillEnabled) {
      val combiners = new AppendOnlyMap[K, C] //类似hash table的实现
      var kv: Product2[K, V] = null //每一个key-value
      
      //参数hadValue表示combiners存在该key,oldValue表示该key存储的值是什么
      val update = (hadValue: Boolean, oldValue: C) => { //返回值就是对key更新的值
        if (hadValue){
          mergeValue(oldValue, kv._2)  //如果该key在combiners中存在,则将老value与新的value进行合并,将合并后的值存储在key上
        }else {
         createCombiner(kv._2) //说明该key在combiners中不存在,则将该value存储在key中
        }
      }
      while (iter.hasNext) {
        kv = iter.next() //循环每一个key-value
        combiners.changeValue(kv._1, update) //对key进行更新
      }
      combiners.iterator //返回合并后的迭代器,key还是以前的key,value是合并后的value
    } else {
      val combiners = new ExternalAppendOnlyMap[K, V, C](createCombiner, mergeValue, mergeCombiners)
      combiners.insertAll(iter)
      updateMetrics(context, combiners)
      combiners.iterator
    }
  }

  @deprecated("use combineCombinersByKey with TaskContext argument", "0.9.0")
  def combineCombinersByKey(iter: Iterator[_ <: Product2[K, C]]) : Iterator[(K, C)] =
    combineCombinersByKey(iter, null)

    //迭代iter,合并每一个K-C
  def combineCombinersByKey(iter: Iterator[_ <: Product2[K, C]], context: TaskContext)
    : Iterator[(K, C)] =
  {
    if (!isSpillEnabled) {
      val combiners = new AppendOnlyMap[K, C]
      var kc: Product2[K, C] = null //迭代iter返回的key-value
      
      //参数hadValue表示combiners存在该key,oldValue表示该key存储的值是什么
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) { //如果key存在,则合并老的value和新的value
         mergeCombiners(oldValue, kc._2) 
        } else {
          kc._2 //如果key不存在,则返回该value
         } 
      }
      while (iter.hasNext) {
        kc = iter.next()
        combiners.changeValue(kc._1, update)
      }
      combiners.iterator
    } else {
      val combiners = new ExternalAppendOnlyMap[K, C, C](identity, mergeCombiners, mergeCombiners)
      combiners.insertAll(iter)
      updateMetrics(context, combiners)
      combiners.iterator
    }
  }

  /** Update task metrics after populating the external map. */
  private def updateMetrics(context: TaskContext, map: ExternalAppendOnlyMap[_, _, _]): Unit = {
    Option(context).foreach { c =>
      c.taskMetrics().incMemoryBytesSpilled(map.memoryBytesSpilled)
      c.taskMetrics().incDiskBytesSpilled(map.diskBytesSpilled)
      c.internalMetricsToAccumulators(
        InternalAccumulator.PEAK_EXECUTION_MEMORY).add(map.peakMemoryUsedBytes)
    }
  }
}
