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

package org.apache.spark.util

import java.util.Set
import java.util.Map.Entry
import java.util.concurrent.ConcurrentHashMap

import scala.collection.{JavaConversions, mutable}

import org.apache.spark.Logging

//存储value和存储value时候的时间戳
private[spark] case class TimeStampedValue[V](value: V, timestamp: Long)

/**
 * This is a custom implementation of scala.collection.mutable.Map which stores the insertion
 * timestamp along with each key-value pair. If specified, the timestamp of each pair can be
 * updated every time it is accessed. Key-value pairs whose timestamp are older than a particular
 * threshold time can then be removed using the clearOldValues method. This is intended to
 * be a drop-in replacement of scala.collection.mutable.HashMap.
 *
 * @param updateTimeStampOnGet Whether timestamp of a pair will be updated when it is accessed
 *
 *参数updateTimeStampOnGet true表示每次get时候也会更新对应的时间戳
 */
private[spark] class TimeStampedHashMap[A, B](updateTimeStampOnGet: Boolean = false)
  extends mutable.Map[A, B]() with Logging {

  private val internalMap = new ConcurrentHashMap[A, TimeStampedValue[B]]()

  //获取key对应的value,并且设置当前时间
  def get(key: A): Option[B] = {
    val value = internalMap.get(key)
    if (value != null && updateTimeStampOnGet) {//key已经存在对应得值了,并且要更新时间戳
      //key 老值 新值
      internalMap.replace(key, value, TimeStampedValue(value.value, currentTime))
    }
    Option(value).map(_.value) //返回Key对应的value,不需要返回时间戳
  }

  def iterator: Iterator[(A, B)] = {
    val jIterator = getEntrySet.iterator
    JavaConversions.asScalaIterator(jIterator).map(kv => (kv.getKey, kv.getValue.value)) //只是返回Key和value,不返回时间戳
  }

  def getEntrySet: Set[Entry[A, TimeStampedValue[B]]] = internalMap.entrySet

  //新增是重新创建一个Map对象
  override def + [B1 >: B](kv: (A, B1)): mutable.Map[A, B1] = {
    val newMap = new TimeStampedHashMap[A, B1] //新的
    val oldInternalMap = this.internalMap.asInstanceOf[ConcurrentHashMap[A, TimeStampedValue[B1]]] //老的
    newMap.internalMap.putAll(oldInternalMap) //新的添加老的全部内容
    kv match { case (a, b) => newMap.internalMap.put(a, TimeStampedValue(b, currentTime)) } //添加一个key-value,以及产生一个新的时间戳
    newMap //返回新的
  }

  //删除也是重新创建一个新的对象
  override def - (key: A): mutable.Map[A, B] = {
    val newMap = new TimeStampedHashMap[A, B]
    newMap.internalMap.putAll(this.internalMap)
    newMap.internalMap.remove(key)
    newMap
  }

  //在原有Map上新增
  override def += (kv: (A, B)): this.type = {
    kv match { case (a, b) => internalMap.put(a, TimeStampedValue(b, currentTime)) }
    this
  }

  //在原有map上删除
  override def -= (key: A): this.type = {
    internalMap.remove(key)
    this
  }

  //在原有map上操作更新
  override def update(key: A, value: B) {
    this += ((key, value))
  }

  override def apply(key: A): B = {
    get(key).getOrElse { throw new NoSuchElementException() }
  }

  //使用p函数进行过滤,仅仅要返回true的数据集合
  override def filter(p: ((A, B)) => Boolean): mutable.Map[A, B] = {
    JavaConversions.mapAsScalaConcurrentMap(internalMap)
      .map { case (k, TimeStampedValue(v, t)) => (k, v) }
      .filter(p)
  }

  override def empty: mutable.Map[A, B] = new TimeStampedHashMap[A, B]()

  override def size: Int = internalMap.size

  override def foreach[U](f: ((A, B)) => U) {
    val it = getEntrySet.iterator
    while(it.hasNext) {
      val entry = it.next()
      val kv = (entry.getKey, entry.getValue.value)
      f(kv)
    }
  }

  //如果不存在key才添加,如果存在,则不添加,返回老的value值
  def putIfAbsent(key: A, value: B): Option[B] = {
    val prev = internalMap.putIfAbsent(key, TimeStampedValue(value, currentTime))
    Option(prev).map(_.value)
  }

  //在原有map上追加一个新的map集合
  def putAll(map: Map[A, B]) {
    map.foreach { case (k, v) => update(k, v) }
  }

  def toMap: Map[A, B] = iterator.toMap

  //移除在threshTime时间戳之前的数据,并且每一个移除的数据,都要进行f函数处理
  def clearOldValues(threshTime: Long, f: (A, B) => Unit) {
    val it = getEntrySet.iterator
    while (it.hasNext) {
      val entry = it.next()
      if (entry.getValue.timestamp < threshTime) {//在threshTime之前的数据都要进行f处理
        f(entry.getKey, entry.getValue.value)
        logDebug("Removing key " + entry.getKey)
        it.remove()
      }
    }
  }

  /** Removes old key-value pairs that have timestamp earlier than `threshTime`. 
   *  仅仅删除过期数据  
   **/
  def clearOldValues(threshTime: Long) {
    clearOldValues(threshTime, (_, _) => ())
  }

  private def currentTime: Long = System.currentTimeMillis

  // For testing

  def getTimeStampedValue(key: A): Option[TimeStampedValue[B]] = {
    Option(internalMap.get(key))
  }

  //获取A对应的时间戳
  def getTimestamp(key: A): Option[Long] = {
    getTimeStampedValue(key).map(_.timestamp)
  }
}
