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

import java.util.concurrent.CopyOnWriteArrayList

import scala.collection.JavaConversions._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.spark.Logging
import org.apache.spark.scheduler.SparkListener

/**
 * An event bus which posts events to its listeners.
 * 事件监听器集合
 */
private[spark] trait ListenerBus[L <: AnyRef, E] extends Logging {

  // Marked `private[spark]` for access in tests.
  //事件监听器集合
  private[spark] val listeners = new CopyOnWriteArrayList[L]

  /**
   * Add a listener to listen events. This method is thread-safe and can be called in any thread.
   * 添加一个监听器
   */
  final def addListener(listener: L) {
    listeners.add(listener)
  }

  /**
   * Post the event to all registered listeners. The `postToAll` caller should guarantee calling
   * `postToAll` in the same thread for all events.
   * 产生一个事件
   *
   * 处理流程
   * 循环每一个监听器，让每一个监听器去处理该事件
   */
  final def postToAll(event: E): Unit = {
    // JavaConversions will create a JIterableWrapper if we use some Scala collection functions.
    // However, this method will be called frequently. To avoid the wrapper cost, here ewe use
    // Java Iterator directly.
    val iter = listeners.iterator //循环每一个监听器listener
    while (iter.hasNext) {
      val listener = iter.next()
      try {
        onPostEvent(listener, event) //对每一个监听器listener都趋合理该event事件
      } catch {
        case NonFatal(e) =>
          logError(s"Listener ${Utils.getFormattedClassName(listener)} threw an exception", e)//某一个监听器有异常,不应该影响整体所有监听器,因此只是记录日志
      }
    }
  }

  /**
   * Post an event to the specified listener. `onPostEvent` is guaranteed to be called in the same
   * thread.
   * 真正某一个监听器去处理该事件
   */
  def onPostEvent(listener: L, event: E): Unit

  //从listeners集合中查找参数类型的子类,返回找到的集合
  private[spark] def findListenersByClass[T <: L : ClassTag](): Seq[T] = {
    val c = implicitly[ClassTag[T]].runtimeClass
    listeners.filter(_.getClass == c).map(_.asInstanceOf[T]).toSeq
  }

}
