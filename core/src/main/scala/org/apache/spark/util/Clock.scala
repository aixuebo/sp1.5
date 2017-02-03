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

/**
 * An interface to represent clocks, so that they can be mocked out in unit tests.
 */
private[spark] trait Clock {
  def getTimeMillis(): Long //返回当前的时间戳
  def waitTillTime(targetTime: Long): Long //等待一直到targetTime时间戳为止,返回超过targetTime时间戳后的一个新的时间戳
}

/**
 * A clock backed by the actual time from the OS as reported by the `System` API.
 */
private[spark] class SystemClock extends Clock {

  val minPollTime = 25L

  /**
   * @return the same time (milliseconds since the epoch)
   *         as is reported by `System.currentTimeMillis()`
   */
  def getTimeMillis(): Long = System.currentTimeMillis()

  /**
   * @param targetTime block until the current time is at least this value
   * @return current system time when wait has completed
   */
  def waitTillTime(targetTime: Long): Long = {
    var currentTime = 0L
    currentTime = System.currentTimeMillis()

    var waitTime = targetTime - currentTime //还需要等待多久
    if (waitTime <= 0) {//不需要等候
      return currentTime //直接返回当前时间
    }

    val pollTime = math.max(waitTime / 10.0, minPollTime).toLong

    while (true) {
      currentTime = System.currentTimeMillis() //获取当前时间
      waitTime = targetTime - currentTime //还需要等待的时间
      if (waitTime <= 0) {//不需要等候
        return currentTime //直接返回当前时间
      }
      val sleepTime = math.min(waitTime, pollTime)
      Thread.sleep(sleepTime)
    }
    -1
  }
}
