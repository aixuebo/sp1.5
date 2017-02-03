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

package org.apache.spark.streaming.util

import org.apache.spark.Logging
import org.apache.spark.util.{Clock, SystemClock}

//一定周期period内,调用一次callback函数,函数需要传入一个时间
private[streaming]
class RecurringTimer(clock: Clock, period: Long, callback: (Long) => Unit, name: String)
  extends Logging {

  //一个线程不断调用loop
  private val thread = new Thread("RecurringTimer - " + name) {
    setDaemon(true)
    override def run() { loop }
  }

  @volatile private var prevTime = -1L //上一次的时间
  @volatile private var nextTime = -1L //下一次的时间
  @volatile private var stopped = false

  /**
   * Get the time when this timer will fire if it is started right now.
   * The time will be a multiple of this timer's period and more than
   * current system time.
   * 返回值是比当前时间戳大的一个时间戳,而具体的时间戳是大整数倍period周期的一个时间戳
   */
  def getStartTime(): Long = {
    //math.floor(clock.getTimeMillis().toDouble / period) 表示当前时间 是period的几个整数倍,比如当前是105,周期是20,则返回5
    //结果+1 表示肯定能超过一个周期
    //然后结果*period 表示若干个周期后的时间是什么
    (math.floor(clock.getTimeMillis().toDouble / period) + 1).toLong * period
  }

  /**
   * Get the time when the timer will fire if it is restarted right now.
   * This time depends on when the timer was started the first time, and was stopped
   * for whatever reason. The time must be a multiple of this timer's period and
   * more than current time.
   */
  def getRestartTime(originalStartTime: Long): Long = {
    val gap = clock.getTimeMillis() - originalStartTime
    (math.floor(gap.toDouble / period).toLong + 1) * period + originalStartTime
  }

  /**
   * Start at the given start time.
   */
  def start(startTime: Long): Long = synchronized {
    nextTime = startTime
    thread.start()
    logInfo("Started timer for " + name + " at time " + nextTime)
    nextTime
  }

  /**
   * Start at the earliest time it can start based on the period.
   */
  def start(): Long = {
    start(getStartTime())
  }

  /**
   * Stop the timer, and return the last time the callback was made.
   * interruptTimer = true will interrupt the callback
   * if it is in progress (not guaranteed to give correct time in this case).
   */
  def stop(interruptTimer: Boolean): Long = synchronized {
    if (!stopped) {
      stopped = true
      if (interruptTimer) {
        thread.interrupt()
      }
      thread.join()
      logInfo("Stopped timer for " + name + " after time " + prevTime)
    }
    prevTime
  }

  /**
   * Repeatedly call the callback every interval.
   */
  private def loop() {
    try {
      while (!stopped) {
        clock.waitTillTime(nextTime) //等待一直到nextTime时间为止
        callback(nextTime) //每一个周期内都调用call back函数
        prevTime = nextTime
        nextTime += period //定义下一个周期时间
        logDebug("Callback for " + name + " called at time " + prevTime)
      }
    } catch {
      case e: InterruptedException =>
    }
  }
}

private[streaming]
object RecurringTimer extends Logging {

  def main(args: Array[String]) {
    var lastRecurTime = 0L
    val period = 1000

    def onRecur(time: Long) {
      val currentTime = System.currentTimeMillis()
      logInfo("" + currentTime + ": " + (currentTime - lastRecurTime))
      lastRecurTime = currentTime
    }
    val timer = new RecurringTimer(new SystemClock(), period, onRecur, "Test")
    timer.start()
    Thread.sleep(30 * 1000)
    timer.stop(true)
  }
}

