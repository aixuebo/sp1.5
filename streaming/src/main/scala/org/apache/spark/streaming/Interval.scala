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

package org.apache.spark.streaming

//参数表示开始--结束时间
private[streaming]
class Interval(val beginTime: Time, val endTime: Time) {
  def this(beginMs: Long, endMs: Long) = this(new Time(beginMs), new Time(endMs))

  def duration(): Duration = endTime - beginTime //时间间隔

  def + (time: Duration): Interval = {
    new Interval(beginTime + time, endTime + time)
  }

  def - (time: Duration): Interval = {
    new Interval(beginTime - time, endTime - time)
  }

  def < (that: Interval): Boolean = {
    if (this.duration != that.duration) {
      throw new Exception("Comparing two intervals with different durations [" + this + ", "
        + that + "]")
    }
    this.endTime < that.endTime
  }

  def <= (that: Interval): Boolean = (this < that || this == that)

  def > (that: Interval): Boolean = !(this <= that)

  def >= (that: Interval): Boolean = !(this < that)

  override def toString: String = "[" + beginTime + ", " + endTime + "]"
}

private[streaming]
object Interval {
  def currentInterval(duration: Duration): Interval = { //传入周期即可
    val time = new Time(System.currentTimeMillis)
    val intervalBegin = time.floor(duration)
    new Interval(intervalBegin, intervalBegin + duration)
  }
}


