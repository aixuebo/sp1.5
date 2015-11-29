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

package org.apache.spark.util.logging

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.Logging

/**
 * Defines the policy based on which [[org.apache.spark.util.logging.RollingFileAppender]] will
 * generate rolling files.
 * 文件切换的策略类
 */
private[spark] trait RollingPolicy {

  /** Whether rollover should be initiated at this moment 
   * true表示应该去回滚切换文件了 
   **/
  def shouldRollover(bytesToBeWritten: Long): Boolean

  /** Notify that rollover has occurred 
   * 通知已经完成回滚
   **/
  def rolledOver()

  /** Notify that bytes have been written 
   *  设置当前文件又被写入了多少个字节
   **/
  def bytesWritten(bytes: Long)

  /** Get the desired name of the rollover file 
   *  获取已经滚回的文件名称 
   **/
  def generateRolledOverFileSuffix(): String
}

/**
 * Defines a [[org.apache.spark.util.logging.RollingPolicy]] by which files will be rolled
 * over at a fixed interval.
 * 基于时间的文件切换方案
 */
private[spark] class TimeBasedRollingPolicy(
    var rolloverIntervalMillis: Long,//滚动的时间周期
    rollingFileSuffixPattern: String,//SimpleDateFormat的格式化容器参数
    checkIntervalConstraint: Boolean = true   // set to false while testing,true表示要检查时间周期,不允许小于最小周期
  ) extends RollingPolicy with Logging {

  import TimeBasedRollingPolicy._ //导入object对象
  
  //如果需要校验时间间隔约束,则去校验滚动周期是否比最小的周期还要小
  if (checkIntervalConstraint && rolloverIntervalMillis < MINIMUM_INTERVAL_SECONDS * 1000L) {
    //说明时间周期太小了,打印日志,并且设置时间周期为最小周期
    logWarning(s"Rolling interval [${rolloverIntervalMillis/1000L} seconds] is too small. " +
      s"Setting the interval to the acceptable minimum of $MINIMUM_INTERVAL_SECONDS seconds.")
    rolloverIntervalMillis = MINIMUM_INTERVAL_SECONDS * 1000L
  }

  @volatile private var nextRolloverTime = calculateNextRolloverTime() //计算下次回滚文件时间戳
  private val formatter = new SimpleDateFormat(rollingFileSuffixPattern)

  /** Should rollover if current time has exceeded next rollover time
   * true表示应该去回滚文件了  
   **/
  def shouldRollover(bytesToBeWritten: Long): Boolean = {
    System.currentTimeMillis > nextRolloverTime
  }

  /** Rollover has occurred, so find the next time to rollover
   * 回滚文件之后的操作  
   **/
  def rolledOver() {
    nextRolloverTime = calculateNextRolloverTime() //设置下一次回滚时间
    logDebug(s"Current time: ${System.currentTimeMillis}, next rollover time: " + nextRolloverTime)
  }

  //写了多少个字节
  def bytesWritten(bytes: Long) { }  // nothing to do

  //计算一下次滚动的时间是多少,时间间隔略小于rolloverIntervalMillis
  private def calculateNextRolloverTime(): Long = {
    val now = System.currentTimeMillis()
    val targetTime = (
      math.ceil(now.toDouble / rolloverIntervalMillis) * rolloverIntervalMillis
    ).toLong
    logDebug(s"Next rollover time is $targetTime")
    targetTime
  }

  //产生时间格式的字符串
  def generateRolledOverFileSuffix(): String = {
    formatter.format(Calendar.getInstance.getTime)
  }
}

private[spark] object TimeBasedRollingPolicy {
  val MINIMUM_INTERVAL_SECONDS = 60L  // 1 minute 最小时间间隔,单位是秒
}

/**
 * Defines a [[org.apache.spark.util.logging.RollingPolicy]] by which files will be rolled
 * over after reaching a particular size.
 * 基于文件大小的文件切换方案
 */
private[spark] class SizeBasedRollingPolicy(
    var rolloverSizeBytes: Long,//文件多少字节时,需要被切换
    checkSizeConstraint: Boolean = true     // set to false while testing,true表示要严格约束文件大小的参数.必须不能小于最小切换文件大小值
  ) extends RollingPolicy with Logging {

  import SizeBasedRollingPolicy._
  if (checkSizeConstraint && rolloverSizeBytes < MINIMUM_SIZE_BYTES) {//严格校验文件切换大小是否比最小值还要小
    logWarning(s"Rolling size [$rolloverSizeBytes bytes] is too small. " +
      s"Setting the size to the acceptable minimum of $MINIMUM_SIZE_BYTES bytes.")
    rolloverSizeBytes = MINIMUM_SIZE_BYTES
  }

  @volatile private var bytesWrittenSinceRollover = 0L
  val formatter = new SimpleDateFormat("--yyyy-MM-dd--HH-mm-ss--SSSS")

  /** Should rollover if the next set of bytes is going to exceed the size limit
   * true表示是时候切换文件了 
   **/
  def shouldRollover(bytesToBeWritten: Long): Boolean = {
    logInfo(s"$bytesToBeWritten + $bytesWrittenSinceRollover > $rolloverSizeBytes")
    bytesToBeWritten + bytesWrittenSinceRollover > rolloverSizeBytes
  }

  /** Rollover has occurred, so reset the counter
   *  当切换发生后,重新初始化被写入的文件字节为0 
   * */
  def rolledOver() {
    bytesWrittenSinceRollover = 0
  }

  /** Increment the bytes that have been written in the current file 
   *  设置当前文件又被写入了多少个字节
   **/
  def bytesWritten(bytes: Long) {
    bytesWrittenSinceRollover += bytes
  }

  /** Get the desired name of the rollover file 
   *  文件回滚时候的文件名
   **/
  def generateRolledOverFileSuffix(): String = {
    formatter.format(Calendar.getInstance.getTime)
  }
}

private[spark] object SizeBasedRollingPolicy {
  val MINIMUM_SIZE_BYTES = RollingFileAppender.DEFAULT_BUFFER_SIZE * 10 //默认文件被切换时候的文件大小
}

