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

import scala.annotation.tailrec

import java.io.OutputStream
import java.util.concurrent.TimeUnit._

import org.apache.spark.Logging

//向OutputStream中写入信息,desiredBytesPerSec表示流量伐值,一秒内不允许超过该流量
private[streaming]
class RateLimitedOutputStream(out: OutputStream, desiredBytesPerSec: Int)
  extends OutputStream
  with Logging {

  require(desiredBytesPerSec > 0)

  private val SYNC_INTERVAL = NANOSECONDS.convert(10, SECONDS) //10秒转换成纳秒,10秒一个周期
  private val CHUNK_SIZE = 8192 //每一个数据块大小

  private var lastSyncTime = System.nanoTime //时间戳
  private var bytesWrittenSinceSync = 0L //在流量单位时间内,已经写入多少个字节了

  override def write(b: Int) {
    waitToWrite(1) //控制流量
    out.write(b)
  }

  override def write(bytes: Array[Byte]) {
    write(bytes, 0, bytes.length)
  }

  @tailrec
  override final def write(bytes: Array[Byte], offset: Int, length: Int) {
    val writeSize = math.min(length - offset, CHUNK_SIZE) //计算写入多少个字节

    if (writeSize > 0) {
      waitToWrite(writeSize) //控制流量
      out.write(bytes, offset, writeSize)
      write(bytes, offset + writeSize, length) //因为每一次只能最多写入CHUNK_SIZE个字节,因此可能一次写不完所有的length,因此要不断的调用write方法
    }
  }

  override def flush() {
    out.flush()
  }

  override def close() {
    out.close()
  }

  //控制流量 参数表示此时要写入多少个字节
  @tailrec
  private def waitToWrite(numBytes: Int) {
    val now = System.nanoTime
    val elapsedNanosecs = math.max(now - lastSyncTime, 1) //经过了多少纳秒
    val rate = bytesWrittenSinceSync.toDouble * 1000000000 / elapsedNanosecs //即一秒内有已经产生多少流量bytesWrittenSinceSync/elapsedNanosecs  表示每一纳秒已经流过多少字节, *1000000000表示一秒流了多少
    if (rate < desiredBytesPerSec) {//没有超过流量
      // It's okay to write; just update some variables and return
      bytesWrittenSinceSync += numBytes //增加写入的数据量
      if (now > lastSyncTime + SYNC_INTERVAL) {//重置流量
        // Sync interval has passed; let's resync
        lastSyncTime = now
        bytesWrittenSinceSync = numBytes
      }
    } else {//说明超过了流量
      // Calculate how much time we should sleep to bring ourselves to the desired rate.
      val targetTimeInMillis = bytesWrittenSinceSync * 1000 / desiredBytesPerSec
      val elapsedTimeInMillis = elapsedNanosecs / 1000000
      val sleepTimeInMillis = targetTimeInMillis - elapsedTimeInMillis
      if (sleepTimeInMillis > 0) {
        logTrace("Natural rate is " + rate + " per second but desired rate is " +
          desiredBytesPerSec + ", sleeping for " + sleepTimeInMillis + " ms to compensate.")
        Thread.sleep(sleepTimeInMillis)
      }
      waitToWrite(numBytes)
    }
  }
}
