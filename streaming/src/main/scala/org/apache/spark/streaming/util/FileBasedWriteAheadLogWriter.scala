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

import java.io._
import java.nio.ByteBuffer

import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataOutputStream

/**
 * A writer for writing byte-buffers to a write ahead log file.
 * 具体的一个存储日志的文件
 */
private[streaming] class FileBasedWriteAheadLogWriter(path: String, hadoopConf: Configuration)
  extends Closeable {

  private lazy val stream = HdfsUtils.getOutputStream(path, hadoopConf) //打开输出流

  //找到hflush或者sync方法,对输出流进行flush操作
  private lazy val hadoopFlushMethod = {
    // Use reflection to get the right flush operation
    val cls = classOf[FSDataOutputStream]
    Try(cls.getMethod("hflush")).orElse(Try(cls.getMethod("sync"))).toOption
  }

  private var nextOffset = stream.getPos() //当前位置,即下一次要开始写入的位置
  private var closed = false //true表示输出流被关闭

  /** Write the bytebuffer to the log file
    * 向文件中写入字节数组内容
    **/
  def write(data: ByteBuffer): FileBasedWriteAheadLogSegment = synchronized {
    assertOpen() //确保文件输出流已经打开
    data.rewind() // Rewind to ensure all data in the buffer is retrieved
    val lengthToWrite = data.remaining() //要写入多少字节
    val segment = new FileBasedWriteAheadLogSegment(path, nextOffset, lengthToWrite) //表示一次写入的数据记录,记录这段数据在path路径下第几个字节位置开始写入的数据,一共写入了多少个字节,即头文件
    stream.writeInt(lengthToWrite) //先写入多少个字节
    if (data.hasArray) {
      stream.write(data.array())
    } else {
      // If the buffer is not backed by an array, we transfer using temp array
      // Note that despite the extra array copy, this should be faster than byte-by-byte copy
      while (data.hasRemaining) {
        val array = new Array[Byte](data.remaining)
        data.get(array)
        stream.write(array)
      }
    }
    flush()
    nextOffset = stream.getPos()
    segment
  }

  override def close(): Unit = synchronized {
    closed = true
    stream.close()
  }

  private def flush() {
    hadoopFlushMethod.foreach { _.invoke(stream) }
    // Useful for local file system where hflush/sync does not work (HADOOP-7844)
    stream.getWrappedStream.flush()
  }

  private def assertOpen() {
    HdfsUtils.checkState(!closed, "Stream is closed. Create a new Writer to write to file.")
  }
}
