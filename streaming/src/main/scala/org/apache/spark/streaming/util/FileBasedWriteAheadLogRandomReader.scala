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

import java.io.Closeable
import java.nio.ByteBuffer

import org.apache.hadoop.conf.Configuration

/**
 * A random access reader for reading write ahead log files written using
 * [[org.apache.spark.streaming.util.FileBasedWriteAheadLogWriter]]. Given the file segment info,
 * this reads the record (ByteBuffer) from the log file.
 * 随机读取一个文件的任意地方
 */
private[streaming] class FileBasedWriteAheadLogRandomReader(path: String, conf: Configuration)
  extends Closeable {

  private val instream = HdfsUtils.getInputStream(path, conf) //打开输入流
  private var closed = false

  def read(segment: FileBasedWriteAheadLogSegment): ByteBuffer = synchronized {
    assertOpen()
    instream.seek(segment.offset) //定位到位置
    val nextLength = instream.readInt() //读取多少个字节
    HdfsUtils.checkState(nextLength == segment.length, //校验必须相同
      s"Expected message length to be ${segment.length}, but was $nextLength")
    val buffer = new Array[Byte](nextLength) //创建字节数组
    instream.readFully(buffer) //真正意义的去读取数据
    ByteBuffer.wrap(buffer)
  }

  override def close(): Unit = synchronized {
    closed = true
    instream.close()
  }

  private def assertOpen() {
    HdfsUtils.checkState(!closed, "Stream is closed. Create a new Reader to read from the file.")
  }
}
