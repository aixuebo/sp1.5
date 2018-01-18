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

package org.apache.spark.storage

import java.io.{BufferedOutputStream, FileOutputStream, File, OutputStream}
import java.nio.channels.FileChannel

import org.apache.spark.Logging
import org.apache.spark.serializer.{SerializerInstance, SerializationStream}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.util.Utils

/**
 * A class for writing JVM objects directly to a file on disk. This class allows data to be appended
 * to an existing block and can guarantee atomicity in the case of faults as it allows the caller to
 * revert partial writes.
 * 能够担保元组操作
 * This class does not support concurrent writes. Also, once the writer has been opened it cannot be
 * reopened again.
 *
 * 将key-value的信息被序列化后写入到压缩的输出流compressStream中,该输出流最终会被写入到file文件中
 * 该文件存储的内容是一个partition的结果,比如5个reduce,即5个partition,因此每一个map都要输出5个DiskBlockObjectWriter对象
 */
private[spark] class DiskBlockObjectWriter(
    val blockId: BlockId,
    file: File,//向该文件写入数据
    serializerInstance: SerializerInstance,//写入数据的时候的序列化方式
    bufferSize: Int,//缓冲区间
    compressStream: OutputStream => OutputStream,//写入数据的时候的压缩方式
    syncWrites: Boolean,
    // These write metrics concurrently shared with other active DiskBlockObjectWriters who
    // are themselves performing writes. All updates must be relative.
    writeMetrics: ShuffleWriteMetrics)
  extends OutputStream
  with Logging {

  /** The file channel, used for repositioning / truncating the file. */
  private var channel: FileChannel = null //文件渠道
  private var fos: FileOutputStream = null //文件的输出流
  private var ts: TimeTrackingOutputStream = null //向outputStream中写入字节的时候,封装一层统计,对写入进行统计写入花费的时间,单位是nanoTime
  private var bs: OutputStream = null //对输出流ts进行BufferedOutputStream包装,并且加入压缩包装,即往该输入流中写入的数据都是进行压缩的

  private var objOut: SerializationStream = null //将序列化的内容写入到bs中,bs是一个缓冲流,并且也是一个压缩的流,即序列化后的字节数组被压缩处理后进入bs流
  private var initialized = false //初始化完成
  private var hasBeenClosed = false //是否已经关闭了
  private var commitAndCloseHasBeenCalled = false

  /**
   * Cursors used to represent positions in the file.
   *
   * xxxxxxxx|--------|---       |
   *         ^        ^          ^
   *         |        |        finalPosition 最终的字节位置
   *         |      reportedPosition 每一次更新的字节的位置
   *       initialPosition初始化的字节位置
   *
   * initialPosition: Offset in the file where we start writing. Immutable.
   * reportedPosition: Position at the time of the last update to the write metrics.
   * finalPosition: Offset where we stopped writing. Set on closeAndCommit() then never changed.
   * -----: Current writes to the underlying file.
   * xxxxx: Existing contents of the file.
   */
  private val initialPosition = file.length()
  private var finalPosition: Long = -1 //文件写入的最后一个字节位置
  private var reportedPosition = initialPosition

  /**
   * Keep track of number of records written and also use this to periodically
   * output bytes written since the latter is expensive to do for each record.
   */
  private var numRecordsWritten = 0 //写入多少条记录

  def open(): DiskBlockObjectWriter = {
    if (hasBeenClosed) {
      throw new IllegalStateException("Writer already closed. Cannot be reopened.")
    }
    fos = new FileOutputStream(file, true)//追加操作流
    ts = new TimeTrackingOutputStream(writeMetrics, fos)
    channel = fos.getChannel()
    bs = compressStream(new BufferedOutputStream(ts, bufferSize))
    objOut = serializerInstance.serializeStream(bs) //将序列化的内容写入到bs中,bs是一个缓冲流,并且也是一个压缩的流,即序列化后的字节数组被压缩处理后进入bs流
    initialized = true
    this
  }

  override def close() {
    if (initialized) {
      Utils.tryWithSafeFinally {
        if (syncWrites) {//是否加入同步字符
          // Force outstanding writes to disk and track how long it takes
          objOut.flush()
          val start = System.nanoTime()
          fos.getFD.sync()
          writeMetrics.incShuffleWriteTime(System.nanoTime() - start)
        }
      } {
        objOut.close()
      }

      channel = null
      bs = null
      fos = null
      ts = null
      objOut = null
      initialized = false
      hasBeenClosed = true
    }
  }

  //true表示该文件正在被写入
  def isOpen: Boolean = objOut != null

  /**
   * Flush the partial writes and commit them as a single atomic block.
   */
  def commitAndClose(): Unit = {
    if (initialized) {
      // NOTE: Because Kryo doesn't flush the underlying stream we explicitly flush both the
      //       serializer stream and the lower level stream.
      objOut.flush()
      bs.flush()
      close()
      finalPosition = file.length()
      // In certain compression codecs, more bytes are written after close() is called
      writeMetrics.incShuffleBytesWritten(finalPosition - reportedPosition)
    } else {
      finalPosition = file.length()
    }
    commitAndCloseHasBeenCalled = true
  }


  /**
   * Reverts writes that haven't been flushed yet. Callers should invoke this function
   * when there are runtime exceptions. This method will not throw, though it may be
   * unsuccessful in truncating written data.
   * 还原已经被写入,但是尚未完成的内容,
   * 当运行异常的时候被调用该函数
   * 这个方法不会抛出异常
   *
   */
  def revertPartialWritesAndClose() {
    // Discard current writes. We do this by flushing the outstanding writes and then
    // truncating the file to its initial position.
    try {
      if (initialized) {
        //减少已经统计的内容
        writeMetrics.decShuffleBytesWritten(reportedPosition - initialPosition) //减少已经统计的字节
        writeMetrics.decShuffleRecordsWritten(numRecordsWritten) //减少已经统计的记录数
        //暂时时间没办法还原
        objOut.flush()
        bs.flush()
        close()
      }

      val truncateStream = new FileOutputStream(file, true) //追加该文件
      try {
        truncateStream.getChannel.truncate(initialPosition) //将channel内的文件太大了,进行截断,参数initialPosition字节偏移量位置后面的字节将被删除,不是真的被删除了,数据还在,只是channel不用这部分数据而已
      } finally {
        truncateStream.close()
      }
    } catch {
      case e: Exception =>
        logError("Uncaught exception while reverting partial writes to file " + file, e)
    }
  }

  /**
   * Writes a key-value pair.
   */
  def write(key: Any, value: Any) {
    if (!initialized) {
      open()
    }

    //将key和value进行序列化后,写入到输出流中
    objOut.writeKey(key)
    objOut.writeValue(value)
    recordWritten()//统计一行记录
  }

  override def write(b: Int): Unit = throw new UnsupportedOperationException()

  override def write(kvBytes: Array[Byte], offs: Int, len: Int): Unit = {
    if (!initialized) {
      open()
    }

    bs.write(kvBytes, offs, len)
  }

  /**
   * Notify the writer that a record worth of bytes has been written with OutputStream#write.
   */
  def recordWritten(): Unit = {
    numRecordsWritten += 1 //记录增加了一条key-value记录
    writeMetrics.incShuffleRecordsWritten(1)//增加统计

    if (numRecordsWritten % 32 == 0) {//每隔32条记录,更新统计一下字节内容
      updateBytesWritten()
    }
  }

  /**
   * Returns the file segment of committed data that this Writer has written.
   * This is only valid after commitAndClose() has been called.
   * 创建一个文件段,该文件段是file的一个引用,表示file文件从initialPosition位置开始,一共多少个字节
   */
  def fileSegment(): FileSegment = {
    if (!commitAndCloseHasBeenCalled) {//必须是提交后才能创建一个文件
      throw new IllegalStateException(
        "fileSegment() is only valid after commitAndClose() has been called")
    }
    new FileSegment(file, initialPosition, finalPosition - initialPosition)
  }

  /**
   * Report the number of bytes written in this writer's shuffle write metrics.
   * Note that this is only valid before the underlying streams are closed.
   */
  private def updateBytesWritten() {
    val pos = channel.position()
    writeMetrics.incShuffleBytesWritten(pos - reportedPosition)//当前位置-上一次提交的位置,就是被写入多少个字节
    reportedPosition = pos
  }

  // For testing
  private[spark] override def flush() {
    objOut.flush()
    bs.flush()
  }
}
