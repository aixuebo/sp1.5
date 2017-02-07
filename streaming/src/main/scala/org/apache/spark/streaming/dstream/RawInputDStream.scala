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

package org.apache.spark.streaming.dstream

import org.apache.spark.{Logging, SparkEnv}
import org.apache.spark.storage.{StorageLevel, StreamBlockId}
import org.apache.spark.streaming.StreamingContext

import scala.reflect.ClassTag

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{ReadableByteChannel, SocketChannel}
import java.io.EOFException
import java.util.concurrent.ArrayBlockingQueue
import org.apache.spark.streaming.receiver.Receiver


/**
 * An input stream that reads blocks of serialized objects from a given network address.
 * The blocks will be inserted directly into the block store. This is the fastest way to get
 * data into Spark Streaming, though it requires the sender to batch data and serialize it
 * in the format that the system is configured with.
 * 启动一个socket客户端去读取数据,数据的内容就是原生内容作存储,不进行字符转换,因此存储的就是字节数组
 */
private[streaming]
class RawInputDStream[T: ClassTag](
    @transient ssc_ : StreamingContext,
    host: String,//要连接哪个host去获取数据
    port: Int,
    storageLevel: StorageLevel //接收的数据如何存储
  ) extends ReceiverInputDStream[T](ssc_ ) with Logging {

  def getReceiver(): Receiver[T] = {
    new RawNetworkReceiver(host, port, storageLevel).asInstanceOf[Receiver[T]]
  }
}

/**
 * 网络接收器,接收数据
 */
private[streaming]
class RawNetworkReceiver(host: String, port: Int, storageLevel: StorageLevel)
  extends Receiver[Any](storageLevel) with Logging {

  var blockPushingThread: Thread = null

  def onStart() {
    // Open a socket to the target address and keep reading from it
    logInfo("Connecting to " + host + ":" + port)
    val channel = SocketChannel.open() //客户端渠道,从该渠道获取数据
    channel.configureBlocking(true)
    channel.connect(new InetSocketAddress(host, port)) //连接host
    logInfo("Connected to " + host + ":" + port)

    val queue = new ArrayBlockingQueue[ByteBuffer](2) //阻塞队列.队列中存储的都是从socket中获取的全部原生的数据

    blockPushingThread = new Thread {
      setDaemon(true)
      override def run() {
        var nextBlockNumber = 0
        while (true) {
          val buffer = queue.take()
          nextBlockNumber += 1
          store(buffer) //存储原生的数据
        }
      }
    }
    blockPushingThread.start()

    val lengthBuffer = ByteBuffer.allocate(4) //用于获取socket中数据的长度length
    while (true) {
      lengthBuffer.clear()
      readFully(channel, lengthBuffer) //从socket中获取4个字节数据
      lengthBuffer.flip()
      val length = lengthBuffer.getInt() //查看获取的数据length

      val dataBuffer = ByteBuffer.allocate(length) //创建数据的缓冲区
      readFully(channel, dataBuffer) //读取数据
      dataBuffer.flip()
      logInfo("Read a block with " + length + " bytes")
      queue.put(dataBuffer) //将读取的数据添加到队列中
    }
  }

  def onStop() {
    if (blockPushingThread != null) blockPushingThread.interrupt()
  }

  /** Read a buffer fully from a given Channel */
  private def readFully(channel: ReadableByteChannel, dest: ByteBuffer) {
    while (dest.position < dest.limit) {//一直读取,直到读取要求的数据为止
      if (channel.read(dest) == -1) {
        throw new EOFException("End of channel")
      }
    }
  }
}
