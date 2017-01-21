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

import java.io.{ByteArrayOutputStream, IOException}
import java.net.ServerSocket
import java.nio.ByteBuffer

import scala.io.Source

import org.apache.spark.{SparkConf, Logging}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.util.IntParam

/**
 * A helper program that sends blocks of Kryo-serialized text strings out on a socket at a
 * specified rate. Used to feed data into RawInputDStream.
 * 一个帮助程序,给连接的sockt发送序列化的内容,并且发送控制一定速度去发送
 *
 * 开启一个服务器,读取文件内容,用于做为一个数据源来不断的给请求他的socket发送信息,用于测试
 */
private[streaming]
object RawTextSender extends Logging {
  def main(args: Array[String]) {
    if (args.length != 4) {
      // scalastyle:off println
      System.err.println("Usage: RawTextSender <port> <file> <blockSize> <bytesPerSec>")
      // scalastyle:on println
      System.exit(1)
    }
    // Parse the arguments using a pattern match 解析参数  端口、文件路径、数据块大小  流量,即每秒多少字节
    val Array(IntParam(port), file, IntParam(blockSize), IntParam(bytesPerSec)) = args

    // Repeat the input data multiple times to fill in a buffer 可以不断的重复多次读取一个文件,一直到buffer数据块大小为止

    val lines = Source.fromFile(file).getLines().toArray //文件内容按照行分成数组
    val bufferStream = new ByteArrayOutputStream(blockSize + 1000) //设置缓冲区大小的数据输出流
    val ser = new KryoSerializer(new SparkConf()).newInstance()
    val serStream = ser.serializeStream(bufferStream) //对缓冲区的输出内容进行序列化
    var i = 0
    while (bufferStream.size < blockSize) {//不断的向输出流中写入文件内容,直到达到数据块大小为止
      serStream.writeObject(lines(i))
      i = (i + 1) % lines.length //不断的读取一个文件,发送数据
    }
    val array = bufferStream.toByteArray //转换成字节数组

    val countBuf = ByteBuffer.wrap(new Array[Byte](4)) //头文件,写入字节数组大小
    countBuf.putInt(array.length)
    countBuf.flip()

    val serverSocket = new ServerSocket(port) //开启服务器
    logInfo("Listening on port " + port)

    while (true) {
      val socket = serverSocket.accept()
      logInfo("Got a new connection") //来了一个连接
      val out = new RateLimitedOutputStream(socket.getOutputStream, bytesPerSec) //输出流的流量控制
      try {
        while (true) {//写入字节信息,不断的写入信息,因为已经流量控制住了,因此while循环没有想的那么快,因为流量里面会有sleep控制
          out.write(countBuf.array)
          out.write(array)
        }
      } catch {
        case e: IOException =>
          logError("Client disconnected")
      } finally {
        socket.close()
      }
    }
  }
}
