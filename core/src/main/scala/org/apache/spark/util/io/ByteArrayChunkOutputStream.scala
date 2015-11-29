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

package org.apache.spark.util.io

import java.io.OutputStream

import scala.collection.mutable.ArrayBuffer


/**
 * An OutputStream that writes to fixed-size chunks of byte arrays.
 *
 * @param chunkSize size of each chunk, in bytes.每一个chunk包含的字节数
 * 使用chunk方式的一个输出流,由chunk数组存储数据,每一个数组的元素包含参数chunkSize个字节
 */
private[spark]
class ByteArrayChunkOutputStream(chunkSize: Int) extends OutputStream {

  private val chunks = new ArrayBuffer[Array[Byte]] //最终存储字节的chunk数组,每一个元素也是一个数组,约束了该元素的数组只能存储chunkSize个字节
 
  /** Index of the last chunk. Starting with -1 when the chunks array is empty. 
   * 当前最后一个chunk是第几个chunk  
   **/
  private var lastChunkIndex = -1

  /**
   * Next position to write in the last chunk.
   *
   * If this equals chunkSize, it means for next write we need to allocate a new chunk.
   * This can also never be 0.
   */
  private var position = chunkSize //表示当前chunk中已经写入了几个字节了,即下一个字节写入的位置

  override def write(b: Int): Unit = {
    allocateNewChunkIfNeeded()
    chunks(lastChunkIndex)(position) = b.toByte //写入一个字节
    position += 1
  }

  //写入N个字节
  override def write(bytes: Array[Byte], off: Int, len: Int): Unit = {
    var written = 0
    while (written < len) {
      allocateNewChunkIfNeeded()
      val thisBatch = math.min(chunkSize - position, len - written)
      System.arraycopy(bytes, written + off, chunks(lastChunkIndex), position, thisBatch)
      written += thisBatch
      position += thisBatch
    }
  }

  //校验是否要重新分配一个chunk
  @inline
  private def allocateNewChunkIfNeeded(): Unit = {
    if (position == chunkSize) {//当前位置已经是chunkSize个位置了,需要一个新的chunk字节数组
      chunks += new Array[Byte](chunkSize)
      lastChunkIndex += 1
      position = 0
    }
  }

  //返回所有的字节数组
  def toArrays: Array[Array[Byte]] = {
    if (lastChunkIndex == -1) {
      new Array[Array[Byte]](0)
    } else {
      // Copy the first n-1 chunks to the output, and then create an array that fits the last chunk.
      // An alternative would have been returning an array of ByteBuffers, with the last buffer
      // bounded to only the last chunk's position. However, given our use case in Spark (to put
      // the chunks in block manager), only limiting the view bound of the buffer would still
      // require the block manager to store the whole chunk.
      val ret = new Array[Array[Byte]](chunks.size)
      for (i <- 0 until chunks.size - 1) {
        ret(i) = chunks(i)
      }
      if (position == chunkSize) {
        ret(lastChunkIndex) = chunks(lastChunkIndex)
      } else {
        ret(lastChunkIndex) = new Array[Byte](position)
        System.arraycopy(chunks(lastChunkIndex), 0, ret(lastChunkIndex), 0, position)
      }
      ret
    }
  }
}
