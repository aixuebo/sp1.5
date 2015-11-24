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

package org.apache.spark.network.nio

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.storage.BlockManager


private[nio]
class BufferMessage(id_ : Int, val buffers: ArrayBuffer[ByteBuffer], var ackId: Int)
  extends Message(Message.BUFFER_MESSAGE, id_) {

  val initialSize = currentSize() //buffer缓冲数组中总字节大小
  
  var gotChunkForSendingOnce = false

  def size: Int = initialSize //buffer缓冲数组中总字节大小

  //buffer缓冲数组中总字节大小
  def currentSize(): Int = {
    if (buffers == null || buffers.isEmpty) {
      0
    } else {
      buffers.map(_.remaining).reduceLeft(_ + _)
    }
  }

  //获取buffer内数据,最多获取参数maxChunkSize个字节,将获取的数据内容组装成MessageChunk返回即可
  //用于发送数据
  def getChunkForSending(maxChunkSize: Int): Option[MessageChunk] = {
    if (maxChunkSize <= 0) {
      throw new Exception("Max chunk size is " + maxChunkSize)
    }

    val security = if (isSecurityNeg) 1 else 0
    if (size == 0 && !gotChunkForSendingOnce) {
      val newChunk = new MessageChunk(
        new MessageChunkHeader(typ, id, 0, 0, ackId, hasError, security, senderAddress), null)
      gotChunkForSendingOnce = true
      return Some(newChunk)
    }

    //如果buffers有内容,则就不断的寻找信息去发送
    while(!buffers.isEmpty) {
      val buffer = buffers(0)//每次获取第一个buffer
      if (buffer.remaining == 0) {//如果第一个buffer内没有数据了,则将buffer从数组中移除
        BlockManager.dispose(buffer)
        buffers -= buffer
      } else {
        //获取新的buffer缓冲区
        val newBuffer = if (buffer.remaining <= maxChunkSize) {
          buffer.duplicate()
        } else {
          buffer.slice().limit(maxChunkSize).asInstanceOf[ByteBuffer]
        }
        //将buffer位置移动
        buffer.position(buffer.position + newBuffer.remaining)
        //将newbuffer的内容组装成MessageChunk发送出去
        val newChunk = new MessageChunk(new MessageChunkHeader(
          typ, id, size, newBuffer.remaining, ackId,
          hasError, security, senderAddress), newBuffer)
        gotChunkForSendingOnce = true
        return Some(newChunk)
      }
    }
    None
  }

  //用于接收数据块
  def getChunkForReceiving(chunkSize: Int): Option[MessageChunk] = {
    // STRONG ASSUMPTION: BufferMessage created when receiving data has ONLY ONE data buffer
    if (buffers.size > 1) {
      throw new Exception("Attempting to get chunk from message with multiple data buffers")
    }
    val buffer = buffers(0)
    val security = if (isSecurityNeg) 1 else 0
    if (buffer.remaining > 0) {
      if (buffer.remaining < chunkSize) {
        throw new Exception("Not enough space in data buffer for receiving chunk")
      }
      val newBuffer = buffer.slice().limit(chunkSize).asInstanceOf[ByteBuffer]
      buffer.position(buffer.position + newBuffer.remaining)
      val newChunk = new MessageChunk(new MessageChunkHeader(
          typ, id, size, newBuffer.remaining, ackId, hasError, security, senderAddress), newBuffer)
      return Some(newChunk)
    }
    None
  }

  //每一个buffer归为,可以重新开始
  def flip() {
    buffers.foreach(_.flip)
  }

  //是否需要回复,true表示需要
  def hasAckId(): Boolean = ackId != 0

  //false表示还有buffer有内容没传输,
  def isCompletelyReceived: Boolean = !buffers(0).hasRemaining

  override def toString: String = {
    if (hasAckId) {
      "BufferAckMessage(aid = " + ackId + ", id = " + id + ", size = " + size + ")"
    } else {
      "BufferMessage(id = " + id + ", size = " + size + ")"
    }
  }
}
