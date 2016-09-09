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

package org.apache.spark.network.protocol;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;

/**
 * A wrapper message that holds two separate pieces (a header and a body).
 *
 * The header must be a ByteBuf, while the body can be a ByteBuf or a FileRegion.
 * 写入header和body正文信息
 */
class MessageWithHeader extends AbstractReferenceCounted implements FileRegion {

  private final ByteBuf header;//header头所包含的字节数组
  private final int headerLength;//header头的字节长度

  private final Object body;//body正文所包含的字节数组
  private final long bodyLength;//body正文的字节长度
  
  private long totalBytesTransferred;//当前已经传输了多少个字节

  /**
   * 
   * @param header header头所包含的字节数组
   * @param body body正文所包含的字节数组
   * @param bodyLength body正文的字节长度
   */
  MessageWithHeader(ByteBuf header, Object body, long bodyLength) {
    Preconditions.checkArgument(body instanceof ByteBuf || body instanceof FileRegion,
      "Body must be a ByteBuf or a FileRegion.");
    this.header = header;
    this.headerLength = header.readableBytes();
    this.body = body;
    this.bodyLength = bodyLength;
  }

  //总字节数
  @Override
  public long count() {
    return headerLength + bodyLength;
  }

  @Override
  public long position() {
    return 0;
  }

  //已经发送了多少个字节
  @Override
  public long transfered() {
    return totalBytesTransferred;
  }

  /**
   * This code is more complicated than you would think because we might require multiple
   * transferTo invocations in order to transfer a single MessageWithHeader to avoid busy waiting.
   *
   * The contract is that the caller will ensure position is properly set to the total number
   * of bytes transferred so far (i.e. value returned by transfered()).
   * 从position位置开始,将buffer数据写入到target中
   */
  @Override
  public long transferTo(final WritableByteChannel target, final long position) throws IOException {
	  //一定从上次记录的字节位置开始传输
    Preconditions.checkArgument(position == totalBytesTransferred, "Invalid position.");
    // Bytes written for header in this call.传输header头信息
    long writtenHeader = 0;
    if (header.readableBytes() > 0) {
      writtenHeader = copyByteBuf(header, target);//传输的字节数
      totalBytesTransferred += writtenHeader;//记录传输的总字节数
      if (header.readableBytes() > 0) {//说明header还有数据没有传送完
        return writtenHeader;//返回已经传输了多少个字节
      }
    }

    // Bytes written for body in this call.写入body内容到target中
    long writtenBody = 0;
    if (body instanceof FileRegion) {
      writtenBody = ((FileRegion) body).transferTo(target, totalBytesTransferred - headerLength);//totalBytesTransferred - headerLength 表示现在已经抓去的字节数 - header头的字节数 ,即body读取了多少字节,从这个位置开始继续读
    } else if (body instanceof ByteBuf) {
      writtenBody = copyByteBuf((ByteBuf) body, target);
    }
    totalBytesTransferred += writtenBody;

    return writtenHeader + writtenBody;
  }

  @Override
  protected void deallocate() {
    header.release();
    ReferenceCountUtil.release(body);
  }

  /**
   * @param buf
   * @param target
   * 将buf的内容写入到target中
   * 返回真正写入多少字节到target中
   */
  private int copyByteBuf(ByteBuf buf, WritableByteChannel target) throws IOException {
    int written = target.write(buf.nioBuffer());//将buf的内容写入到target中
    buf.skipBytes(written);//因为写入了written个字节,因此buffer的指针需要移动written位置
    return written;//返回写入多少字节到target中
  }
}
