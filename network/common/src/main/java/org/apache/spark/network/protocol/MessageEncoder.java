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

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encoder used by the server side to encode server-to-client responses.
 * This encoder is stateless so it is safe to be shared by multiple threads.
 * 编码类
 */
@ChannelHandler.Sharable
public final class MessageEncoder extends MessageToMessageEncoder<Message> {

  private final Logger logger = LoggerFactory.getLogger(MessageEncoder.class);

  /***
   * Encodes a Message by invoking its encode() method. For non-data messages, we will add one
   * ByteBuf to 'out' containing the total frame length, the message type, and the message itself.
   * In the case of a ChunkFetchSuccess, we will also add the ManagedBuffer corresponding to the
   * data to 'out', in order to enable zero-copy transfer.
   * 输出的对象会追加到out中
   */
  @Override
  public void encode(ChannelHandlerContext ctx, Message in, List<Object> out) {
    Object body = null;//body正文对象
    long bodyLength = 0;//body字节大小

    // Only ChunkFetchSuccesses have data besides the header.
    // The body is used in order to enable zero-copy transfer for the payload.
    if (in instanceof ChunkFetchSuccess) {//body正文
      ChunkFetchSuccess resp = (ChunkFetchSuccess) in;
      try {
        bodyLength = resp.buffer.size();//具体数据块字节大小
        body = resp.buffer.convertToNetty();//将数据块转换成Netty方式读取数据
      } catch (Exception e) {
        // Re-encode this message as BlockFetchFailure.
        logger.error(String.format("Error opening block %s for client %s",
          resp.streamChunkId, ctx.channel().remoteAddress()), e);
        encode(ctx, new ChunkFetchFailure(resp.streamChunkId, e.getMessage()), out);
        return;
      }
    }

    Message.Type msgType = in.type();
    // All messages have the frame length, message type, and message itself.
      //header+body的字节大小总数、type类型字节数、message信息字节数
    int headerLength = 8 + msgType.encodedLength() + in.encodedLength();//头字节数,8表示frameLength传输的总字节数,即头字节+body字节总数,+信息分类标示+header头内容总和
    long frameLength = headerLength + bodyLength;//传输的总字节数,即头字节+body字节总数
    
    //分配头空间,写入头信息
    ByteBuf header = ctx.alloc().heapBuffer(headerLength);//创建header头对象字节大小缓冲区
    header.writeLong(frameLength);//设置多少个字节数
    msgType.encode(header);//将type序列化
    in.encode(header);//将header的具体内容序列化
    
    assert header.writableBytes() == 0;//确保header头已经都写完了

    //如果body不是空,则添加到out中
    if (body != null && bodyLength > 0) {
      out.add(new MessageWithHeader(header, body, bodyLength));
    } else {
      out.add(header);
    }
  }

}
