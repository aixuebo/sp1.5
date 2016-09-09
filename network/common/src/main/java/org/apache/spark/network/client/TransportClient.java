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

package org.apache.spark.network.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.protocol.ChunkFetchRequest;
import org.apache.spark.network.protocol.RpcRequest;
import org.apache.spark.network.protocol.StreamChunkId;
import org.apache.spark.network.util.NettyUtils;

/**
 * Client for fetching consecutive chunks of a pre-negotiated stream. This API is intended to allow
 * efficient transfer of a large amount of data, broken up into chunks with size ranging from
 * hundreds of KB to a few MB.
 * 客户端连续抓去数据块流,这个api允许有效的传输一个大的数据,将大数据拆分成若干连续的块
 *
 * Note that while this client deals with the fetching of chunks from a stream (i.e., data plane),
 * the actual setup of the streams is done outside the scope of the transport layer. The convenience
 * method "sendRPC" is provided to enable control plane communication between the client and server
 * to perform this setup.
 *
 *
 * For example, a typical workflow might be:
 * client.sendRPC(new OpenFile("/foo")) --&gt; returns StreamId = 100
 * client.fetchChunk(streamId = 100, chunkIndex = 0, callback)
 * client.fetchChunk(streamId = 100, chunkIndex = 1, callback)
 * ...
 * client.sendRPC(new CloseStream(100))
 *
 * Construct an instance of TransportClient using {@link TransportClientFactory}. A single
 * TransportClient may be used for multiple streams, but any given stream must be restricted to a
 * single client, in order to avoid out-of-order responses.
 *
 * NB: This class is used to make requests to the server, while {@link TransportResponseHandler} is
 * responsible for handling responses from the server.
 *
 * Concurrency: thread safe and can be called from multiple threads.
 * 真正的一个客户端与ip:port的链接
 */
public class TransportClient implements Closeable {
  private final Logger logger = LoggerFactory.getLogger(TransportClient.class);

  private final Channel channel;//客户端与ip:port交流的通道
  private final TransportResponseHandler handler;//处理每次请求request和返回值response的工具类

  public TransportClient(Channel channel, TransportResponseHandler handler) {
    this.channel = Preconditions.checkNotNull(channel);
    this.handler = Preconditions.checkNotNull(handler);
  }

  public boolean isActive() {
    return channel.isOpen() || channel.isActive();
  }

  //获取服务器地址
  public SocketAddress getSocketAddress() {
    return channel.remoteAddress();
  }

  /**
   * Requests a single chunk from the remote side, from the pre-negotiated streamId.
   *
   * Chunk indices go from 0 onwards. It is valid to request the same chunk multiple times, though
   * some streams may not support this.
   *
   * Multiple fetchChunk requests may be outstanding simultaneously, and the chunks are guaranteed
   * to be returned in the same order that they were requested, assuming only a single
   * TransportClient is used to fetch the chunks.
   *
   * @param streamId Identifier that refers to a stream in the remote StreamManager. This should
   *                 be agreed upon by client and server beforehand.
   * @param chunkIndex 0-based index of the chunk to fetch
   * @param callback Callback invoked upon successful receipt of chunk, or upon any failure.
   * 抓取哪个流ID的第几个数据块
   */
  public void fetchChunk(
      long streamId,//抓取哪个流ID
      final int chunkIndex,//抓取哪个流ID的第几个数据块
      final ChunkReceivedCallback callback) {//回调函数
	//获取服务器地址
    final String serverAddr = NettyUtils.getRemoteAddress(channel);
    final long startTime = System.currentTimeMillis();
    //打印日志,准备抓取哪个流ID的第几个数据块
    logger.debug("Sending fetch chunk request {} to {}", chunkIndex, serverAddr);

    //创建StreamChunkId对象,并且将StreamChunkId与回调函数进行关联绑定
    final StreamChunkId streamChunkId = new StreamChunkId(streamId, chunkIndex);
    handler.addFetchRequest(streamChunkId, callback);

    
    channel.writeAndFlush(new ChunkFetchRequest(streamChunkId)).addListener(
      new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (future.isSuccess()) {//操作成功获取结果,则打印消耗的时间
            long timeTaken = System.currentTimeMillis() - startTime;
            logger.trace("Sending request {} to {} took {} ms", streamChunkId, serverAddr,
              timeTaken);
          } else {//没有成功获取,则打印异常日志
            String errorMsg = String.format("Failed to send request %s to %s: %s", streamChunkId,
              serverAddr, future.cause());
            logger.error(errorMsg, future.cause());
            handler.removeFetchRequest(streamChunkId);
            channel.close();
            try {
              //回调函数中记录失败原因
              callback.onFailure(chunkIndex, new IOException(errorMsg, future.cause()));
            } catch (Exception e) {
              logger.error("Uncaught exception in RPC response callback handler!", e);
            }
          }
        }
      });
  }

  /**
   * Sends an opaque message to the RpcHandler on the server-side. The callback will be invoked
   * with the server's response or upon any failure.
   * 向channel对应的服务器发送信息message,完成后回调callback函数
   * 该方法是异步的发送数据,一旦方法结束后,会像对应的callback类写入内容
   */
  public void sendRpc(byte[] message, final RpcResponseCallback callback) {
    final String serverAddr = NettyUtils.getRemoteAddress(channel);//channel的服务器地址
    final long startTime = System.currentTimeMillis();
    logger.trace("Sending RPC to {}", serverAddr);//向服务器发送RPC信息

    final long requestId = Math.abs(UUID.randomUUID().getLeastSignificantBits());//随机产生一个请求ID
    handler.addRpcRequest(requestId, callback);

      //像该渠道发送请求,请求包含请求id和信息,同时添加监听,当请求完成的时候,确定是成功完成还是失败完成
    channel.writeAndFlush(new RpcRequest(requestId, message)).addListener(
      new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (future.isSuccess()) {
        	  //成功,则打印消耗时间
            long timeTaken = System.currentTimeMillis() - startTime;
            logger.trace("Sending request {} to {} took {} ms", requestId, serverAddr, timeTaken);
          } else {
            String errorMsg = String.format("Failed to send RPC %s to %s: %s", requestId,
              serverAddr, future.cause());
            logger.error(errorMsg, future.cause());
            handler.removeRpcRequest(requestId);//失败完成则移除该请求正在等待的回复
            channel.close();//关闭与服务器的通道
            try {
              //失败,则打印异常日志
              callback.onFailure(new IOException(errorMsg, future.cause()));
            } catch (Exception e) {
              logger.error("Uncaught exception in RPC response callback handler!", e);
            }
          }
        }
      });
  }

  /**
   * Synchronously sends an opaque message to the RpcHandler on the server-side, waiting for up to
   * a specified timeout for a response.
   * 同步发送一个信息到服务器,同时在timeout时间后要返回数据
   */
  public byte[] sendRpcSync(byte[] message, long timeoutMs) {
    final SettableFuture<byte[]> result = SettableFuture.create();

      //异步发送数据
    sendRpc(message, new RpcResponseCallback() {
      @Override
      public void onSuccess(byte[] response) {
        result.set(response);
      }

      @Override
      public void onFailure(Throwable e) {
        result.setException(e);
      }
    });

      //获取返回结果,该方法是同步方法,在超市时间内必须获取到返回值,如果超时还为获取返回值,则返回null
    try {
      return result.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      throw Throwables.propagate(e.getCause());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void close() {
    // close is a local operation and should finish with milliseconds; timeout just to be safe
    channel.close().awaitUninterruptibly(10, TimeUnit.SECONDS);//关闭与服务器的通道
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("remoteAdress", channel.remoteAddress())
      .add("isActive", isActive())
      .toString();
  }
}
