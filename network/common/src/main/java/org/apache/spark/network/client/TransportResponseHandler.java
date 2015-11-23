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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.protocol.ChunkFetchFailure;
import org.apache.spark.network.protocol.ChunkFetchSuccess;
import org.apache.spark.network.protocol.ResponseMessage;
import org.apache.spark.network.protocol.RpcFailure;
import org.apache.spark.network.protocol.RpcResponse;
import org.apache.spark.network.protocol.StreamChunkId;
import org.apache.spark.network.server.MessageHandler;
import org.apache.spark.network.util.NettyUtils;

/**
 * Handler that processes server responses, in response to requests issued from a
 * [[TransportClient]]. It works by tracking the list of outstanding requests (and their callbacks).
 *
 * Concurrency: thread safe and can be called from multiple threads.
 * 处理传输过程中response,一个channel渠道对应一个该处理对象
 */
public class TransportResponseHandler extends MessageHandler<ResponseMessage> {
	
  private final Logger logger = LoggerFactory.getLogger(TransportResponseHandler.class);

  //在哪个渠道上进行的操作
  private final Channel channel;

  //每一个数据块对应一个该数据块的回调函数
  private final Map<StreamChunkId, ChunkReceivedCallback> outstandingFetches;

  //key是每一个请求id,value是该请求对应的回调函数
  private final Map<Long, RpcResponseCallback> outstandingRpcs;

  /** Records the time (in system nanoseconds) that the last fetch or RPC request was sent. 
   * 最后一个fetch抓取或者RPC的时间 
   **/
  private final AtomicLong timeOfLastRequestNs;

  public TransportResponseHandler(Channel channel) {
    this.channel = channel;
    this.outstandingFetches = new ConcurrentHashMap<StreamChunkId, ChunkReceivedCallback>();
    this.outstandingRpcs = new ConcurrentHashMap<Long, RpcResponseCallback>();
    this.timeOfLastRequestNs = new AtomicLong(0);
  }

  //添加一个抓取请求
  public void addFetchRequest(StreamChunkId streamChunkId, ChunkReceivedCallback callback) {
    timeOfLastRequestNs.set(System.nanoTime());
    outstandingFetches.put(streamChunkId, callback);
  }
  
  //移除一个抓取请求
  public void removeFetchRequest(StreamChunkId streamChunkId) {
    outstandingFetches.remove(streamChunkId);
  }

  //为RPC请求添加一个回调函数
  public void addRpcRequest(long requestId, RpcResponseCallback callback) {
    timeOfLastRequestNs.set(System.nanoTime());
    outstandingRpcs.put(requestId, callback);
  }

  //移除一个RPC请求
  public void removeRpcRequest(long requestId) {
    outstandingRpcs.remove(requestId);
  }

  /**
   * Fire the failure callback for all outstanding requests. This is called when we have an
   * uncaught exception or pre-mature connection termination.
   * 有异常
   */
  private void failOutstandingRequests(Throwable cause) {
    for (Map.Entry<StreamChunkId, ChunkReceivedCallback> entry : outstandingFetches.entrySet()) {
      entry.getValue().onFailure(entry.getKey().chunkIndex, cause);
    }
    for (Map.Entry<Long, RpcResponseCallback> entry : outstandingRpcs.entrySet()) {
      entry.getValue().onFailure(cause);
    }

    // It's OK if new fetches appear, as they will fail immediately.
    outstandingFetches.clear();
    outstandingRpcs.clear();
  }

  //
  @Override
  public void channelUnregistered() {
    if (numOutstandingRequests() > 0) {
    	//打印日志,有多少个等待回调的,在连接被关闭的时候
      String remoteAddress = NettyUtils.getRemoteAddress(channel);
      logger.error("Still have {} requests outstanding when connection from {} is closed",
        numOutstandingRequests(), remoteAddress);
      failOutstandingRequests(new IOException("Connection from " + remoteAddress + " closed"));
    }
  }

  //产生移除
  @Override
  public void exceptionCaught(Throwable cause) {
    if (numOutstandingRequests() > 0) {
      String remoteAddress = NettyUtils.getRemoteAddress(channel);
      logger.error("Still have {} requests outstanding when connection from {} is closed",
        numOutstandingRequests(), remoteAddress);
      failOutstandingRequests(cause);
    }
  }

  //收到response信息后,进行处理
  @Override
  public void handle(ResponseMessage message) {
    String remoteAddress = NettyUtils.getRemoteAddress(channel);//服务器地址
    if (message instanceof ChunkFetchSuccess) {//抓取数据块成功
      ChunkFetchSuccess resp = (ChunkFetchSuccess) message;
      ChunkReceivedCallback listener = outstandingFetches.get(resp.streamChunkId);
      if (listener == null) {
        logger.warn("Ignoring response for block {} from {} since it is not outstanding",
          resp.streamChunkId, remoteAddress);
        resp.buffer.release();
      } else {
        outstandingFetches.remove(resp.streamChunkId);
        listener.onSuccess(resp.streamChunkId.chunkIndex, resp.buffer);
        resp.buffer.release();
      }
    } else if (message instanceof ChunkFetchFailure) {
      ChunkFetchFailure resp = (ChunkFetchFailure) message;
      ChunkReceivedCallback listener = outstandingFetches.get(resp.streamChunkId);
      if (listener == null) {
        logger.warn("Ignoring response for block {} from {} ({}) since it is not outstanding",
          resp.streamChunkId, remoteAddress, resp.errorString);
      } else {
        outstandingFetches.remove(resp.streamChunkId);
        listener.onFailure(resp.streamChunkId.chunkIndex, new ChunkFetchFailureException(
          "Failure while fetching " + resp.streamChunkId + ": " + resp.errorString));
      }
    } else if (message instanceof RpcResponse) {
      RpcResponse resp = (RpcResponse) message;
      RpcResponseCallback listener = outstandingRpcs.get(resp.requestId);
      if (listener == null) {
        logger.warn("Ignoring response for RPC {} from {} ({} bytes) since it is not outstanding",
          resp.requestId, remoteAddress, resp.response.length);
      } else {
        outstandingRpcs.remove(resp.requestId);
        listener.onSuccess(resp.response);
      }
    } else if (message instanceof RpcFailure) {
      RpcFailure resp = (RpcFailure) message;
      RpcResponseCallback listener = outstandingRpcs.get(resp.requestId);
      if (listener == null) {
        logger.warn("Ignoring response for RPC {} from {} ({}) since it is not outstanding",
          resp.requestId, remoteAddress, resp.errorString);
      } else {
        outstandingRpcs.remove(resp.requestId);
        listener.onFailure(new RuntimeException(resp.errorString));
      }
    } else {
      throw new IllegalStateException("Unknown response type: " + message.type());
    }
  }

  /** Returns total number of outstanding requests (fetch requests + rpcs) 处理的总请求数*/
  public int numOutstandingRequests() {
    return outstandingFetches.size() + outstandingRpcs.size();
  }

  /** Returns the time in nanoseconds of when the last request was sent out.最后一次访问时间 */
  public long getTimeOfLastRequestNs() {
    return timeOfLastRequestNs.get();
  }
}
