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

package org.apache.spark.network.server;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.ManagedBuffer;

import com.google.common.base.Preconditions;

/**
 * StreamManager which allows registration of an Iterator&lt;ManagedBuffer&gt;, which are individually
 * fetched as chunks by the client. Each registered buffer is one chunk.
 * 服务器这边注册一个流ID,该流ID包含了该服务器的数据块集合要发送给哪些客户端
 */
public class OneForOneStreamManager extends StreamManager {
  private final Logger logger = LoggerFactory.getLogger(OneForOneStreamManager.class);

  private final AtomicLong nextStreamId;//通过累加的方式产生数据流ID,StreamId
  
  //key是streamId,value是该数据流上要传输的数据块集合对象StreamState
  private final ConcurrentHashMap<Long, StreamState> streams;

  /** State of a single stream. 
   * 该对象包含一个数据流Id对应要传输到客户端的所有数据块信息集合
   **/
  private static class StreamState {
    final Iterator<ManagedBuffer> buffers;//要传输到客户端的所有数据块信息集合

    // The channel associated to the stream 该数据流关联的渠道,即该数据流要向哪个客户端传输数据
    Channel associatedChannel = null;

    // Used to keep track of the index of the buffer that the user has retrieved, just to ensure
    // that the caller only requests each chunk one at a time, in order.
    //服务器会按顺序一个一个数据块发给客户端,因此该值表示当前要准备要发给客户端的是哪个数据块
    int curChunk = 0;

    StreamState(Iterator<ManagedBuffer> buffers) {
      this.buffers = Preconditions.checkNotNull(buffers);
    }
  }

  public OneForOneStreamManager() {
    // For debugging purposes, start with a random stream id to help identifying different streams.
    // This does not need to be globally unique, only unique to this class.
    nextStreamId = new AtomicLong((long) new Random().nextInt(Integer.MAX_VALUE) * 1000);
    streams = new ConcurrentHashMap<Long, StreamState>();
  }

  //向数据流关联一个要接收数据的客户端流渠道channel
  @Override
  public void registerChannel(Channel channel, long streamId) {
    if (streams.containsKey(streamId)) {
      streams.get(streamId).associatedChannel = channel;
    }
  }

  /**
   * 客户端要获取下一个数据块,返回该数据块对应的内容
   */
  @Override
  public ManagedBuffer getChunk(long streamId, int chunkIndex) {
    StreamState state = streams.get(streamId);
    //校验客户端请求的下一个数据块是否正确
    if (chunkIndex != state.curChunk) {
      throw new IllegalStateException(String.format(
        "Received out-of-order chunk index %s (expected %s)", chunkIndex, state.curChunk));
    } else if (!state.buffers.hasNext()) {//校验确保服务器还有该chunkIndex下标的数据块存在
      throw new IllegalStateException(String.format(
        "Requested chunk index beyond end %s", chunkIndex));
    }
    
    //设置下一次数据块位置
    state.curChunk += 1;
    //获取准备要传输的数据块
    ManagedBuffer nextChunk = state.buffers.next();

    //如果数据块传输完了,则从内存中删除该数据流对应的映射
    if (!state.buffers.hasNext()) {
      logger.trace("Removing stream id {}", streamId);
      streams.remove(streamId);
    }

    return nextChunk;
  }

  //与客户端channel失去联系
  @Override
  public void connectionTerminated(Channel channel) {
    // Close all streams which have been associated with the channel.
    for (Map.Entry<Long, StreamState> entry: streams.entrySet()) {
      StreamState state = entry.getValue();
      if (state.associatedChannel == channel) {//找到与该客户端有传输联系的流
        streams.remove(entry.getKey());//使该流从关联中移除

        // Release all remaining buffers.
        while (state.buffers.hasNext()) {
          state.buffers.next().release();//释放
        }
      }
    }
  }

  /**
   * Registers a stream of ManagedBuffers which are served as individual chunks one at a time to
   * callers. Each ManagedBuffer will be release()'d after it is transferred on the wire. If a
   * client connection is closed before the iterator is fully drained, then the remaining buffers
   * will all be release()'d.
   * 注册一个数据流到内存管理起来
   * 该数据流ID对应要传输的一组Block内容集合
   * 每一个数据块内容ManagedBuffer被传输到调用者后,都会被release,
   * 如果一个客户端连接在数据块没有全部传输完前被close,则剩余的buffer也将会被release操作
   */
  public long registerStream(Iterator<ManagedBuffer> buffers) {
    long myStreamId = nextStreamId.getAndIncrement(); //通过累加的方式产生数据流ID,StreamId
    streams.put(myStreamId, new StreamState(buffers));
    return myStreamId;
  }
}
