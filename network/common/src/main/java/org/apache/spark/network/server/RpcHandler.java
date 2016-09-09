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

import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;

/**
 * Handler for sendRPC() messages sent by {@link org.apache.spark.network.client.TransportClient}s.
 * 处理TransportClient发送过来的rpc请求
 */
public abstract class RpcHandler {
  /**
   * Receive a single RPC message. Any exception thrown while in this method will be sent back to
   * the client in string form as a standard RPC failure.
   * 接收一个prc信息,在这个方法中任何异常都要发送回客户端中,以流的方式返回给客户端
   *
   * This method will not be called in parallel for a single TransportClient (i.e., channel).
   * 该方法对于同一个客户端,不能并行被调用,即对于同一个客户端来说,请求服务器是一个请求接着一个请求进行的
   *
   * @param client A channel client which enables the handler to make requests back to the sender
   *               of this RPC. This will always be the exact same object for a particular channel.客户端是哪个流
   * @param message The serialized bytes of the RPC.客户端请求的序列化的信息
   * @param callback Callback which should be invoked exactly once upon success or failure of the
   *                 RPC.
   * 接收client客户端传来的命令,并且回调信息写入RpcResponseCallback中    
   */
  public abstract void receive(
      TransportClient client,//对应的客户端
      byte[] message,//客户端请求的序列化的信息
      RpcResponseCallback callback);//回调函数,当成功或者失败的时候,返回客户端信息

  /**
   * Returns the StreamManager which contains the state about which streams are currently being
   * fetched by a TransportClient.
   * 处理客户端请求的流管理器
   */
  public abstract StreamManager getStreamManager();

  /**
   * Invoked when the connection associated with the given client has been invalidated.
   * No further requests will come from this client.
   * 与客户端失去连接该如何处理
   */
  public void connectionTerminated(TransportClient client) { }
}
