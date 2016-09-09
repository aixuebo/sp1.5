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

import io.netty.channel.Channel;

/**
 * A bootstrap which is executed on a TransportClient before it is returned to the user.
 * 相当于拦截器,在一个TransportClient返回给用户前,执行一个程序引导操作
 * This enables an initial exchange of information (e.g., SASL authentication tokens) on a once-per-
 * connection basis.
 * 
 * Since connections (and TransportClients) are reused as much as possible, it is generally
 * reasonable to perform an expensive bootstrapping operation, as they often share a lifespan with
 * the JVM itself.
 */
public interface TransportClientBootstrap {
  /** Performs the bootstrapping operation, throwing an exception on failure.
   *  在返回给用户一个TransportClient使用之前,对TransportClient和交流的渠道Channel进行拦截处理程序引导
   *  参数client 表示刚刚产生的连接的客户端
   *  参数channel 表示与服务器连接的通道
   **/
  void doBootstrap(TransportClient client, Channel channel) throws RuntimeException;
}
