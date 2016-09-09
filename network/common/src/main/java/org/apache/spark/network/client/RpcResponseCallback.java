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

/**
 * Callback for the result of a single RPC. This will be invoked once with either success or
 * failure.
 * 回调函数,当执行成功或者失败后,会调用该函数,该函数是TransportClient产生的一个函数,发送到服务端,服务端处理完后,执行该回调函数
 */
public interface RpcResponseCallback {
  /** Successful serialized result from server. RPC请求成功返回,并且有返回值*/
  void onSuccess(byte[] response);

  /** Exception either propagated from server or raised on client side.RPC请求失败返回,并且带有异常对象 */
  void onFailure(Throwable e);
}
