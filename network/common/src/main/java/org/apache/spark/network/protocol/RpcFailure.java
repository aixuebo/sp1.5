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

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

/** Response to {@link RpcRequest} for a failed RPC.
 * RPC失败
 * 作为RPC的返回值,仅仅返回请求ID和失败的回复信息
 **/
public final class RpcFailure implements ResponseMessage {
  public final long requestId;//针对某个请求产生的RPC失败
  public final String errorString;//失败原因

  public RpcFailure(long requestId, String errorString) {
    this.requestId = requestId;
    this.errorString = errorString;
  }

  @Override
  public Type type() { return Type.RpcFailure; }

  //8表示long类型的requestId
  @Override
  public int encodedLength() {
    return 8 + Encoders.Strings.encodedLength(errorString);
  }

  //编码--序列化
  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(requestId);
    Encoders.Strings.encode(buf, errorString);
  }

  //解码--反序列化
  public static RpcFailure decode(ByteBuf buf) {
    long requestId = buf.readLong();
    String errorString = Encoders.Strings.decode(buf);
    return new RpcFailure(requestId, errorString);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(requestId, errorString);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof RpcFailure) {
      RpcFailure o = (RpcFailure) other;
      return requestId == o.requestId && errorString.equals(o.errorString);
    }
    return false;
  }

  @Override
   public String toString() {
    return Objects.toStringHelper(this)
      .add("requestId", requestId)
      .add("errorString", errorString)
      .toString();
  }
}
