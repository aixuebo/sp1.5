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

import java.util.Arrays;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

/** Response to {@link RpcRequest} for a successful RPC.
 * 作为RPC的返回值,仅仅返回请求ID和成功的回复信息
 **/
public final class RpcResponse implements ResponseMessage {
  public final long requestId;//针对请求requestId的Response
  public final byte[] response;//回复的内容信息

  public RpcResponse(long requestId, byte[] response) {
    this.requestId = requestId;
    this.response = response;
  }

  @Override
  public Type type() { return Type.RpcResponse; }

  //8表示long类型的requestId
  @Override
  public int encodedLength() { return 8 + Encoders.ByteArrays.encodedLength(response); }

  //编码--序列化
  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(requestId);//写入id
    Encoders.ByteArrays.encode(buf, response);//写入输出值
  }

  //解码--反序列化
  public static RpcResponse decode(ByteBuf buf) {
    long requestId = buf.readLong();
    byte[] response = Encoders.ByteArrays.decode(buf);
    return new RpcResponse(requestId, response);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(requestId, Arrays.hashCode(response));
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof RpcResponse) {
      RpcResponse o = (RpcResponse) other;
      return requestId == o.requestId && Arrays.equals(response, o.response);
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("requestId", requestId)
      .add("response", response)
      .toString();
  }
}
