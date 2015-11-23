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

package org.apache.spark.network.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * This interface provides an immutable view for data in the form of bytes. The implementation
 * should specify how the data is provided:
 *  提供不可变的字节数据的视图功能,有以下三种方式缓存该不变的字节数组
 * - {@link FileSegmentManagedBuffer}: data backed by part of a file 在文件中缓存字节数组
 * - {@link NioManagedBuffer}: data backed by a NIO ByteBuffer 使用Nio技术缓存字节数组
 * - {@link NettyManagedBuffer}: data backed by a Netty ByteBuf 使用Netty技术缓存字节数组
 *
 * The concrete buffer implementation might be managed outside the JVM garbage collector.
 * 一个具体的buffer实现可能托管在JVM垃圾回收之外
 * For example, in the case of {@link NettyManagedBuffer}, the buffers are reference counted.
 * In that case, if the buffer is going to be passed around to a different thread, retain/release
 * should be called.
 */
public abstract class ManagedBuffer {

  /** Number of bytes of the data.缓存的数据大小 */
  public abstract long size();

  /**
   * Exposes this buffer's data as an NIO ByteBuffer. Changing the position and limit of the
   * returned ByteBuffer should not affect the content of this buffer.
   */
  // TODO: Deprecate this, usage may require expensive memory mapping or allocation.
  public abstract ByteBuffer nioByteBuffer() throws IOException;

  /**
   * Exposes this buffer's data as an InputStream. The underlying implementation does not
   * necessarily check for the length of bytes read, so the caller is responsible for making sure
   * it does not go over the limit.
   * 以流的形式获取数据
   */
  public abstract InputStream createInputStream() throws IOException;

  /**
   * Increment the reference count by one if applicable.
   * 增加映射,说明持有该缓存的多了一个
   */
  public abstract ManagedBuffer retain();

  /**
   * If applicable, decrement the reference count by one and deallocates the buffer if the
   * reference count reaches zero.
   * 释放映射,说明持有该缓存的少了一个
   */
  public abstract ManagedBuffer release();

  /**
   * Convert the buffer into an Netty object, used to write the data out.
   * 将数据转换成Netty
   */
  public abstract Object convertToNetty() throws IOException;
}
