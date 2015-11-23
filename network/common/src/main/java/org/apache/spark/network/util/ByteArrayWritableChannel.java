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

package org.apache.spark.network.util;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * A writable channel that stores the written data in a byte array in memory.
 * 提供内存data数组存储数据
 */
public class ByteArrayWritableChannel implements WritableByteChannel {

  private final byte[] data;//存储数据的内存字节数组
  private int offset;//下一次写入内存数组的位置

  public ByteArrayWritableChannel(int size) {
    this.data = new byte[size];
  }

  public byte[] getData() {
    return data;
  }

  public int length() {
    return offset;
  }

  /** Resets the channel so that writing to it will overwrite the existing buffer. */
  public void reset() {
    offset = 0;
  }

  /**
   * Reads from the given buffer into the internal byte array.
   */
  @Override
  public int write(ByteBuffer src) {
	  //将src中的内容写入到data内存数组中
    int toTransfer = Math.min(src.remaining(), data.length - offset);//最多写满data字节数组为止
    src.get(data, offset, toTransfer);//读取src的数据,读取toTransfer个数据,将读取的数据写入到data数组中
    offset += toTransfer;//因为写入了toTransfer数据,则offset移动toTransfer个位置
    return toTransfer;
  }

  @Override
  public void close() {

  }

  @Override
  public boolean isOpen() {
    return true;
  }

}
