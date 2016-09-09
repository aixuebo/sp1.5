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

import java.io.FileInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import com.google.common.base.Objects;
import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;

import org.apache.spark.network.util.JavaUtils;

/**
 * A FileRegion implementation that only creates the file descriptor when the region is being
 * transferred. This cannot be used with Epoll because there is no native support for it.
 * 
 * This is mostly copied from DefaultFileRegion implementation in Netty. In the future, we
 * should push this into Netty so the native Epoll transport can support this feature.
 * 读取一个file文件,将文件的内容发送到流中,该类会设置要读取什么文件,以及从该文件的什么位置开始读取,读取多少个字节
 */
public final class LazyFileRegion extends AbstractReferenceCounted implements FileRegion {

  private final File file;
  private final long position;
  private final long count;

  private FileChannel channel;

  private long numBytesTransferred = 0L;//表示已经真正发送了多少个字节

  /**
   * @param file file to transfer.
   * @param position start position for the transfer.
   * @param count number of bytes to transfer starting from position.
   */
  public LazyFileRegion(File file, long position, long count) {
    this.file = file;
    this.position = position;
    this.count = count;
  }

  @Override
  protected void deallocate() {
    JavaUtils.closeQuietly(channel);
  }

  @Override
  public long position() {
    return position;
  }

  //已经发送了多少个字节
  @Override
  public long transfered() {
    return numBytesTransferred;
  }

  @Override
  public long count() {
    return count;
  }

    /**
     * 将file文件的内容发送到target流中
     *  该position是相对位置,在该文件的position和position+count之间。
     比如要读取该文件,从position位置是100开始读取,读取count=1000,说明要读取该文件的100-1100之间的数据。
     如果该参数position是300,则说明本次要从开始位置100+300位置开始读取,因此就是从400开始读取,读取最多就不是1000个字节了，而是1000-400个字节
     */
  @Override
  public long transferTo(WritableByteChannel target, long position) throws IOException {
    if (channel == null) {
      channel = new FileInputStream(file).getChannel();
    }

    long count = this.count - position;
    if (count < 0 || position < 0) {
      throw new IllegalArgumentException(
          "position out of range: " + position + " (expected: 0 - " + (count - 1) + ')');
    }

    if (count == 0) {
      return 0L;
    }

      //将channel的内容发送到target中,从channel的position+position位置开始读取,最多读取count字节
    long written = channel.transferTo(this.position + position, count, target);//返回真正发送了多少个字节
    if (written > 0) {
      numBytesTransferred += written;
    }
    return written;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("file", file)
        .add("position", position)
        .add("count", count)
        .toString();
  }
}
