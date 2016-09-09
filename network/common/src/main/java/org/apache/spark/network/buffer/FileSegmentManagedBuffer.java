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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.google.common.base.Objects;
import com.google.common.io.ByteStreams;
import io.netty.channel.DefaultFileRegion;

import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.network.util.TransportConf;

/**
 * A {@link ManagedBuffer} backed by a segment in a file.
 * 就是读取一个文件,从指定位置开始读取,读取最多多少个字节
 */
public final class FileSegmentManagedBuffer extends ManagedBuffer {
  private final TransportConf conf;
  private final File file;//读取一个文件
  private final long offset;//从什么地方开始读取
  private final long length;//最多读取多少个字节

  public FileSegmentManagedBuffer(TransportConf conf, File file, long offset, long length) {
    this.conf = conf;
    this.file = file;
    this.offset = offset;
    this.length = length;
  }

  @Override
  public long size() {
    return length;
  }

  //用channel流读取该文件
  @Override
  public ByteBuffer nioByteBuffer() throws IOException {
    FileChannel channel = null;
    try {
      channel = new RandomAccessFile(file, "r").getChannel();//创建channel流
      // Just copy the buffer if it's sufficiently small, as memory mapping has a high overhead.
        //仅仅copy到内存中,因为要读取的内容很小
      if (length < conf.memoryMapBytes()) {//需要的大小 比内存要求的小时候
        ByteBuffer buf = ByteBuffer.allocate((int) length);//创建内存buffer对象
        channel.position(offset);//定位到开始位置
        while (buf.remaining() != 0) {//读取数据到buffer中,填满buffer为止,因为buffer的大小就是length
          if (channel.read(buf) == -1) {
            throw new IOException(String.format("Reached EOF before filling buffer\n" +
              "offset=%s\nfile=%s\nbuf.remaining=%s",
              offset, file.getAbsoluteFile(), buf.remaining()));
          }
        }
        buf.flip();//将buffer的limit的值设为position的当前值,position设置到0的位置
        return buf;
      } else {
        return channel.map(FileChannel.MapMode.READ_ONLY, offset, length);//添加内存映射
      }
    } catch (IOException e) {
      try {
        if (channel != null) {
          long size = channel.size();
          throw new IOException("Error in reading " + this + " (actual file length " + size + ")",
            e);
        }
      } catch (IOException ignored) {
        // ignore
      }
      throw new IOException("Error in opening " + this, e);
    } finally {
      JavaUtils.closeQuietly(channel);
    }
  }

  //对文件创建流,并且读取到offset位置,返回流文件,并且限制了最多读取到什么位置就不能读取了
  //该流是正常的IO流,不是channel流
  @Override
  public InputStream createInputStream() throws IOException {
    FileInputStream is = null;
    try {
      is = new FileInputStream(file);
      ByteStreams.skipFully(is, offset);
      return new LimitedInputStream(is, length);
    } catch (IOException e) {
      try {
        if (is != null) {
          long size = file.length();
          throw new IOException("Error in reading " + this + " (actual file length " + size + ")",
              e);
        }
      } catch (IOException ignored) {
        // ignore
      } finally {
        JavaUtils.closeQuietly(is);
      }
      throw new IOException("Error in opening " + this, e);
    } catch (RuntimeException e) {
      JavaUtils.closeQuietly(is);
      throw e;
    }
  }

  @Override
  public ManagedBuffer retain() {
    return this;
  }

  @Override
  public ManagedBuffer release() {
    return this;
  }

  //判断是否是懒加载,然后创建对应的传输对象
  //使用netty方式去读取该文件
  @Override
  public Object convertToNetty() throws IOException {
    if (conf.lazyFileDescriptor()) {
      return new LazyFileRegion(file, offset, length);
    } else {
      FileChannel fileChannel = new FileInputStream(file).getChannel();
      return new DefaultFileRegion(fileChannel, offset, length);
    }
  }

  public File getFile() { return file; }

  public long getOffset() { return offset; }

  public long getLength() { return length; }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("file", file)
      .add("offset", offset)
      .add("length", length)
      .toString();
  }
}
