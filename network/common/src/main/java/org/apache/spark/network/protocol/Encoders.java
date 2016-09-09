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


import com.google.common.base.Charsets;
import io.netty.buffer.ByteBuf;

/** Provides a canonical set of Encoders for simple types. 
 * 提供一个权威的编码与解码器
 **/
public class Encoders {

  /** Strings are encoded with their length followed by UTF-8 bytes. 
   * 对String进行编码
   * 格式
   * 4个字节的int,表示String转换成UTF-8编码后字节大小
   * UTF-8编码的字节数组
   **/
  public static class Strings {
    public static int encodedLength(String s) {
      return 4 + s.getBytes(Charsets.UTF_8).length;
    }

    //编码--序列化
    public static void encode(ByteBuf buf, String s) {
      byte[] bytes = s.getBytes(Charsets.UTF_8);//将字符串转换成字节数组
      buf.writeInt(bytes.length);//发送字节数组长度
      buf.writeBytes(bytes);//发送字节数组
    }

    //解码--反序列化
    public static String decode(ByteBuf buf) {
      int length = buf.readInt();
      byte[] bytes = new byte[length];
      buf.readBytes(bytes);
      return new String(bytes, Charsets.UTF_8);
    }
  }

  /** Byte arrays are encoded with their length followed by bytes.
   * 对字节数组编码
   * 格式
   * 4个字节的int,表示字节数组大小
   * 字节数组内容
   **/
  public static class ByteArrays {
    public static int encodedLength(byte[] arr) {
      return 4 + arr.length;
    }

    //编码--序列化
    public static void encode(ByteBuf buf, byte[] arr) {
      buf.writeInt(arr.length);//发送字节数组长度
      buf.writeBytes(arr);//发送字节数组
    }

    //解码--反序列化
    public static byte[] decode(ByteBuf buf) {
      int length = buf.readInt();
      byte[] bytes = new byte[length];
      buf.readBytes(bytes);
      return bytes;
    }
  }

  /** String arrays are encoded with the number of strings followed by per-String encoding. 
   * 对String数组编码
   * 格式:
   * 4个字节的int,表示数组大小
   * 每一个String,又用UTF-8的字节数组存储字节
   **/
  public static class StringArrays {
    public static int encodedLength(String[] strings) {
      int totalLength = 4;
      for (String s : strings) {
        totalLength += Strings.encodedLength(s);
      }
      return totalLength;
    }

    //编码--序列化
    public static void encode(ByteBuf buf, String[] strings) {
      buf.writeInt(strings.length);//发送有多少个字节数组
      for (String s : strings) {
        Strings.encode(buf, s);//写入每一个字节数组
      }
    }

    //解码--反序列化
    public static String[] decode(ByteBuf buf) {
      int numStrings = buf.readInt();
      String[] strings = new String[numStrings];
      for (int i = 0; i < strings.length; i ++) {
        strings[i] = Strings.decode(buf);
      }
      return strings;
    }
  }
}
