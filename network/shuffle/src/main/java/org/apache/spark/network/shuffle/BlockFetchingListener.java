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

package org.apache.spark.network.shuffle;

import java.util.EventListener;

import org.apache.spark.network.buffer.ManagedBuffer;

//抓取监听,在ShuffleClient去抓取一个ip:port的数据的时候,使用该监听,及时通知ShuffleClient抓取情况
public interface BlockFetchingListener extends EventListener {
  /**
   * Called once per successfully fetched block. After this call returns, data will be released
   * automatically. If the data will be passed to another thread, the receiver should retain()
   * and release() the buffer on their own, or copy the data to a new buffer.
   * 每一个成功抓取数据块,就会调用该函数
   * 调用函数后,数据将被自动释放,如果该数据还需要传递给其他线程,则需要接受者使用retain和release方法,将在自己的环境下做copy一个新的缓冲进行处理
   * 
   * 参数data是该blockId数据块对应的字节内容
   */
  void onBlockFetchSuccess(String blockId, ManagedBuffer data);

  /**
   * Called at least once per block upon failures.
   * 抓取失败时调用
   */
  void onBlockFetchFailure(String blockId, Throwable exception);
}
