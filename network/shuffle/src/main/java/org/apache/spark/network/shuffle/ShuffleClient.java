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

import java.io.Closeable;

/** Provides an interface for reading shuffle files, either from an Executor or external service. 
 * 提供一个接口去读取shuffle的文件,该文件可以是执行进程里面读取,也可以是额外的服务读取 
 **/
public abstract class ShuffleClient implements Closeable {

  /**
   * Initializes the ShuffleClient, specifying this Executor's appId.
   * 通过指定执行的appId,初始化Shuffle客户端
   * Must be called before any other method on the ShuffleClient.
   * 任何在shuffle客户端上的操作,必须先调用该初始化方法
   */
  public void init(String appId) { }

  /**
   * Fetch a sequence of blocks from a remote node asynchronously,
   * 从远程节点异步的抓取一组序列化的数据块集合
   * Note that this API takes a sequence so the implementation can batch requests, and does not
   * return a future so the underlying implementation can invoke onBlockFetchSuccess as soon as
   * the data of a block is fetched, rather than waiting for all blocks to be fetched.
   * 注意:该api抓取一个序列化的数据块,因此实现必须能够批量抓取请求
   */
  public abstract void fetchBlocks(
      String host,//去哪个host的哪个Port上抓取数据
      int port,
      String execId,//抓取哪个进程的数据
      String[] blockIds,//抓取哪些数据块数据
      BlockFetchingListener listener);//抓取监听,当抓取成功或者失败的时候,进行相应的处理
}
