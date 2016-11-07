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

package org.apache.spark.storage

import java.util.concurrent.ConcurrentHashMap

/**
 * 表示数据块的大小  或者 该数据块是失败了或者等待处理中
 * @param level
 * @param tellMaster
 */
private[storage] class BlockInfo(val level: StorageLevel, val tellMaster: Boolean) {
  // To save space, 'pending' and 'failed' are encoded as special sizes:
  @volatile var size: Long = BlockInfo.BLOCK_PENDING //最终数据块大小
  private def pending: Boolean = size == BlockInfo.BLOCK_PENDING //true表示是等待状态
  private def failed: Boolean = size == BlockInfo.BLOCK_FAILED //true表示是已经失败状态
  private def initThread: Thread = BlockInfo.blockInfoInitThreads.get(this)//持有该数据块的线程

  setInitThread()

  //将该数据块与当前线程绑定
  private def setInitThread() {
    /* Set current thread as init thread - waitForReady will not block this thread
     * (in case there is non trivial initialization which ends up calling waitForReady
     * as part of initialization itself) */
    BlockInfo.blockInfoInitThreads.put(this, Thread.currentThread())
  }

  /**
   * Wait for this BlockInfo to be marked as ready (i.e. block is finished writing).
   * 等候该数据块被标记成已完成,例如该数据块已经完成了写入
   * Return true if the block is available, false otherwise.
   * true表示该数据块可用,fasle表示该数据块不可用,该方法是阻塞方法
   */
  def waitForReady(): Boolean = {
    if (pending && initThread != Thread.currentThread()) { //如果等待状态,并且当前线程不符合要求,则进行等待
      synchronized {
        while (pending) {
          this.wait()
        }
      }
    }
    !failed
  }

  /** Mark this BlockInfo as ready (i.e. block is finished writing) 
   *  数据块被标记成已完成,例如该数据块已经完成了写入
   *  
   *  参数是该数据块有多少个字节
   **/
  def markReady(sizeInBytes: Long) {
    require(sizeInBytes >= 0, s"sizeInBytes was negative: $sizeInBytes")
    assert(pending) //校验状态必须是等待状态
    size = sizeInBytes //设置该数据块的字节数
    BlockInfo.blockInfoInitThreads.remove(this)
    synchronized {
      this.notifyAll()
    }
  }

  /** Mark this BlockInfo as ready but failed
    *
    * 标志该数据块已经失败
    **/
  def markFailure() {
    assert(pending)//断言此时状态一定是等待状态
    size = BlockInfo.BLOCK_FAILED//设置为失败状态
    BlockInfo.blockInfoInitThreads.remove(this) //数据块从等待的队列中移除
    synchronized {//通知waitForReady方法返回true
      this.notifyAll()
    }
  }
}

private object BlockInfo {
  /* initThread is logically a BlockInfo field, but we store it here because
   * it's only needed while this block is in the 'pending' state and we want
   * to minimize BlockInfo's memory footprint. 
   * 全局属性,因为是静态对象,获取操作该BlockInfo数据块的线程,该队列属于等待的数据块队列
   * 因为当前数据块仅仅处于等待状态,我们想要使用最小的内存去保存该数据块的足迹
   *
   * 一旦数据块完成了或者失败了,则从该队列中删除
   **/
  private val blockInfoInitThreads = new ConcurrentHashMap[BlockInfo, Thread]

  private val BLOCK_PENDING: Long = -1L //等待的状态
  private val BLOCK_FAILED: Long = -2L //失败的状态
}
