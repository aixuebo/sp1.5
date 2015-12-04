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

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.Utils

/**
 * :: DeveloperApi ::
 * This class represent an unique identifier for a BlockManager.
 *
 * The first 2 constructors of this class is made private to ensure that BlockManagerId objects
 * can be created only using the apply method in the companion object. This allows de-duplication
 * of ID objects. Also, constructor parameters are private to ensure that parameters cannot be
 * modified from outside this class.、
 * 
 * 一个执行者和所在的host-port组成一个数据块管理器,该管理器管理着自己执行环境的内存信息
 */
@DeveloperApi
class BlockManagerId private (
    private var executorId_ : String,//该值为<driver>或者driver时,表示是driver
    private var host_ : String,
    private var port_ : Int)
  extends Externalizable {

  private def this() = this(null, null, 0)  // For deserialization only

  def executorId: String = executorId_

  //校验参数一定是host,不能有port
  if (null != host_){
    Utils.checkHost(host_, "Expected hostname")
    assert (port_ > 0)
  }

  //返回 host:port
  def hostPort: String = {
    // DEBUG code
    Utils.checkHost(host)
    assert (port > 0)
    host + ":" + port
  }

  def host: String = host_

  def port: Int = port_

  //是否是driver
  def isDriver: Boolean = {
    executorId == SparkContext.DRIVER_IDENTIFIER ||
      executorId == SparkContext.LEGACY_DRIVER_IDENTIFIER
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    out.writeUTF(executorId_)
    out.writeUTF(host_)
    out.writeInt(port_)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    executorId_ = in.readUTF()
    host_ = in.readUTF()
    port_ = in.readInt()
  }

  @throws(classOf[IOException])
  private def readResolve(): Object = BlockManagerId.getCachedBlockManagerId(this)

  override def toString: String = s"BlockManagerId($executorId, $host, $port)"

  override def hashCode: Int = (executorId.hashCode * 41 + host.hashCode) * 41 + port

  override def equals(that: Any): Boolean = that match {
    case id: BlockManagerId =>
      executorId == id.executorId && port == id.port && host == id.host
    case _ =>
      false
  }
}


private[spark] object BlockManagerId {

  /**
   * Returns a [[org.apache.spark.storage.BlockManagerId]] for the given configuration.
   *
   * @param execId ID of the executor.
   * @param host Host name of the block manager.
   * @param port Port of the block manager.
   * @return A new [[org.apache.spark.storage.BlockManagerId]].
   */
  def apply(execId: String, host: String, port: Int): BlockManagerId =
    getCachedBlockManagerId(new BlockManagerId(execId, host, port))

  def apply(in: ObjectInput): BlockManagerId = {
    val obj = new BlockManagerId()
    obj.readExternal(in)
    getCachedBlockManagerId(obj)
  }

  //静态方法,获取唯一的BlockManagerId对象
  val blockManagerIdCache = new ConcurrentHashMap[BlockManagerId, BlockManagerId]()

  //返回缓存过得BlockManagerId对象
  def getCachedBlockManagerId(id: BlockManagerId): BlockManagerId = {
    blockManagerIdCache.putIfAbsent(id, id)
    blockManagerIdCache.get(id)
  }
}
