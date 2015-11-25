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

package org.apache.spark.deploy.master

import java.nio.ByteBuffer

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.deploy.SparkCuratorUtil
import org.apache.spark.serializer.Serializer

//使用zookeeper存储序列化后的内容
private[master] class ZooKeeperPersistenceEngine(conf: SparkConf, val serializer: Serializer)
  extends PersistenceEngine
  with Logging {

  //获取zookeeper路径
  private val WORKING_DIR = conf.get("spark.deploy.zookeeper.dir", "/spark") + "/master_status"
  
  //创建zookeeper
  private val zk: CuratorFramework = SparkCuratorUtil.newClient(conf)

  //在zookeeper上创建路径
  SparkCuratorUtil.mkdir(zk, WORKING_DIR)

  //在zookeeper上name路径下存储obj内容
  override def persist(name: String, obj: Object): Unit = {
    serializeIntoFile(WORKING_DIR + "/" + name, obj)
  }

  //删除zookeeper上指定name的节点
  override def unpersist(name: String): Unit = {
    zk.delete().forPath(WORKING_DIR + "/" + name)
  }

  //读取全部符合prefix前缀的内容.并且反序列化成对象集合
  override def read[T: ClassTag](prefix: String): Seq[T] = {
    val file = zk.getChildren.forPath(WORKING_DIR).filter(_.startsWith(prefix))
    file.map(deserializeFromFile[T]).flatten
  }

  override def close() {
    zk.close()
  }

  //将value对象序列化到path路径节点存储
  private def serializeIntoFile(path: String, value: AnyRef) {
    val serialized = serializer.newInstance().serialize(value)
    val bytes = new Array[Byte](serialized.remaining())
    serialized.get(bytes)
    zk.create().withMode(CreateMode.PERSISTENT).forPath(path, bytes)
  }

  //将path路径节点存储的数据反序列化成对象
  private def deserializeFromFile[T](filename: String)(implicit m: ClassTag[T]): Option[T] = {
    val fileData = zk.getData().forPath(WORKING_DIR + "/" + filename)
    try {
      Some(serializer.newInstance().deserialize[T](ByteBuffer.wrap(fileData)))
    } catch {
      case e: Exception => {
        logWarning("Exception while reading persisted file, deleting", e)
        zk.delete().forPath(WORKING_DIR + "/" + filename)
        None
      }
    }
  }
}
