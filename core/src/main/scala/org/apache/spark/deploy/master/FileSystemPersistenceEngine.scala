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

import java.io._

import scala.reflect.ClassTag

import org.apache.spark.Logging
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer}
import org.apache.spark.util.Utils


/**
 * Stores data in a single on-disk directory with one file per application and worker.
 * Files are deleted when applications and workers are removed.
 * 该对象用于在磁盘上存储数据
 * 每一个应用或者worker使用一个文件,当应用或者worker被删除的时候,文件也跟着被删除
 * @param dir Directory to store files. Created if non-existent (but not recursively).存储数据的目录
 * @param serializer Used to serialize our objects.序列化对象工具
 */
private[master] class FileSystemPersistenceEngine(
    val dir: String,
    val serializer: Serializer)
  extends PersistenceEngine with Logging {

  //创建该目录
  new File(dir).mkdir()

  //创建一个文件,名字就是dir/name,讲obj对象序列化后存储在该文件里
  override def persist(name: String, obj: Object): Unit = {
    serializeIntoFile(new File(dir + File.separator + name), obj)
  }

  //删除该name对应的文件
  override def unpersist(name: String): Unit = {
    new File(dir + File.separator + name).delete()
  }

  //读取以参数开头的文件集合,将每一个对象进行反序列化成对象集合
  override def read[T: ClassTag](prefix: String): Seq[T] = {
    val files = new File(dir).listFiles().filter(_.getName.startsWith(prefix))
    files.map(deserializeFromFile[T])
  }

  //将value序列化到文件中
  private def serializeIntoFile(file: File, value: AnyRef) {
    val created = file.createNewFile()//创建一个新文件
    if (!created) { throw new IllegalStateException("Could not create file: " + file) }
    //将value的内容序列化到文件里
    val fileOut = new FileOutputStream(file)
    var out: SerializationStream = null
    Utils.tryWithSafeFinally {
      out = serializer.newInstance().serializeStream(fileOut)
      out.writeObject(value)
    } {
      fileOut.close()
      if (out != null) {
        out.close()
      }
    }
  }

  //将文件的内容反序列化成对象返回
  private def deserializeFromFile[T](file: File)(implicit m: ClassTag[T]): T = {
    val fileIn = new FileInputStream(file)
    var in: DeserializationStream = null
    try {
      in = serializer.newInstance().deserializeStream(fileIn)
      in.readObject[T]()
    } finally {
      fileIn.close()
      if (in != null) {
        in.close()
      }
    }
  }

}
