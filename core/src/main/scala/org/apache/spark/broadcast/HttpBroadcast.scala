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

package org.apache.spark.broadcast

import java.io.{File, FileOutputStream, ObjectInputStream, ObjectOutputStream, OutputStream}
import java.io.{BufferedInputStream, BufferedOutputStream}
import java.net.{URL, URLConnection, URI}
import java.util.concurrent.TimeUnit

import scala.reflect.ClassTag

import org.apache.spark.{HttpServer, Logging, SecurityManager, SparkConf, SparkEnv}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.storage.{BroadcastBlockId, StorageLevel}
import org.apache.spark.util.{MetadataCleaner, MetadataCleanerType, TimeStampedHashSet, Utils}

/**
 * A [[org.apache.spark.broadcast.Broadcast]] implementation that uses HTTP server
 * as a broadcast mechanism. The first time a HTTP broadcast variable (sent as part of a
 * task) is deserialized in the executor, the broadcasted data is fetched from the driver
 * (through a HTTP server running at the driver) and stored in the BlockManager of the
 * executor to speed up future accesses.
 * 
 * 通过HTTP协议进行传输广播对象的序列化与反序列化流
 */
private[spark] class HttpBroadcast[T: ClassTag](
    @transient var value_ : T, isLocal: Boolean, id: Long)
  extends Broadcast[T](id) with Logging with Serializable {

  override protected def getValue() = value_

  private val blockId = BroadcastBlockId(id) //该广播对象的唯一标示

  /*
   * Broadcasted data is also stored in the BlockManager of the driver. The BlockManagerMaster
   * does not need to be told about this block as not only need to know about this data block.
   */
  HttpBroadcast.synchronized {
    SparkEnv.get.blockManager.putSingle(
      blockId, value_, StorageLevel.MEMORY_AND_DISK, tellMaster = false)
  }

  //将对象写入到文件系统中
  if (!isLocal) {
    HttpBroadcast.write(id, value_)
  }

  /**
   * Remove all persisted state associated with this HTTP broadcast on the executors.
   */
  override protected def doUnpersist(blocking: Boolean) {
    HttpBroadcast.unpersist(id, removeFromDriver = false, blocking)
  }

  /**
   * Remove all persisted state associated with this HTTP broadcast on the executors and driver.
   */
  override protected def doDestroy(blocking: Boolean) {
    HttpBroadcast.unpersist(id, removeFromDriver = true, blocking)
  }

  /** Used by the JVM when serializing this object. */
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    assertValid()
    out.defaultWriteObject()
  }

  /** Used by the JVM when deserializing this object. */
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    HttpBroadcast.synchronized {
      SparkEnv.get.blockManager.getSingle(blockId) match {
        case Some(x) => value_ = x.asInstanceOf[T]
        case None => {
          logInfo("Started reading broadcast variable " + id)
          val start = System.nanoTime
          value_ = HttpBroadcast.read[T](id)
          /*
           * We cache broadcast data in the BlockManager so that subsequent tasks using it
           * do not need to re-fetch. This data is only used locally and no other node
           * needs to fetch this block, so we don't notify the master.
           */
          SparkEnv.get.blockManager.putSingle(
            blockId, value_, StorageLevel.MEMORY_AND_DISK, tellMaster = false)
          val time = (System.nanoTime - start) / 1e9
          logInfo("Reading broadcast variable " + id + " took " + time + " s")
        }
      }
    }
  }
}

//通过HTTP协议进行传输广播对象的序列化与反序列化流
private[broadcast] object HttpBroadcast extends Logging {
  private var initialized = false //是否初始化了
  private var broadcastDir: File = null //创建broadcast文件夹,并且在该目录下常见一个服务
  private var compress: Boolean = false //是否压缩数据
  private var bufferSize: Int = 65536 //缓存大小
  private var serverUri: String = null //开启的服务访问url
  private var server: HttpServer = null //开启的服务
  private var securityManager: SecurityManager = null

  // TODO: This shouldn't be a global variable so that multiple SparkContexts can coexist
  //存储文件集合,并且提供按照给定时间戳删除文件功能
  private val files = new TimeStampedHashSet[File]
  private val httpReadTimeout = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES).toInt //反序列化时,通过网络请求的延迟时间
  private var compressionCodec: CompressionCodec = null //压缩方式
  private var cleaner: MetadataCleaner = null

  def initialize(isDriver: Boolean, conf: SparkConf, securityMgr: SecurityManager) {
    synchronized {
      if (!initialized) {//进行初始化
        bufferSize = conf.getInt("spark.buffer.size", 65536)
        compress = conf.getBoolean("spark.broadcast.compress", true)
        securityManager = securityMgr
        if (isDriver) {
          createServer(conf)
          conf.set("spark.httpBroadcast.uri", serverUri)
        }
        //获取HttpServer的uri
        serverUri = conf.get("spark.httpBroadcast.uri")
        cleaner = new MetadataCleaner(MetadataCleanerType.HTTP_BROADCAST, cleanup, conf)
        compressionCodec = CompressionCodec.createCodec(conf)
        initialized = true
      }
    }
  }

  def stop() {
    synchronized {
      if (server != null) {
        server.stop()
        server = null
      }
      if (cleaner != null) {
        cleaner.cancel()
        cleaner = null
      }
      compressionCodec = null
      initialized = false
    }
  }

  private def createServer(conf: SparkConf) {
    //创建broadcast文件夹,并且在该目录下常见一个服务
    broadcastDir = Utils.createTempDir(Utils.getLocalDir(conf), "broadcast")
    val broadcastPort = conf.getInt("spark.broadcast.port", 0)
    server =
      new HttpServer(conf, broadcastDir, securityManager, broadcastPort, "HTTP broadcast server")
    server.start()
    serverUri = server.uri
    logInfo("Broadcast server started at " + serverUri)
  }


  def getFile(id: Long): File = new File(broadcastDir, BroadcastBlockId(id).name)

  //将服务器上的文件读取成流,发送给客户端
  private def write(id: Long, value: Any) {
    val file = getFile(id)
    val fileOutputStream = new FileOutputStream(file) //原始文件的输出流
    Utils.tryWithSafeFinally {
      //对原始文件输出流进行包装
      val out: OutputStream = {
        if (compress) {
          compressionCodec.compressedOutputStream(fileOutputStream)
        } else {
          new BufferedOutputStream(fileOutputStream, bufferSize)
        }
      }

      //序列化对象
      val ser = SparkEnv.get.serializer.newInstance()
      val serOut = ser.serializeStream(out)
      Utils.tryWithSafeFinally {
        serOut.writeObject(value) //将value对象写入到序列化的流中
      } {
        serOut.close()
      }
      files += file
    } {
      fileOutputStream.close()
    }
  }

  //从server上根据广播ID反序列化对象
  private def read[T: ClassTag](id: Long): T = {
    logDebug("broadcast read server: " + serverUri + " id: broadcast-" + id)
    val url = serverUri + "/" + BroadcastBlockId(id).name //获取对象的url

    var uc: URLConnection = null //建立连接请求
    if (securityManager.isAuthenticationEnabled()) {
      logDebug("broadcast security enabled")
      val newuri = Utils.constructURIForAuthentication(new URI(url), securityManager)
      uc = newuri.toURL.openConnection()
      uc.setConnectTimeout(httpReadTimeout)
      uc.setAllowUserInteraction(false)
    } else {
      logDebug("broadcast not using security")
      uc = new URL(url).openConnection()
      uc.setConnectTimeout(httpReadTimeout)
    }
    Utils.setupSecureURLConnection(uc, securityManager)

    //从输入流中反序列化对象
    val in = {//对流进行包装
      uc.setReadTimeout(httpReadTimeout)
      val inputStream = uc.getInputStream
      if (compress) {
        compressionCodec.compressedInputStream(inputStream)
      } else {
        new BufferedInputStream(inputStream, bufferSize)
      }
    }

    //反序列化类包装器
    val ser = SparkEnv.get.serializer.newInstance()
    val serIn = ser.deserializeStream(in)
    Utils.tryWithSafeFinally {
      serIn.readObject[T]() //真正的反序列化成对象
    } {
      serIn.close()
    }
  }

  /**
   * Remove all persisted blocks associated with this HTTP broadcast on the executors.
   * If removeFromDriver is true, also remove these persisted blocks on the driver
   * and delete the associated broadcast file.
   */
  def unpersist(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit = synchronized {
    SparkEnv.get.blockManager.master.removeBroadcast(id, removeFromDriver, blocking)
    if (removeFromDriver) {//删除该文件
      val file = getFile(id)
      files.remove(file)
      deleteBroadcastFile(file)
    }
  }

  /**
   * Periodically clean up old broadcasts by removing the associated map entries and
   * deleting the associated files.
   * 删除 小于 给定参数时间戳的文件
   */
  private def cleanup(cleanupTime: Long) {
    val iterator = files.internalMap.entrySet().iterator()
    while(iterator.hasNext) {
      val entry = iterator.next()
      val (file, time) = (entry.getKey, entry.getValue)
      if (time < cleanupTime) {
        iterator.remove() //删除内存file映射
        deleteBroadcastFile(file)//真正删除文件
      }
    }
  }

  //删除一个广播文件
  private def deleteBroadcastFile(file: File) {
    try {
      if (file.exists) {
        if (file.delete()) {
          logInfo("Deleted broadcast file: %s".format(file))
        } else {
          logWarning("Could not delete broadcast file: %s".format(file))
        }
      }
    } catch {
      case e: Exception =>
        logError("Exception while deleting broadcast file: %s".format(file), e)
    }
  }
}
