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

package org.apache.spark.util.logging

import java.io.{File, FileOutputStream, InputStream}

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.util.{IntParam, Utils}

/**
 * Continuously appends the data from an input stream into the given file.
 * 将inputStream的信息写入到file文件中
 */
private[spark] class FileAppender(inputStream: InputStream, file: File, bufferSize: Int = 8192)
  extends Logging {
  @volatile private var outputStream: FileOutputStream = null //file文件的输出流
  @volatile private var markedForStop = false     // has the appender been asked to stopped
  @volatile private var stopped = false           // has the appender stopped

  // Thread that reads the input stream and writes to file 线程,不断的去读input信息,并且写入到file文件中
  private val writingThread = new Thread("File appending thread for " + file) {
    setDaemon(true)
    override def run() {
      Utils.logUncaughtExceptions {
        appendStreamToFile()
      }
    }
  }
  writingThread.start()

  /**
   * Wait for the appender to stop appending, either because input stream is closed
   * or because of any error in appending
   */
  def awaitTermination() {
    synchronized {
      if (!stopped) {
        wait()
      }
    }
  }

  /** Stop the appender */
  def stop() {
    markedForStop = true
  }

  /** Continuously read chunks from the input stream and append to the file 
   *  不断的去读input信息,并且写入到file文件中
   **/
  protected def appendStreamToFile() {
    try {
      logDebug("Started appending thread")
      openFile()
      val buf = new Array[Byte](bufferSize)
      var n = 0
      while (!markedForStop && n != -1) {
        n = inputStream.read(buf)
        if (n != -1) {
          appendToFile(buf, n)
        }
      }
    } catch {
      case e: Exception =>
        logError(s"Error writing stream to file $file", e)
    } finally {
      closeFile()
      synchronized {
        stopped = true
        notifyAll()
      }
    }
  }

  /** Append bytes to the file output stream 
   * 写入字节到文件输出流中  
   **/
  protected def appendToFile(bytes: Array[Byte], len: Int) {
    if (outputStream == null) {
      openFile()
    }
    outputStream.write(bytes, 0, len)
  }

  /** Open the file output stream 
   *  打开file文件 
   **/
  protected def openFile() {
    outputStream = new FileOutputStream(file, false)
    logDebug(s"Opened file $file")
  }

  /** Close the file output stream 
   * 关闭file文件  
   **/
  protected def closeFile() {
    outputStream.flush()
    outputStream.close()
    logDebug(s"Closed file $file")
  }
}

/**
 * Companion object to [[org.apache.spark.util.logging.FileAppender]] which has helper
 * functions to choose the correct type of FileAppender based on SparkConf configuration.
 */
private[spark] object FileAppender extends Logging {

  /** Create the right appender based on Spark configuration */
  def apply(inputStream: InputStream, file: File, conf: SparkConf): FileAppender = {

    import RollingFileAppender._

    val rollingStrategy = conf.get(STRATEGY_PROPERTY, STRATEGY_DEFAULT) //切换策略
    val rollingSizeBytes = conf.get(SIZE_PROPERTY, STRATEGY_DEFAULT) //基于字节数量的字节数
    val rollingInterval = conf.get(INTERVAL_PROPERTY, INTERVAL_DEFAULT) //基于time的时间间隔

    //创建基于时间的切换策略
    def createTimeBasedAppender(): FileAppender = {
      
      //返回值元祖,第一个是切换间隔,第二个是字符串形式表示的切换间隔,用于生成文件后缀
      val validatedParams: Option[(Long, String)] = rollingInterval match {
        case "daily" =>
          logInfo(s"Rolling executor logs enabled for $file with daily rolling")
          Some(24 * 60 * 60 * 1000L, "--yyyy-MM-dd")
        case "hourly" =>
          logInfo(s"Rolling executor logs enabled for $file with hourly rolling")
          Some(60 * 60 * 1000L, "--yyyy-MM-dd--HH")
        case "minutely" =>
          logInfo(s"Rolling executor logs enabled for $file with rolling every minute")
          Some(60 * 1000L, "--yyyy-MM-dd--HH-mm")
        case IntParam(seconds) =>
          logInfo(s"Rolling executor logs enabled for $file with rolling $seconds seconds")
          Some(seconds * 1000L, "--yyyy-MM-dd--HH-mm-ss")
        case _ =>
          logWarning(s"Illegal interval for rolling executor logs [$rollingInterval], " +
              s"rolling logs not enabled")
          None
      }
      validatedParams.map {
        case (interval, pattern) =>
          new RollingFileAppender(
            inputStream, file, new TimeBasedRollingPolicy(interval, pattern), conf)
      }.getOrElse {
        new FileAppender(inputStream, file)
      }
    }

    //创建基于文件大小的切换策略
    def createSizeBasedAppender(): FileAppender = {
      rollingSizeBytes match {
        case IntParam(bytes) =>
          logInfo(s"Rolling executor logs enabled for $file with rolling every $bytes bytes")
          new RollingFileAppender(inputStream, file, new SizeBasedRollingPolicy(bytes), conf)
        case _ =>
          logWarning(
            s"Illegal size [$rollingSizeBytes] for rolling executor logs, rolling logs not enabled")
          new FileAppender(inputStream, file)
      }
    }

    //根据文件策略,生成文件输出流
    rollingStrategy match {
      case "" =>
        new FileAppender(inputStream, file)
      case "time" =>
        createTimeBasedAppender()
      case "size" =>
        createSizeBasedAppender()
      case _ =>
        logWarning(
          s"Illegal strategy [$rollingStrategy] for rolling executor logs, " +
            s"rolling logs not enabled")
        new FileAppender(inputStream, file)
    }
  }
}


