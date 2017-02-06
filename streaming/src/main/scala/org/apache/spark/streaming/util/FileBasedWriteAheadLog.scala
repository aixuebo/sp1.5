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
package org.apache.spark.streaming.util

import java.nio.ByteBuffer
import java.util.{Iterator => JIterator}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.util.{CompletionIterator, ThreadUtils}
import org.apache.spark.{Logging, SparkConf}

/**
 * This class manages write ahead log files.
 * - Writes records (bytebuffers) to periodically rotating log files.
 * - Recovers the log files and the reads the recovered records upon failures.
 * - Cleans up old log files.
 *
 * Uses [[org.apache.spark.streaming.util.FileBasedWriteAheadLogWriter]] to write
 * and [[org.apache.spark.streaming.util.FileBasedWriteAheadLogReader]] to read.
 *
 * @param logDirectory Directory when rotating log files will be created.
 * @param hadoopConf Hadoop configuration for reading/writing log files.
 *
 * FileBasedWriteAheadLog 表示监听一个根目录,在根目录下定期创建一个文件
 * 向文件中写入真正的数据块内容,返回一个FileBasedWriteAheadLogSegment对象作为返回值,该对象表示这个数据块写入到哪个文件下了,从该文件的第几个字节开始属于该数据块的,一共写入多少个字节
 */
private[streaming] class FileBasedWriteAheadLog(
    conf: SparkConf,
    logDirectory: String,//向哪个目录写入日志
    hadoopConf: Configuration,
    rollingIntervalSecs: Int,//切换文件的时间间隔,单位是秒
    maxFailures: Int //写入的最大失败次数
  ) extends WriteAheadLog with Logging {

  import FileBasedWriteAheadLog._

  private val pastLogs = new ArrayBuffer[LogInfo] //已经完成的文件集合
  private val callerNameTag = getCallerName.map(c => s" for $c").getOrElse("")

  private val threadpoolName = s"WriteAheadLogManager $callerNameTag"
  implicit private val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonSingleThreadExecutor(threadpoolName))
  override protected val logName = s"WriteAheadLogManager $callerNameTag"


  private var currentLogPath: Option[String] = None //当前正在写入的日志文件路径
  private var currentLogWriter: FileBasedWriteAheadLogWriter = null //当前正在写入的日志文件
  private var currentLogWriterStartTime: Long = -1L //当前日志文件处理的开始时间
  private var currentLogWriterStopTime: Long = -1L //当前日志文件处理的结束时间

  initializeOrRecover()

  /**
   * Write a byte buffer to the log file. This method synchronously writes the data in the
   * ByteBuffer to HDFS. When this method returns, the data is guaranteed to have been flushed
   * to HDFS, and will be available for readers to read.
   * 将数据写入到日志文件中,参数time可以帮助找到数据内容应该写入到哪个文件中
   */
  def write(byteBuffer: ByteBuffer, time: Long): FileBasedWriteAheadLogSegment = synchronized {
    var fileSegment: FileBasedWriteAheadLogSegment = null //文件头对象

    var failures = 0 //失败次数
    var lastException: Exception = null //最后一次失败的异常
    var succeeded = false //true表示写入成功

    while (!succeeded && failures < maxFailures) {
      try {
        fileSegment = getLogWriter(time).write(byteBuffer) //写入数据
        succeeded = true
      } catch {
        case ex: Exception =>
          lastException = ex
          logWarning("Failed to write to write ahead log")
          resetWriter()
          failures += 1
      }
    }
    if (fileSegment == null) {
      logError(s"Failed to write to write ahead log after $failures failures")
      throw lastException
    }
    fileSegment
  }

  //读取一个数据段落,传入数据段落头文件即可
  def read(segment: WriteAheadLogRecordHandle): ByteBuffer = {
    val fileSegment = segment.asInstanceOf[FileBasedWriteAheadLogSegment]
    var reader: FileBasedWriteAheadLogRandomReader = null
    var byteBuffer: ByteBuffer = null
    try {
      reader = new FileBasedWriteAheadLogRandomReader(fileSegment.path, hadoopConf) //根据文件路径,创建读取流
      byteBuffer = reader.read(fileSegment) //读取一个数据段落
    } finally {
      reader.close()
    }
    byteBuffer //返回段落内容
  }

  /**
   * Read all the existing logs from the log directory.
   *
   * Note that this is typically called when the caller is initializing and wants
   * to recover past state from the write ahead logs (that is, before making any writes).
   * If this is called after writes have been made using this manager, then it may not return
   * the latest the records. This does not deal with currently active log files, and
   * hence the implementation is kept simple.
   * 读取所有已经完成的文件以及当前处理的文件
   */
  def readAll(): JIterator[ByteBuffer] = synchronized {
    import scala.collection.JavaConversions._
    val logFilesToRead = pastLogs.map{ _.path} ++ currentLogPath //读取所有已经完成的文件以及当前处理的文件  路径集合
    logInfo("Reading from the logs: " + logFilesToRead.mkString("\n"))

    logFilesToRead.iterator.map { file =>
      logDebug(s"Creating log reader with $file")
      val reader = new FileBasedWriteAheadLogReader(file, hadoopConf) //从头读取文件
      CompletionIterator[ByteBuffer, Iterator[ByteBuffer]](reader, reader.close _)
    } flatMap { x => x }
  }

  /**
   * Delete the log files that are older than the threshold time.
   *
   * Its important to note that the threshold time is based on the time stamps used in the log
   * files, which is usually based on the local system time. So if there is coordination necessary
   * between the node calculating the threshTime (say, driver node), and the local system time
   * (say, worker node), the caller has to take account of possible time skew.
   *
   * If waitForCompletion is set to true, this method will return only after old logs have been
   * deleted. This should be set to true only for testing. Else the files will be deleted
   * asynchronously.
   *
   * 参数waitForCompletion为true,表示只有当老文件都删除后,该方法才会被返回
   */
  def clean(threshTime: Long, waitForCompletion: Boolean): Unit = {
    val oldLogFiles = synchronized { pastLogs.filter { _.endTime < threshTime } } //获取老文件集合
    logInfo(s"Attempting to clear ${oldLogFiles.size} old log files in $logDirectory " +
      s"older than $threshTime: ${oldLogFiles.map { _.path }.mkString("\n")}")

    //删除文件
    def deleteFiles() {
      oldLogFiles.foreach { logInfo =>
        try {
          val path = new Path(logInfo.path)
          val fs = HdfsUtils.getFileSystemForPath(path, hadoopConf)
          fs.delete(path, true) //删除物理文件
          synchronized { pastLogs -= logInfo } //删除内存映射
          logDebug(s"Cleared log file $logInfo")
        } catch {
          case ex: Exception =>
            logWarning(s"Error clearing write ahead log file $logInfo", ex)
        }
      }
      logInfo(s"Cleared log files in $logDirectory older than $threshTime")
    }
    if (!executionContext.isShutdown) {
      val f = Future { deleteFiles() }//已经执行了该删除方法,属于异步执行
      if (waitForCompletion) {//true,表示只有当老文件都删除后,该方法才会被返回
        import scala.concurrent.duration._
        Await.ready(f, 1 second)
      }
    }
  }


  /** Stop the manager, close any open log writer */
  def close(): Unit = synchronized {
    if (currentLogWriter != null) {
      currentLogWriter.close()
    }
    executionContext.shutdown()
    logInfo("Stopped write ahead log manager")
  }

  /** Get the current log writer while taking care of rotation
    * 获取时间参数对应的文件是哪个
    **/
  private def getLogWriter(currentTime: Long): FileBasedWriteAheadLogWriter = synchronized {
    if (currentLogWriter == null || currentTime > currentLogWriterStopTime) {//说明文件不存在,或者当前时间已经超过了当前文件的最后时间范围了
      resetWriter() //关闭当前的文件
      currentLogPath.foreach {
        pastLogs += LogInfo(currentLogWriterStartTime, currentLogWriterStopTime, _) //追加一个数据块LogInfo  其中_表示当前文件路径
      }
      currentLogWriterStartTime = currentTime
      currentLogWriterStopTime = currentTime + (rollingIntervalSecs * 1000)
      val newLogPath = new Path(logDirectory,
        timeToLogFile(currentLogWriterStartTime, currentLogWriterStopTime)) //新文件的路径
      currentLogPath = Some(newLogPath.toString)
      currentLogWriter = new FileBasedWriteAheadLogWriter(currentLogPath.get, hadoopConf) //创建新文件输出流
    }
    currentLogWriter
  }

  /** Initialize the log directory or recover existing logs inside the directory */
  private def initializeOrRecover(): Unit = synchronized {
    val logDirectoryPath = new Path(logDirectory) //日志目录
    val fileSystem = HdfsUtils.getFileSystemForPath(logDirectoryPath, hadoopConf) //文件系统

    if (fileSystem.exists(logDirectoryPath) && fileSystem.getFileStatus(logDirectoryPath).isDir) {
      val logFileInfo = logFilesTologInfo(fileSystem.listStatus(logDirectoryPath).map { _.getPath }) //根目录下所有子目录对应的文件路径集合去排序,文件名字的格式是log-开始时间-结束时间
      pastLogs.clear()
      pastLogs ++= logFileInfo
      logInfo(s"Recovered ${logFileInfo.size} write ahead log files from $logDirectory") //打印日志说明目录下有多少个日志文件
      logDebug(s"Recovered files are:\n${logFileInfo.map(_.path).mkString("\n")}")
    }
  }

  private def resetWriter(): Unit = synchronized {
    if (currentLogWriter != null) {
      currentLogWriter.close()
      currentLogWriter = null
    }
  }
}

private[streaming] object FileBasedWriteAheadLog {

  case class LogInfo(startTime: Long, endTime: Long, path: String)

  val logFileRegex = """log-(\d+)-(\d+)""".r

  def timeToLogFile(startTime: Long, stopTime: Long): String = {
    s"log-$startTime-$stopTime"
  }

  def getCallerName(): Option[String] = {
    val stackTraceClasses = Thread.currentThread.getStackTrace().map(_.getClassName)
    stackTraceClasses.find(!_.contains("WriteAheadLog")).flatMap(_.split(".").lastOption)
  }

  /** Convert a sequence of files to a sequence of sorted LogInfo objects
    * 将文件集合转换成LogInfo集合,并且LogInfo集合是有顺序的
    **/
  def logFilesTologInfo(files: Seq[Path]): Seq[LogInfo] = {
    files.flatMap { file =>
      logFileRegex.findFirstIn(file.getName()) match { //只要文件名字符合正则表达式,则就按照开始时间进行排序
        case Some(logFileRegex(startTimeStr, stopTimeStr)) =>
          val startTime = startTimeStr.toLong
          val stopTime = stopTimeStr.toLong
          Some(LogInfo(startTime, stopTime, file.toString))
        case None =>
          None
      }
    }.sortBy { _.startTime }
  }
}
