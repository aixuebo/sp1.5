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

package org.apache.spark.streaming

import java.io._
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkException, SparkConf, Logging}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.{MetadataCleaner, Utils}
import org.apache.spark.streaming.scheduler.JobGenerator


private[streaming]
class Checkpoint(@transient ssc: StreamingContext, val checkpointTime: Time) //checkpointTime表示此时执行checkpoint的时间点
  extends Logging with Serializable { //该对象表示一次checkPoint
  val master = ssc.sc.master
  val framework = ssc.sc.appName
  val jars = ssc.sc.jars
  val graph = ssc.graph
  val checkpointDir = ssc.checkpointDir
  val checkpointDuration = ssc.checkpointDuration //checkpoint的周期
  val pendingTimes = ssc.scheduler.getPendingTimes().toArray //此时正在调度器中调度的jobset集合
  val delaySeconds = MetadataCleaner.getDelaySeconds(ssc.conf)
  val sparkConfPairs = ssc.conf.getAll

  def createSparkConf(): SparkConf = {

    // Reload properties for the checkpoint application since user wants to set a reload property
    // or spark had changed its value and user wants to set it back.
    //需要重新覆盖的配置key
    val propertiesToReload = List(
      "spark.driver.host",
      "spark.driver.port",
      "spark.master",
      "spark.yarn.keytab",
      "spark.yarn.principal")

    val newSparkConf = new SparkConf(loadDefaults = false).setAll(sparkConfPairs)
      .remove("spark.driver.host")
      .remove("spark.driver.port")
    val newReloadConf = new SparkConf(loadDefaults = true)
    propertiesToReload.foreach { prop =>
      newReloadConf.getOption(prop).foreach { value =>
        newSparkConf.set(prop, value)
      }
    }
    newSparkConf
  }

  def validate() {
    assert(master != null, "Checkpoint.master is null")
    assert(framework != null, "Checkpoint.framework is null")
    assert(graph != null, "Checkpoint.graph is null")
    assert(checkpointTime != null, "Checkpoint.checkpointTime is null")
    logInfo("Checkpoint for time " + checkpointTime + " validated")
  }
}

private[streaming]
object Checkpoint extends Logging {
  val PREFIX = "checkpoint-" //文件名字的前缀
  val REGEX = (PREFIX + """([\d]+)([\w\.]*)""").r //文件名字的正则表达式

  /** Get the checkpoint file for the given checkpoint time
    * 时间戳文件  格式:checkpoint-时间戳
    **/
  def checkpointFile(checkpointDir: String, checkpointTime: Time): Path = {
    new Path(checkpointDir, PREFIX + checkpointTime.milliseconds)
  }

  /** Get the checkpoint backup file for the given checkpoint time
    * 备份文件  格式:checkpoint-时间戳.bk
    **/
  def checkpointBackupFile(checkpointDir: String, checkpointTime: Time): Path = {
    new Path(checkpointDir, PREFIX + checkpointTime.milliseconds + ".bk")
  }

  /** Get checkpoint files present in the give directory, ordered by oldest-first
    * 去hdfs上读取一个目录path下文件
    * 返回排序后的子文件路径集合
    **/
  def getCheckpointFiles(checkpointDir: String, fsOption: Option[FileSystem] = None): Seq[Path] = {

    //排序算法,先比较时间戳,然后比较名字
    def sortFunc(path1: Path, path2: Path): Boolean = {
      val (time1, bk1) = path1.getName match { case REGEX(x, y) => (x.toLong, !y.isEmpty) } //根据正则表达式,获取时间戳和名字
      val (time2, bk2) = path2.getName match { case REGEX(x, y) => (x.toLong, !y.isEmpty) }
      (time1 < time2) || (time1 == time2 && bk1)
    }

    val path = new Path(checkpointDir)
    val fs = fsOption.getOrElse(path.getFileSystem(SparkHadoopUtil.get.conf))
    if (fs.exists(path)) {
      val statuses = fs.listStatus(path)
      if (statuses != null) {
        val paths = statuses.map(_.getPath) //文件下所有子文件集合
        val filtered = paths.filter(p => REGEX.findFirstIn(p.toString).nonEmpty) //找到符合正则表达式的文件
        filtered.sortWith(sortFunc) //排序
      } else {
        logWarning("Listing " + path + " returned null")
        Seq.empty
      }
    } else {
      logInfo("Checkpoint directory " + path + " does not exist")
      Seq.empty
    }
  }

  /** Serialize the checkpoint, or throw any exception that occurs
    *
    * 将Checkpoint对象  序列化并且压缩,然后返回字节数组
    **/
  def serialize(checkpoint: Checkpoint, conf: SparkConf): Array[Byte] = {
    val compressionCodec = CompressionCodec.createCodec(conf) //压缩方式
    val bos = new ByteArrayOutputStream() //输出流
    val zos = compressionCodec.compressedOutputStream(bos) //对输出流包装一层压缩

    val oos = new ObjectOutputStream(zos) //序列化该Checkpoint对象,将Checkpoint对象的内容序列化,并且压缩,存储到bos中
    Utils.tryWithSafeFinally {
      oos.writeObject(checkpoint)
    } {
      oos.close()
    }
    bos.toByteArray //返回Checkpoint对象 序列化并且压缩后的字节数组
  }

  /** Deserialize a checkpoint from the input stream, or throw any exception that occurs
    * 将字节数组转换成Checkpoint对象
    **/
  def deserialize(inputStream: InputStream, conf: SparkConf): Checkpoint = {
    val compressionCodec = CompressionCodec.createCodec(conf) //压缩算法
    var ois: ObjectInputStreamWithLoader = null
    Utils.tryWithSafeFinally {

      // ObjectInputStream uses the last defined user-defined class loader in the stack
      // to find classes, which maybe the wrong class loader. Hence, a inherited version
      // of ObjectInputStream is used to explicitly use the current thread's default class
      // loader to find and load classes. This is a well know Java issue and has popped up
      // in other places (e.g., http://jira.codehaus.org/browse/GROOVY-1627)
      val zis = compressionCodec.compressedInputStream(inputStream) //对字节数组流进行解压缩包装

      //将字节数组范序列化成Checkpoint对象
      ois = new ObjectInputStreamWithLoader(zis,
        Thread.currentThread().getContextClassLoader)
      val cp = ois.readObject.asInstanceOf[Checkpoint]
      cp.validate()
      cp
    } {
      if (ois != null) {
        ois.close()
      }
    }
  }
}


/**
 * Convenience class to handle the writing of graph checkpoint to file
 */
private[streaming]
class CheckpointWriter(
    jobGenerator: JobGenerator,
    conf: SparkConf,
    checkpointDir: String,
    hadoopConf: Configuration
  ) extends Logging {
  val MAX_ATTEMPTS = 3//最大尝试次数
  val executor = Executors.newFixedThreadPool(1)
  val compressionCodec = CompressionCodec.createCodec(conf)
  private var stopped = false
  private var fs_ : FileSystem = _

  class CheckpointWriteHandler(
      checkpointTime: Time,//在做checkpoint的时间
      bytes: Array[Byte],//要写入到输出流中的字节内容
      clearCheckpointDataLater: Boolean) //是否在checkpoint后要对数据进行清理
      extends Runnable {
    def run() {
      var attempts = 0
      val startTime = System.currentTimeMillis()
      val tempFile = new Path(checkpointDir, "temp") //临时文件夹tmp
      val checkpointFile = Checkpoint.checkpointFile(checkpointDir, checkpointTime) // 获取文件 格式 checkpoint-时间戳
      val backupFile = Checkpoint.checkpointBackupFile(checkpointDir, checkpointTime) // 获取文件 格式checkpoint-时间戳.bk

      while (attempts < MAX_ATTEMPTS && !stopped) {//只要不停止,并且尝试次数还够,就不断循环
        attempts += 1
        try {
          logInfo("Saving checkpoint for time " + checkpointTime + " to file '" + checkpointFile
            + "'")

          // Write checkpoint to temp file
          if (fs.exists(tempFile)) {
            fs.delete(tempFile, true)   // just in case it exists 删除临时文件
          }
          val fos = fs.create(tempFile) //创建临时文件输出流

          //将字节内容写入到临时文件流中,并且关闭输出流
          Utils.tryWithSafeFinally {
            fos.write(bytes)
          } {
            fos.close()
          }

          // If the checkpoint file exists, back it up
          // If the backup exists as well, just delete it, otherwise rename will fail
          if (fs.exists(checkpointFile)) {
            if (fs.exists(backupFile)){//删除备份目录
              fs.delete(backupFile, true) // just in case it exists
            }
            if (!fs.rename(checkpointFile, backupFile)) {//创建新的备份
              logWarning("Could not rename " + checkpointFile + " to " + backupFile)
            }
          }

          // Rename temp file to the final checkpoint file
          if (!fs.rename(tempFile, checkpointFile)) {//临时文件变成新的文件
            logWarning("Could not rename " + tempFile + " to " + checkpointFile)
          }

          // Delete old checkpoint files 删除老的10个以上的文件
          val allCheckpointFiles = Checkpoint.getCheckpointFiles(checkpointDir, Some(fs))
          if (allCheckpointFiles.size > 10) {//超过10个
            allCheckpointFiles.take(allCheckpointFiles.size - 10).foreach(file => {
              logInfo("Deleting " + file)
              fs.delete(file, true)
            })
          }

          // All done, print success
          val finishTime = System.currentTimeMillis()
          logInfo("Checkpoint for time " + checkpointTime + " saved to file '" + checkpointFile +
            "', took " + bytes.length + " bytes and " + (finishTime - startTime) + " ms")
          jobGenerator.onCheckpointCompletion(checkpointTime, clearCheckpointDataLater)
          return
        } catch {
          case ioe: IOException =>
            logWarning("Error in attempt " + attempts + " of writing checkpoint to "
              + checkpointFile, ioe)
            reset()
        }
      }

      //最终依然不成功,则打印日志说checkpoint不成功
      logWarning("Could not write checkpoint for time " + checkpointTime + " to file "
        + checkpointFile + "'")
    }
  }

  def write(checkpoint: Checkpoint, clearCheckpointDataLater: Boolean) {
    try {
      val bytes = Checkpoint.serialize(checkpoint, conf) //将Checkpoint对象序列化成字节数组
      executor.execute(new CheckpointWriteHandler(
        checkpoint.checkpointTime, bytes, clearCheckpointDataLater)) //将字节数组用线程池写入到磁盘上
      logDebug("Submitted checkpoint of time " + checkpoint.checkpointTime + " writer queue")
    } catch {
      case rej: RejectedExecutionException =>
        logError("Could not submit checkpoint task to the thread pool executor", rej)
    }
  }

  def stop(): Unit = synchronized {
    if (stopped) return

    executor.shutdown()
    val startTime = System.currentTimeMillis()
    val terminated = executor.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)
    if (!terminated) {
      executor.shutdownNow()
    }
    val endTime = System.currentTimeMillis()
    logInfo("CheckpointWriter executor terminated ? " + terminated +
      ", waited for " + (endTime - startTime) + " ms.")
    stopped = true
  }

  private def fs = synchronized {
    if (fs_ == null) fs_ = new Path(checkpointDir).getFileSystem(hadoopConf)
    fs_
  }

  private def reset() = synchronized {
    fs_ = null
  }
}


private[streaming]
object CheckpointReader extends Logging {

  /**
   * Read checkpoint files present in the given checkpoint directory. If there are no checkpoint
   * files, then return None, else try to return the latest valid checkpoint object. If no
   * checkpoint files could be read correctly, then return None.
   */
  def read(checkpointDir: String): Option[Checkpoint] = {
    read(checkpointDir, new SparkConf(), SparkHadoopUtil.get.conf, ignoreReadError = true)
  }

  /**
   * Read checkpoint files present in the given checkpoint directory. If there are no checkpoint
   * files, then return None, else try to return the latest valid checkpoint object. If no
   * checkpoint files could be read correctly, then return None (if ignoreReadError = true),
   * or throw exception (if ignoreReadError = false).
   * 从hadoop的hdfs上path读取一个输入源,
   */
  def read(
      checkpointDir: String,//要读取的路径
      conf: SparkConf,//spark配置
      hadoopConf: Configuration,//hadoop配置
      ignoreReadError: Boolean = false) //是否忽略异常
      : Option[Checkpoint] = {
    val checkpointPath = new Path(checkpointDir)

    // TODO(rxin): Why is this a def?!
    def fs: FileSystem = checkpointPath.getFileSystem(hadoopConf)

    // Try to find the checkpoint files
    val checkpointFiles = Checkpoint.getCheckpointFiles(checkpointDir, Some(fs)).reverse //读取所有的有效文件,并且按照顺序从大到小排列
    if (checkpointFiles.isEmpty) {
      return None
    }

    // Try to read the checkpoint files in the order
    logInfo("Checkpoint files found: " + checkpointFiles.mkString(",")) //打印日志说找到了哪些文件路径
    val compressionCodec = CompressionCodec.createCodec(conf)
    checkpointFiles.foreach(file => {//循环每一个文件,反序列化文件内容
      logInfo("Attempting to load checkpoint from file " + file)
      try {
        val fis = fs.open(file)
        val cp = Checkpoint.deserialize(fis, conf) //反序列化文件内容
        logInfo("Checkpoint successfully loaded from file " + file)
        logInfo("Checkpoint was generated at time " + cp.checkpointTime)
        return Some(cp)
      } catch {
        case e: Exception =>
          logWarning("Error reading checkpoint from file " + file, e)
      }
    })

    // If none of checkpoint files could be read, then throw exception
    if (!ignoreReadError) { //false表示不忽略异常,因此如果有异常的时候要抛出
      throw new SparkException(s"Failed to read checkpoint from directory $checkpointPath")
    }
    None
  }
}

private[streaming]
class ObjectInputStreamWithLoader(inputStream_ : InputStream, loader: ClassLoader)
  extends ObjectInputStream(inputStream_) {

  override def resolveClass(desc: ObjectStreamClass): Class[_] = {
    try {
      return loader.loadClass(desc.getName())
    } catch {
      case e: Exception =>
    }
    super.resolveClass(desc)
  }
}
