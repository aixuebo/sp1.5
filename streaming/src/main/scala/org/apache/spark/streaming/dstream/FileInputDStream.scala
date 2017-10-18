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

package org.apache.spark.streaming.dstream

import java.io.{IOException, ObjectInputStream}

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}

import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.streaming._
import org.apache.spark.streaming.scheduler.StreamInputInfo
import org.apache.spark.util.{SerializableConfiguration, TimeStampedHashMap, Utils}

/**
 * This class represents an input stream that monitors a Hadoop-compatible filesystem for new
 * files and creates a stream out of them. The way it works as follows.
 *
 * At each batch interval, the file system is queried for files in the given directory and
 * detected new files are selected for that batch. In this case "new" means files that
 * became visible to readers during that time period. Some extra care is needed to deal
 * with the fact that files may become visible after they are created. For this purpose, this
 * class remembers the information about the files selected in past batches for
 * a certain duration (say, "remember window") as shown in the figure below.
 * 在每一个批处理时间内,去查找文件符合该批次下的文件集合
 *
 *                      |<----- remember window ----->|
 * ignore threshold --->|                             |<--- current batch time
 *                      |____.____.____.____.____.____|
 *                      |    |    |    |    |    |    |
 * ---------------------|----|----|----|----|----|----|-----------------------> Time
 *                      |____|____|____|____|____|____|
 *                             remembered batches
 *
 * The trailing end of the window is the "ignore threshold" and all files whose mod times
 * are less than this threshold are assumed to have already been selected and are therefore
 * ignored. Files whose mod times are within the "remember window" are checked against files
 * that have already been selected. At a high level, this is how new files are identified in
 * each batch - files whose mod times are greater than the ignore threshold and
 * have not been considered within the remember window. See the documentation on the method
 * `isNewFile` for more details.
 *
 * This makes some assumptions from the underlying file system that the system is monitoring.
 * - The clock of the file system is assumed to synchronized with the clock of the machine running
 *   the streaming app.
 * - If a file is to be visible in the directory listings, it must be visible within a certain
 *   duration of the mod time of the file. This duration is the "remember window", which is set to
 *   1 minute (see `FileInputDStream.minRememberDuration`). Otherwise, the file will never be
 *   selected as the mod time will be less than the ignore threshold when it becomes visible.
 * - Once a file is visible, the mod time cannot change. If it does due to appends, then the
 *   processing semantics are undefined.
 *   读取hdfs上的一个文件目录
 *
 *   根据目录参数,不断的找到该目录下以前没有加入的文件,将其转换成RDD,即读取文件产生RDD
 */
private[streaming]
class FileInputDStream[K, V, F <: NewInputFormat[K, V]]( //K和V表示hdfs上存储的key和value的类型,F是如何读取该文件的格式,比如TextInputFormat
    @transient ssc_ : StreamingContext,
    directory: String,//要加载的path路径,该path是一个目录
    filter: Path => Boolean = FileInputDStream.defaultFilter, //true表示要该path,false表示该路径非法
    newFilesOnly: Boolean = true,
    conf: Option[Configuration] = None)
    (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F])
  extends InputDStream[(K, V)](ssc_) {

  private val serializableConfOpt = conf.map(new SerializableConfiguration(_))//序列化conf对象

  /**
   * Minimum duration of remembering the information of selected files. Defaults to 60 seconds.
   *
   * Files with mod times older than this "window" of remembering will be ignored. So if new
   * files are visible within this window, then the file will get selected in the next batch.
   */
  private val minRememberDurationS = {
    Seconds(ssc.conf.getTimeAsSeconds("spark.streaming.fileStream.minRememberDuration",
      ssc.conf.get("spark.streaming.minRememberDuration", "60s")))
  }

  // This is a def so that it works during checkpoint recovery:
  private def clock = ssc.scheduler.clock

  // Data to be saved as part of the streaming checkpoints
  protected[streaming] override val checkpointData = new FileInputDStreamCheckpointData

  // Initial ignore threshold based on which old, existing files in the directory (at the time of
  // starting the streaming application) will be ignored or considered
  private val initialModTimeIgnoreThreshold = if (newFilesOnly) clock.getTimeMillis() else 0L

  /*
   * Make sure that the information of files selected in the last few batches are remembered.
   * This would allow us to filter away not-too-old files which have already been recently
   * selected and processed.
   */
  private val numBatchesToRemember = FileInputDStream
    .calculateNumBatchesToRemember(slideDuration, minRememberDurationS)
  private val durationToRemember = slideDuration * numBatchesToRemember
  remember(durationToRemember)

  // Map of batch-time to selected file info for the remembered batches
  // This is a concurrent map because it's also accessed in unit tests
  //每个时间戳下找到了哪些文件集合
  @transient private[streaming] var batchTimeToSelectedFiles =
    new mutable.HashMap[Time, Array[String]] with mutable.SynchronizedMap[Time, Array[String]]

  // Set of files that were selected in the remembered batches 已经被选择的文件集合
  @transient private var recentlySelectedFiles = new mutable.HashSet[String]()

  // Read-through cache of file mod times, used to speed up mod time lookups
  @transient private var fileToModTime = new TimeStampedHashMap[String, Long](true) //true表示无论什么操作都会更新时间戳

  // Timestamp of the last round of finding files
  @transient private var lastNewFileFindingTime = 0L

  @transient private var path_ : Path = null //目录
  @transient private var fs_ : FileSystem = null

  override def start() { }

  override def stop() { }

  /**
   * Finds the files that were modified since the last time this method was called and makes
   * a union RDD out of them. Note that this maintains the list of files that were processed
   * in the latest modification time in the previous call to this method. This is because the
   * modification time returned by the FileStatus API seems to return times only at the
   * granularity of seconds. And new files may have the same modification time as the
   * latest modification time in the previous call to this method yet was not reported in
   * the previous call.
   */
  override def compute(validTime: Time): Option[RDD[(K, V)]] = {
    // Find new files
    val newFiles = findNewFiles(validTime.milliseconds) //找到新文件
    logInfo("New files at time " + validTime + ":\n" + newFiles.mkString("\n")) //找到文件集合
    batchTimeToSelectedFiles += ((validTime, newFiles))
    recentlySelectedFiles ++= newFiles
    val rdds = Some(filesToRDD(newFiles)) //转换成RDD
    // Copy newFiles to immutable.List to prevent from being modified by the user
    val metadata = Map(
      "files" -> newFiles.toList,
      StreamInputInfo.METADATA_KEY_DESCRIPTION -> newFiles.mkString("\n"))
    val inputInfo = StreamInputInfo(id, 0, metadata)//文件描述,描述这次处理的是哪些文件集合
    ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)
    rdds
  }

  /** Clear the old time-to-files mappings along with old RDDs */
  protected[streaming] override def clearMetadata(time: Time) {
    super.clearMetadata(time)
    val oldFiles = batchTimeToSelectedFiles.filter(_._1 < (time - rememberDuration))//筛选老文件
    batchTimeToSelectedFiles --= oldFiles.keys
    recentlySelectedFiles --= oldFiles.values.flatten
    logInfo("Cleared " + oldFiles.size + " old files that were older than " +
      (time - rememberDuration) + ": " + oldFiles.keys.mkString(", "))
    logDebug("Cleared files are:\n" +
      oldFiles.map(p => (p._1, p._2.mkString(", "))).mkString("\n"))
    // Delete file mod times that weren't accessed in the last round of getting new files
    fileToModTime.clearOldValues(lastNewFileFindingTime - 1)
  }

  /**
   * Find new files for the batch of `currentTime`. This is done by first calculating the
   * ignore threshold for file mod times, and then getting a list of files filtered based on
   * the current batch time and the ignore threshold. The ignore threshold is the max of
   * initial ignore threshold and the trailing end of the remember window (that is, which ever
   * is later in time).
   * 去找新文件---找到currentTime前产生的文件
   */
  private def findNewFiles(currentTime: Long): Array[String] = {
    try {
      lastNewFileFindingTime = clock.getTimeMillis()

      // Calculate ignore threshold 因为currentTime前有很多文件,因此要进一步过滤,比如durationToRemember周期内可能文件正在创作中,因此不去加载
      val modTimeIgnoreThreshold = math.max(
        initialModTimeIgnoreThreshold,   // initial threshold based on newFilesOnly setting
        currentTime - durationToRemember.milliseconds  // trailing end of the remember window
      )
      logDebug(s"Getting new files for time $currentTime, " +
        s"ignoring files older than $modTimeIgnoreThreshold")

      //筛选是否是新文件的条件
      val filter = new PathFilter {
        def accept(path: Path): Boolean = isNewFile(path, currentTime, modTimeIgnoreThreshold)
      }
      val newFiles = fs.listStatus(directoryPath, filter).map(_.getPath.toString) //新文件集合

      val timeTaken = clock.getTimeMillis() - lastNewFileFindingTime //找文件时间
      logInfo("Finding new files took " + timeTaken + " ms") //打印日志找新文件花费的时间
      logDebug("# cached file times = " + fileToModTime.size)
      if (timeTaken > slideDuration.milliseconds) {
        logWarning(
          "Time taken to find new files exceeds the batch size. " +
            "Consider increasing the batch size or reducing the number of " +
            "files in the monitored directory."
        ) //打印警告日志,说明找文件花费时间较久
      }
      newFiles
    } catch {
      case e: Exception =>
        logWarning("Error finding new files", e)
        reset()
        Array.empty
    }
  }

  /**
   * Identify whether the given `path` is a new file for the batch of `currentTime`. For it to be
   * accepted, it has to pass the following criteria.
   * - It must pass the user-provided file filter.必须通过用户定义的文件名称过滤器
   * - It must be newer than the ignore threshold. It is assumed that files older than the ignore
   *   threshold have already been considered or are existing files before start
   *   (when newFileOnly = true). 必须是新产生的文件,即在modTimeIgnoreThreshold之后产生的文件才是选择范围内的
   *
   * - It must not be present in the recently selected files that this class remembers.不能再已经选择的集合内存在
   *
   * - It must not be newer than the time of the batch (i.e. `currentTime` for which this
   *   file is being tested. This can occur if the driver was recovered, and the missing batches
   *   (during downtime) are being generated. In that case, a batch of time T may be generated
   *   at time T+x. Say x = 5. If that batch T contains file of mod time T+5, then bad things can
   *   happen. Let's say the selected files are remembered for 60 seconds.  At time t+61,
   *   the batch of time t is forgotten, and the ignore threshold is still T+1.
   *   The files with mod time T+5 are not remembered and cannot be ignored (since, t+5 > t+1).
   *   Hence they can get selected as new files again. To prevent this, files whose mod time is more
   *   than current batch time are not considered. 必须不能超过currentTime时间上限
   *
   *   该函数确定path是否是需要的文件
   *   true表示要该path,false表示该路径非法
   *
   *   参数currentTime 表示此次操作的时间上限
   *   参数modTimeIgnoreThreshold 表示一个伐值,比该伐值小的时间戳,都忽略掉,即在modTimeIgnoreThreshold之前产生的文件都忽略掉
   */
  private def isNewFile(path: Path, currentTime: Long, modTimeIgnoreThreshold: Long): Boolean = {
    val pathStr = path.toString

    //1.首先校验path名字合法性
    // Reject file if it does not satisfy filter
    //true表示要该path,false表示该路径非法
    if (!filter(path)) {//非法,因此返回false
      logDebug(s"$pathStr rejected by filter")
      return false
    }
    // Reject file if it was created before the ignore time
    //2.校验文件的最后修改时间
    val modTime = getFileModTime(path)
    if (modTime <= modTimeIgnoreThreshold) {//忽略之前产生的文件
      // Use <= instead of < to avoid SPARK-4518
      logDebug(s"$pathStr ignored as mod time $modTime <= ignore time $modTimeIgnoreThreshold")
      return false
    }

    //最后修改时间大于此次操作时间的也忽略掉
    // Reject file if mod time > current batch time
    if (modTime > currentTime) {
      logDebug(s"$pathStr not selected as mod time $modTime > current time $currentTime")
      return false
    }

    // Reject file if it was considered earlier
    //说明已经选择该文件了,因此也会被忽略掉
    if (recentlySelectedFiles.contains(pathStr)) {
      logDebug(s"$pathStr already considered")
      return false
    }

    logDebug(s"$pathStr accepted with mod time $modTime")
    return true
  }

  /** Generate one RDD from an array of files
    * 参数file表示文件路径集合,将文件集合组装成一个RDD
    **/
  private def filesToRDD(files: Seq[String]): RDD[(K, V)] = {
    //获取文件的RDD集合
    val fileRDDs = files.map { file =>
      val rdd = serializableConfOpt.map(_.value) match {//看配置文件是什么类型的文件RDD
        case Some(config) => context.sparkContext.newAPIHadoopFile(
          file,
          fm.runtimeClass.asInstanceOf[Class[F]],
          km.runtimeClass.asInstanceOf[Class[K]],
          vm.runtimeClass.asInstanceOf[Class[V]],
          config)
        case None => context.sparkContext.newAPIHadoopFile[K, V, F](file)
      }
      if (rdd.partitions.size == 0) {//说明没有数据
        logError("File " + file + " has no data in it. Spark Streaming can only ingest " +
          "files that have been \"moved\" to the directory assigned to the file stream. " +
          "Refer to the streaming programming guide for more details.")
      }
      rdd
    }
    //全部文件RDD集合组成新的RDD
    new UnionRDD(context.sparkContext, fileRDDs)
  }

  /** Get file mod time from cache or fetch it from the file system
    * 获取文件的更新时间,如果不存在,则返回文件的最后修改时间
    **/
  private def getFileModTime(path: Path) = {
    fileToModTime.getOrElseUpdate(path.toString, fs.getFileStatus(path).getModificationTime())
  }

  //获取目录
  private def directoryPath: Path = {
    if (path_ == null) path_ = new Path(directory)
    path_
  }

  private def fs: FileSystem = {
    if (fs_ == null) fs_ = directoryPath.getFileSystem(ssc.sparkContext.hadoopConfiguration)
    fs_
  }

  private def reset()  {
    fs_ = null
  }

  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream): Unit = Utils.tryOrIOException {
    logDebug(this.getClass().getSimpleName + ".readObject used")
    ois.defaultReadObject()
    generatedRDDs = new mutable.HashMap[Time, RDD[(K, V)]]()
    batchTimeToSelectedFiles =
      new mutable.HashMap[Time, Array[String]] with mutable.SynchronizedMap[Time, Array[String]]
    recentlySelectedFiles = new mutable.HashSet[String]()
    fileToModTime = new TimeStampedHashMap[String, Long](true)
  }

  /**
   * A custom version of the DStreamCheckpointData that stores names of
   * Hadoop files as checkpoint data.
   */
  private[streaming]
  class FileInputDStreamCheckpointData extends DStreamCheckpointData(this) {

    private def hadoopFiles = data.asInstanceOf[mutable.HashMap[Time, Array[String]]]

    override def update(time: Time) {
      hadoopFiles.clear()
      hadoopFiles ++= batchTimeToSelectedFiles
    }

    override def cleanup(time: Time) { }

    //还原数据
    override def restore() {
      hadoopFiles.toSeq.sortBy(_._1)(Time.ordering).foreach {
        case (t, f) => {
          // Restore the metadata in both files and generatedRDDs
          logInfo("Restoring files for time " + t + " - " +
            f.mkString("[", ", ", "]") )
          batchTimeToSelectedFiles += ((t, f))
          recentlySelectedFiles ++= f
          generatedRDDs += ((t, filesToRDD(f)))
        }
      }
    }

    override def toString: String = {
      "[\n" + hadoopFiles.size + " file sets\n" +
        hadoopFiles.map(p => (p._1, p._2.mkString(", "))).mkString("\n") + "\n]"
    }
  }
}

private[streaming]
object FileInputDStream {
  //默认文件过滤器
  def defaultFilter(path: Path): Boolean = !path.getName().startsWith(".")

  /**
   * Calculate the number of last batches to remember, such that all the files selected in
   * at least last minRememberDurationS duration can be remembered.
   */
  def calculateNumBatchesToRemember(batchDuration: Duration,
                                    minRememberDurationS: Duration): Int = {
    math.ceil(minRememberDurationS.milliseconds.toDouble / batchDuration.milliseconds).toInt
  }
}
