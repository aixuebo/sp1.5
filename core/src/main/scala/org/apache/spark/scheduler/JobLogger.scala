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

package org.apache.spark.scheduler

import java.io.{File, FileNotFoundException, IOException, PrintWriter}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import scala.collection.mutable.HashMap

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.TaskMetrics

/**
 * :: DeveloperApi ::
 * A logger class to record runtime information for jobs in Spark. This class outputs one log file
 * for each Spark job, containing tasks start/stop and shuffle information. JobLogger is a subclass
 * of SparkListener, use addSparkListener to add JobLogger to a SparkContext after the SparkContext
 * is created. Note that each JobLogger only works for one SparkContext
 * 一个logger类记录spark的job的运行期记录日志,这个类是一个job对应一个输出文件
  * 文件内容包含任务的开始和结束,shuffle的信息
  * JobLogger是SparkListener的子类,在SparkContext创建后,使用addSparkListener方法添加到SparkContext中的
  * 注意该对象仅仅工作在一个SparkContext中
  *
 * NOTE: The functionality of this class is heavily stripped down to accommodate for a general
 * refactor of the SparkListener interface. In its place, the EventLoggingListener is introduced
 * to log application information as SparkListenerEvents. To enable this functionality, set
 * spark.eventLog.enabled to true.
 * 
 * 每一个jobId对应一个日志文件
 * 所有的日志文件都存储在logDir/logDirName目录下
 */
@DeveloperApi
@deprecated("Log application information by setting spark.eventLog.enabled.", "1.0.0")
class JobLogger(val user: String, val logDirName: String) extends SparkListener with Logging {

  //默认第二个参数logDirName.文件夹名字是时间戳
  def this() = this(System.getProperty("user.name", "<unknown>"),//用户名
    String.valueOf(System.currentTimeMillis()))//时间戳

    //获取base文件夹
  private val logDir =
    if (System.getenv("SPARK_LOG_DIR") != null) {
      System.getenv("SPARK_LOG_DIR")
    } else {
      "/tmp/spark-%s".format(user)
    }

  //每一个jobId对应一个日志文件PrintWriter,因为一个jobid对应一个日志文件输出
  private val jobIdToPrintWriter = new HashMap[Int, PrintWriter]

  //因为一个job是包含多个stage的
  //每一个阶段ID,一定对应一个JobId,但是一个jobId是对应多个阶段ID的
  private val stageIdToJobId = new HashMap[Int, Int]
  //key是JobId,value是该Job对应的阶段ID集合
  private val jobIdToStageIds = new HashMap[Int, Seq[Int]]
  
  //获取格式化时间对象
  private val dateFormat = new ThreadLocal[SimpleDateFormat]() {
    override def initialValue(): SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
  }

  //初始化目录
  createLogDir()

  /** Create a folder for log files, the folder's name is the creation time of jobLogger 
   *  创建一个目录,目录的名字是创建jobLogger的时间戳
   **/
  protected def createLogDir() {
    val dir = new File(logDir + "/" + logDirName + "/")
    if (dir.exists()) {
      return
    }
    if (!dir.mkdirs()) {
      // JobLogger should throw a exception rather than continue to construct this object.
      throw new IOException("create log directory error:" + logDir + "/" + logDirName + "/")
    }
  }

  /**
   * Create a log file for one job
   * @param jobId ID of the job
   * @throws FileNotFoundException Fail to create log file
   * 为jobId创建一个日志文件
   */
  protected def createLogWriter(jobId: Int) {
    try {
      val fileWriter = new PrintWriter(logDir + "/" + logDirName + "/" + jobId)
      jobIdToPrintWriter += (jobId -> fileWriter)
    } catch {
      case e: FileNotFoundException => e.printStackTrace()
    }
  }

  /**
   * Close log file, and clean the stage relationship in stageIdToJobId
   * @param jobId ID of the job
   * 关闭一个jobId,并且清理内存关系
   */
  protected def closeLogWriter(jobId: Int) {
    jobIdToPrintWriter.get(jobId).foreach { fileWriter =>
      fileWriter.close()//关闭文件流

      //将该job对应的stage都删除掉
      jobIdToStageIds.get(jobId).foreach(_.foreach { stageId =>
        stageIdToJobId -= stageId
      })

      //删除掉job映射关系
      jobIdToPrintWriter -= jobId
      jobIdToStageIds -= jobId
    }
  }

  /**
   * Build up the maps that represent stage-job relationships
   * @param jobId ID of the job job的ID
   * @param stageIds IDs of the associated stages 该jobId对一个哪些阶段集合
   * 关联JobId与他对应的阶段ID集合关系
   */
  protected def buildJobStageDependencies(jobId: Int, stageIds: Seq[Int]) = {
    jobIdToStageIds(jobId) = stageIds //设置jobId与stage集合的映射关系
    stageIds.foreach { stageId => stageIdToJobId(stageId) = jobId }//设置jobId与stage集合的映射关系
  }

  /**
   * Write info into log file
    * 想jobId对应的log文件写入信息
   * @param jobId ID of the job
   * @param info Info to be recorded 要写入的信息
   * @param withTime Controls whether to record time stamp before the info, default is true 信息内容前是否要加入时间格式
   * 将info信息写入jobId对应的日志文件中,如果withTime=true,则要先写入当前时间
   */
  protected def jobLogInfo(jobId: Int, info: String, withTime: Boolean = true) {
    var writeInfo = info
    if (withTime) {//是否要写入时间戳
      val date = new Date(System.currentTimeMillis())
      writeInfo = dateFormat.get.format(date) + ": " + info //加入时间格式
    }
    // scalastyle:off println
    jobIdToPrintWriter.get(jobId).foreach(_.println(writeInfo))//打印该job的信息
    // scalastyle:on println
  }

  /**
   * Write info into log file
    * 一个阶段产生的日志,写入到该阶段对应的jobid日志中
   * @param stageId ID of the stage
   * @param info Info to be recorded
   * @param withTime Controls whether to record time stamp before the info, default is true
   * 将info信息写入jobId对应的日志文件中,如果withTime=true,则要先写入当前时间
   * 只不过该info信息是在某一个阶段产生的日志而已
   */
  protected def stageLogInfo(stageId: Int, info: String, withTime: Boolean = true) {
    stageIdToJobId.get(stageId).foreach(jobId => jobLogInfo(jobId, info, withTime))
  }

  /**
   * Record task metrics into job log files, including execution info and shuffle metrics
   * @param stageId Stage ID of the task
   * @param status Status info of the task
   * @param taskInfo Task description info
   * @param taskMetrics Task running metrics
   */
  //任务完成的时候,打印任务的统计信息的内容
  protected def recordTaskMetrics(stageId: Int, status: String,
                                taskInfo: TaskInfo, taskMetrics: TaskMetrics) {
    val info = " TID=" + taskInfo.taskId + " STAGE_ID=" + stageId +
               " START_TIME=" + taskInfo.launchTime + " FINISH_TIME=" + taskInfo.finishTime +
               " EXECUTOR_ID=" + taskInfo.executorId +  " HOST=" + taskMetrics.hostname
    val executorRunTime = " EXECUTOR_RUN_TIME=" + taskMetrics.executorRunTime
    val gcTime = " GC_TIME=" + taskMetrics.jvmGCTime
    val inputMetrics = taskMetrics.inputMetrics match {
      case Some(metrics) =>
        " READ_METHOD=" + metrics.readMethod.toString +
        " INPUT_BYTES=" + metrics.bytesRead
      case None => ""
    }
    val outputMetrics = taskMetrics.outputMetrics match {
      case Some(metrics) =>
        " OUTPUT_BYTES=" + metrics.bytesWritten
      case None => ""
    }
    val shuffleReadMetrics = taskMetrics.shuffleReadMetrics match {
      case Some(metrics) =>
        " BLOCK_FETCHED_TOTAL=" + metrics.totalBlocksFetched +
        " BLOCK_FETCHED_LOCAL=" + metrics.localBlocksFetched +
        " BLOCK_FETCHED_REMOTE=" + metrics.remoteBlocksFetched +
        " REMOTE_FETCH_WAIT_TIME=" + metrics.fetchWaitTime +
        " REMOTE_BYTES_READ=" + metrics.remoteBytesRead +
        " LOCAL_BYTES_READ=" + metrics.localBytesRead
      case None => ""
    }
    val writeMetrics = taskMetrics.shuffleWriteMetrics match {
      case Some(metrics) =>
        " SHUFFLE_BYTES_WRITTEN=" + metrics.shuffleBytesWritten +
        " SHUFFLE_WRITE_TIME=" + metrics.shuffleWriteTime
      case None => ""
    }
    stageLogInfo(stageId, status + info + executorRunTime + gcTime + inputMetrics + outputMetrics +
      shuffleReadMetrics + writeMetrics)
  }

  /**
   * When stage is submitted, record stage submit info
   * @param stageSubmitted Stage submitted event 阶段被提交事件
   * 记录该阶段被提交了,同时该阶段有多少个任务
   */
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) {
    val stageInfo = stageSubmitted.stageInfo
    stageLogInfo(stageInfo.stageId, "STAGE_ID=%d STATUS=SUBMITTED TASK_SIZE=%d".format(
      stageInfo.stageId, stageInfo.numTasks))//记录日志
  }

  /**
   * When stage is completed, record stage completion status
   * @param stageCompleted Stage completed event 阶段完成事件
   * 记录该阶段成功完成了,还是失败完成了
   */
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
    val stageId = stageCompleted.stageInfo.stageId
    if (stageCompleted.stageInfo.failureReason.isEmpty) {//记录日志
      stageLogInfo(stageId, s"STAGE_ID=$stageId STATUS=COMPLETED")
    } else {
      stageLogInfo(stageId, s"STAGE_ID=$stageId STATUS=FAILED")
    }
  }

  /**
   * When task ends, record task completion status and metrics
   * @param taskEnd Task end event 任务完成事件
   * 每一个任务完成后记录的日志
   */
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    val taskInfo = taskEnd.taskInfo
    var taskStatus = "TASK_TYPE=%s".format(taskEnd.taskType)
    val taskMetrics = if (taskEnd.taskMetrics != null) taskEnd.taskMetrics else TaskMetrics.empty
    taskEnd.reason match {
      case Success => taskStatus += " STATUS=SUCCESS"
        recordTaskMetrics(taskEnd.stageId, taskStatus, taskInfo, taskMetrics)
      case Resubmitted =>
        taskStatus += " STATUS=RESUBMITTED TID=" + taskInfo.taskId +
                      " STAGE_ID=" + taskEnd.stageId
        stageLogInfo(taskEnd.stageId, taskStatus)
      case FetchFailed(bmAddress, shuffleId, mapId, reduceId, message) =>
        taskStatus += " STATUS=FETCHFAILED TID=" + taskInfo.taskId + " STAGE_ID=" +
                      taskEnd.stageId + " SHUFFLE_ID=" + shuffleId + " MAP_ID=" +
                      mapId + " REDUCE_ID=" + reduceId
        stageLogInfo(taskEnd.stageId, taskStatus)//打印任务完成日志
      case _ =>
    }
  }

  /**
   * When job ends, recording job completion status and close log file
   * @param jobEnd Job end event job结束事件
   * job完成,写入成功完成日志,以及失败信息日志
   */
  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    val jobId = jobEnd.jobId
    var info = "JOB_ID=" + jobId
    jobEnd.jobResult match {
      case JobSucceeded => info += " STATUS=SUCCESS"
      case JobFailed(exception) =>
        info += " STATUS=FAILED REASON="
        exception.getMessage.split("\\s+").foreach(info += _ + "_")
      case _ =>
    }
    jobLogInfo(jobId, info.substring(0, info.length - 1).toUpperCase) //记录该job的日志信息
    closeLogWriter(jobId) //关闭该close的输出流,清理映射关系
  }

  /**
   * Record job properties into job log file
   * @param jobId ID of the job
   * @param properties Properties of the job
   * 写入该job的描述信息
   */
  protected def recordJobProperties(jobId: Int, properties: Properties) {
    if (properties != null) {
      val description = properties.getProperty(SparkContext.SPARK_JOB_DESCRIPTION, "")
      jobLogInfo(jobId, description, withTime = false)
    }
  }

  /**
   * When job starts, record job property and stage graph
   * @param jobStart Job start event job启动事件
   * job启动
   */
  override def onJobStart(jobStart: SparkListenerJobStart) {
    val jobId = jobStart.jobId //获取jobId
    val properties = jobStart.properties //启动属性
    //为该job创建日志文件
    createLogWriter(jobId)
    //将该job的描述写入到日志中
    recordJobProperties(jobId, properties)
    //设置该job以及对应的阶段集合映射关系
    buildJobStageDependencies(jobId, jobStart.stageIds)
    //记录该job开始日志
    jobLogInfo(jobId, "JOB_ID=" + jobId + " STATUS=STARTED")
  }
}
