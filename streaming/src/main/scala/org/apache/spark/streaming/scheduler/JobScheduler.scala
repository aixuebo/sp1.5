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

package org.apache.spark.streaming.scheduler

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success}

import org.apache.spark.Logging
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.streaming._
import org.apache.spark.util.{EventLoop, ThreadUtils}


private[scheduler] sealed trait JobSchedulerEvent
private[scheduler] case class JobStarted(job: Job) extends JobSchedulerEvent //开始执行jobset中的一个job
private[scheduler] case class JobCompleted(job: Job) extends JobSchedulerEvent //表示jobset中的一个job完成执行
private[scheduler] case class ErrorReported(msg: String, e: Throwable) extends JobSchedulerEvent

/**
 * This class schedules jobs to be run on Spark. It uses the JobGenerator to generate
 * the jobs and runs them using a thread pool.
 *
 * 该类在spark上进行任务job的调度工作
 * JobGenerator产生job任务,去让线程池去运行这些job
 */
private[streaming]
class JobScheduler(val ssc: StreamingContext) extends Logging {

  // Use of ConcurrentHashMap.keySet later causes an odd runtime problem due to Java 7/8 diff
  // https://gist.github.com/AlainODea/1375759b8720a3f9f094
  //已经被提交上来的job任务集合
  private val jobSets: java.util.Map[Time, JobSet] = new ConcurrentHashMap[Time, JobSet]

  private val numConcurrentJobs = ssc.conf.getInt("spark.streaming.concurrentJobs", 1) //多少个job任务可以并发执行
  private val jobExecutor = ThreadUtils.newDaemonFixedThreadPool(numConcurrentJobs, "streaming-job-executor") //job任务的线程池

  private val jobGenerator = new JobGenerator(this) //该对象会告诉我们如何定期的产生job任务
  val clock = jobGenerator.clock
  val listenerBus = new StreamingListenerBus()//事件监听器

  // These two are created only when scheduler starts.
  // eventLoop not being null means the scheduler has been started and not stopped
  var receiverTracker: ReceiverTracker = null
  // A tracker to track all the input stream information as well as processed record number
  var inputInfoTracker: InputInfoTracker = null

  //接收job的调度事件
  private var eventLoop: EventLoop[JobSchedulerEvent] = null //是一个线程

  def start(): Unit = synchronized {
    if (eventLoop != null) return // scheduler has already been started

    logDebug("Starting JobScheduler")
    eventLoop = new EventLoop[JobSchedulerEvent]("JobScheduler") {
      override protected def onReceive(event: JobSchedulerEvent): Unit = processEvent(event) //处理该job的事件

      override protected def onError(e: Throwable): Unit = reportError("Error in job scheduler", e) //事件接收异常时候执行该函数
    }
    eventLoop.start() //开启处理事件线程

    // attach rate controllers of input streams to receive batch completion updates
    for {
      inputDStream <- ssc.graph.getInputStreams
      rateController <- inputDStream.rateController
    } ssc.addStreamingListener(rateController) //添加每一个输入流对应的rate流量控制监听器

    listenerBus.start(ssc.sparkContext) //开启监听器
    receiverTracker = new ReceiverTracker(ssc)
    inputInfoTracker = new InputInfoTracker(ssc)
    receiverTracker.start()
    jobGenerator.start()
    logInfo("Started JobScheduler")
  }

  def stop(processAllReceivedData: Boolean): Unit = synchronized {
    if (eventLoop == null) return // scheduler has already been stopped
    logDebug("Stopping JobScheduler")

    // First, stop receiving
    receiverTracker.stop(processAllReceivedData)

    // Second, stop generating jobs. If it has to process all received data,
    // then this will wait for all the processing through JobScheduler to be over.
    jobGenerator.stop(processAllReceivedData)

    // Stop the executor for receiving new jobs
    logDebug("Stopping job executor")
    jobExecutor.shutdown()

    // Wait for the queued jobs to complete if indicated
    val terminated = if (processAllReceivedData) {
      jobExecutor.awaitTermination(1, TimeUnit.HOURS)  // just a very large period of time
    } else {
      jobExecutor.awaitTermination(2, TimeUnit.SECONDS)
    }
    if (!terminated) {
      jobExecutor.shutdownNow()
    }
    logDebug("Stopped job executor")

    // Stop everything else
    listenerBus.stop()
    eventLoop.stop()
    eventLoop = null
    logInfo("Stopped JobScheduler")
  }

  //是jobGenerator每隔一段时间就产生的一组要执行的任务
  def submitJobSet(jobSet: JobSet) {
    if (jobSet.jobs.isEmpty) {
      logInfo("No jobs added for time " + jobSet.time)
    } else {
      listenerBus.post(StreamingListenerBatchSubmitted(jobSet.toBatchInfo))//调度器通知该批次已经提交给job调度器了
      jobSets.put(jobSet.time, jobSet)
      jobSet.jobs.foreach(job => jobExecutor.execute(new JobHandler(job))) //线程池去调用每一个job任务
      logInfo("Added jobs for time " + jobSet.time)
    }
  }

  //有哪些jobset还在等待中
  def getPendingTimes(): Seq[Time] = {
    jobSets.keySet.toSeq
  }

  def reportError(msg: String, e: Throwable) {
    eventLoop.post(ErrorReported(msg, e))
  }

  def isStarted(): Boolean = synchronized {
    eventLoop != null
  }

  //处理job的事件---开始、完成、失败
  private def processEvent(event: JobSchedulerEvent) {
    try {
      event match {
        case JobStarted(job) => handleJobStart(job)
        case JobCompleted(job) => handleJobCompletion(job)
        case ErrorReported(m, e) => handleError(m, e)
      }
    } catch {
      case e: Throwable =>
        reportError("Error in job scheduler", e)
    }
  }

  //说明此时jobset中的一个job的任务已经开始运行了
  private def handleJobStart(job: Job) {
    val jobSet = jobSets.get(job.time) //获取该job对应的jobSet对象
    val isFirstJobOfJobSet = !jobSet.hasStarted //true表示该jobset第一次启动
    jobSet.handleJobStart(job) //通知jobSet,其中的一个job开始执行了
    if (isFirstJobOfJobSet) {//第一次启动时候产生事件
      // "StreamingListenerBatchStarted" should be posted after calling "handleJobStart" to get the
      // correct "jobSet.processingStartTime".
      listenerBus.post(StreamingListenerBatchStarted(jobSet.toBatchInfo))
    }
    logInfo("Starting job " + job.id + " from job set of time " + jobSet.time)
  }

  //说明此时jobset中的一个job的任务已经运行结束了
  private def handleJobCompletion(job: Job) {
    job.result match {
      case Success(_) =>
        val jobSet = jobSets.get(job.time)
        jobSet.handleJobCompletion(job) //通知jobSet,其中的一个job执行完了
        logInfo("Finished job " + job.id + " from job set of time " + jobSet.time)
        if (jobSet.hasCompleted) { //如果已经没有job可以执行了
          jobSets.remove(jobSet.time) //移除该jobset
          jobGenerator.onBatchCompletion(jobSet.time) //设置该jobSet全部完成
          logInfo("Total delay: %.3f s for time %s (execution: %.3f s)".format(
            jobSet.totalDelay / 1000.0, jobSet.time.toString,
            jobSet.processingDelay / 1000.0
          )) //打印日志
          listenerBus.post(StreamingListenerBatchCompleted(jobSet.toBatchInfo))//通知调度器,全部完成了job
        }
      case Failure(e) =>
        reportError("Error running job " + job, e)
    }
  }

  private def handleError(msg: String, e: Throwable) {
    logError(msg, e)
    ssc.waiter.notifyError(e)
  }

  //处理每一个Job,线程池去调用该任务
  private class JobHandler(job: Job) extends Runnable with Logging {
    def run() {
      //设置线程级别的属性信息
      ssc.sc.setLocalProperty(JobScheduler.BATCH_TIME_PROPERTY_KEY, job.time.milliseconds.toString) //该jobset的创建时间
      ssc.sc.setLocalProperty(JobScheduler.OUTPUT_OP_ID_PROPERTY_KEY, job.outputOpId.toString) //属于jobset中第几个任务
      try {
        // We need to assign `eventLoop` to a temp variable. Otherwise, because
        // `JobScheduler.stop(false)` may set `eventLoop` to null when this method is running, then
        // it's possible that when `post` is called, `eventLoop` happens to null.
        var _eventLoop = eventLoop
        if (_eventLoop != null) {
          _eventLoop.post(JobStarted(job))
          // Disable checks for existing output directories in jobs launched by the streaming
          // scheduler, since we may need to write output to an existing directory during checkpoint
          // recovery; see SPARK-4835 for more details.
          PairRDDFunctions.disableOutputSpecValidation.withValue(true) {
            job.run() //执行这个RDD
          }
          _eventLoop = eventLoop
          if (_eventLoop != null) {
            _eventLoop.post(JobCompleted(job))
          }
        } else {
          // JobScheduler has been stopped.
        }
      } finally {
        ssc.sc.setLocalProperty(JobScheduler.BATCH_TIME_PROPERTY_KEY, null)
        ssc.sc.setLocalProperty(JobScheduler.OUTPUT_OP_ID_PROPERTY_KEY, null)
      }
    }
  }
}

private[streaming] object JobScheduler {
  val BATCH_TIME_PROPERTY_KEY = "spark.streaming.internal.batchTime" //该jobset的创建时间
  val OUTPUT_OP_ID_PROPERTY_KEY = "spark.streaming.internal.outputOpId" //属于jobset中第几个任务
}
