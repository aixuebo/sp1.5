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

/**
 * An object that waits for a DAGScheduler job to complete. As tasks finish, it passes their
 * results to the given handler function.
  * 一个对象,等候job完成,每一个任务task完成都会调用resultHandler函数,传递task的Id和task的结果集
 */
private[spark] class JobWaiter[T](//T是task的结果集
    dagScheduler: DAGScheduler,
    val jobId: Int,//job所属id
    totalTasks: Int,//该job总共多少个task任务,即有多少个partition分区
    resultHandler: (Int, T) => Unit)//该函数说明该index的任务完成了,第二个参数是task的返回值
  extends JobListener {

  private var finishedTasks = 0 //已经完成的任务task数量

  // Is the job as a whole finished (succeeded or failed)?
  @volatile
  private var _jobFinished = totalTasks == 0 //标示job状态是否完成,先与0比较,判断是否完成该job

  def jobFinished: Boolean = _jobFinished

  // If the job is finished, this will be its result. In the case of 0 task jobs (e.g. zero
  // partition RDDs), we set the jobResult directly to JobSucceeded.
  private var jobResult: JobResult = if (jobFinished) JobSucceeded else null

  /**
   * Sends a signal to the DAGScheduler to cancel the job. The cancellation itself is handled
   * asynchronously. After the low level scheduler cancels all the tasks belonging to this job, it
   * will fail this job with a SparkException.
    * 发送一个信号去取消该job,该任务是异步的
   */
  def cancel() {
    dagScheduler.cancelJob(jobId)
  }

  //说明一个task已经完成了
  override def taskSucceeded(index: Int, result: Any): Unit = synchronized {
    if (_jobFinished) {//job肯定不能完成
      throw new UnsupportedOperationException("taskSucceeded() called on a finished JobWaiter")
    }

    //调用函数,说明该index的任务完成了,第二个参数是task的返回值
    resultHandler(index, result.asInstanceOf[T])
    finishedTasks += 1
    if (finishedTasks == totalTasks) {//说明全部完成了
      _jobFinished = true//设置完成状态
      jobResult = JobSucceeded//设置结果集
      this.notifyAll()
    }
  }

  //说明job结束了
  override def jobFailed(exception: Exception): Unit = synchronized {
    _jobFinished = true //设置job状态为完成
    jobResult = JobFailed(exception) //设置带有异常参数的返回值
    this.notifyAll()
  }

  //等待最终结果
  def awaitResult(): JobResult = synchronized {
    while (!_jobFinished) {//如果没有完成job,则一直等待
      this.wait()
    }
    return jobResult//返回结果
  }
}
