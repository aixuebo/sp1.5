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

package org.apache.spark.deploy.worker

import java.io._

import scala.collection.JavaConversions._

import com.google.common.base.Charsets.UTF_8
import com.google.common.io.Files

import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.{SecurityManager, SparkConf, Logging}
import org.apache.spark.deploy.{ApplicationDescription, ExecutorState}
import org.apache.spark.deploy.DeployMessages.ExecutorStateChanged
import org.apache.spark.util.{ShutdownHookManager, Utils}
import org.apache.spark.util.logging.FileAppender

/**
 * Manages the execution of one executor process.
 * This is currently only used in standalone mode.
 * worker接受到master传来的LaunchExecutor事件,启动一个执行者执行命令
 */
private[deploy] class ExecutorRunner(
    val appId: String,//master传递过来的执行者所属app的appId
    val execId: Int,//master传递过来的执行者Id,执行者的唯一标示
    val appDesc: ApplicationDescription,//关于执行者的app任务描述
    val cores: Int,//master传递过来的该执行者需要使用多少cpu
    val memory: Int,//master传递过来的该执行者需要使用多少内存
    val worker: RpcEndpointRef,//本地worker的通信对象
    val workerId: String,//本地workerID,worker的唯一标示
    val host: String,//本地worker的host
    val webUiPort: Int,//本地worker的webUi的port
    val publicAddress: String,//本地worker对外公开的地址
    val sparkHome: File,//本地worker配置的sparkHome
    val executorDir: File,//执行者的工作目录是worker/appID/execId
    val workerUrl: String,//本地worker的url
    conf: SparkConf,//本地worker的总配置信息
    val appLocalDirs: Seq[String],//关于该执行者所属的app在本地的存储路径集合,因为是多个磁盘,因此是集合
    @volatile var state: ExecutorState.Value)//执行者在本地的状态
  extends Logging {

  private val fullId = appId + "/" + execId
  private var workerThread: Thread = null //开启工作线程去执行fetchAndRunExecutor方法
  private var process: Process = null //执行工作的进程对象
  private var stdoutAppender: FileAppender = null
  private var stderrAppender: FileAppender = null

  // NOTE: This is now redundant with the automated shut-down enforced by the Executor. It might
  // make sense to remove this in the future.
  private var shutdownHook: AnyRef = null //设置一个钩子,当workershut down时候kill该执行者

  //worker的执行者开始入口
  private[worker] def start() {
    
    //开启工作线程去执行fetchAndRunExecutor方法
    workerThread = new Thread("ExecutorRunner for " + fullId) {
      override def run() { fetchAndRunExecutor() } //下载资源,并且运行执行者程序
    }
    workerThread.start()
    // Shutdown hook that kills actors on shutdown.
    //设置一个钩子,当workershut down时候kill该执行者
    shutdownHook = ShutdownHookManager.addShutdownHook { () =>
      killProcess(Some("Worker shutting down")) }
  }

  /**
   * Kill executor process, wait for exit and notify worker to update resource status.
   * 执行者被kill掉,等候退出,并且通知worker更新资源状态
   * @param message the exception message which caused the executor's death 退出的异常信息,是说明原因导致执行者死亡的
   * 
   */
  private def killProcess(message: Option[String]) {
    var exitCode: Option[Int] = None
    if (process != null) {
      
      //先关闭日志
      logInfo("Killing process!")
      if (stdoutAppender != null) {
        stdoutAppender.stop()
      }
      if (stderrAppender != null) {
        stderrAppender.stop()
      }
      
      //进城关闭
      process.destroy()
      
      //返回退出状态码
      exitCode = Some(process.waitFor())
    }
    
    //向worker发送信息,执行者状态变更
    worker.send(ExecutorStateChanged(appId, execId, state, message, exitCode))
  }

  /** Stop this executor runner, including killing the process it launched 
   *  kill掉工作者线程,并且更改执行者状态,从钩子中移除
   **/
  private[worker] def kill() {
    if (workerThread != null) {
      // the workerThread will kill the child process when interrupted
      workerThread.interrupt()
      workerThread = null
      state = ExecutorState.KILLED
      try {
        ShutdownHookManager.removeShutdownHook(shutdownHook)
      } catch {
        case e: IllegalStateException => None
      }
    }
  }

  /** Replace variables such as {{EXECUTOR_ID}} and {{CORES}} in a command argument passed to us 
   *  代替环境变量
   **/
  private[worker] def substituteVariables(argument: String): String = argument match {
    case "{{WORKER_URL}}" => workerUrl
    case "{{EXECUTOR_ID}}" => execId.toString
    case "{{HOSTNAME}}" => host
    case "{{CORES}}" => cores.toString
    case "{{APP_ID}}" => appId
    case other => other
  }

  /**
   * Download and run the executor described in our ApplicationDescription
   * 下载资源,并且运行执行者程序
   */
  private def fetchAndRunExecutor() {
    try {
      // Launch the process
      val builder = CommandUtils.buildProcessBuilder(appDesc.command, new SecurityManager(conf),
        memory, sparkHome.getAbsolutePath, substituteVariables)
      val command = builder.command()
      logInfo("Launch command: " + command.mkString("\"", "\" \"", "\"")) //打印启动命令日志

      builder.directory(executorDir) //设置本地工作目录
      
      //设置环境变量信息
      builder.environment.put("SPARK_EXECUTOR_DIRS", appLocalDirs.mkString(File.pathSeparator))
      // In case we are running this from within the Spark Shell, avoid creating a "scala"
      // parent process for the executor command
      builder.environment.put("SPARK_LAUNCH_WITH_SCALA", "0")

      // Add webUI log urls 设置web页面访问日志的页面
      val baseUrl =
        s"http://$publicAddress:$webUiPort/logPage/?appId=$appId&executorId=$execId&logType="
      builder.environment.put("SPARK_LOG_URL_STDERR", s"${baseUrl}stderr")
      builder.environment.put("SPARK_LOG_URL_STDOUT", s"${baseUrl}stdout")

      //开始运行命令脚本
      process = builder.start()
      val header = "Spark Executor Command: %s\n%s\n\n".format(
        command.mkString("\"", "\" \"", "\""), "=" * 40)

        //将正常的输出流输出到工作目录的stdout文件里
      // Redirect its stdout and stderr to files
      val stdout = new File(executorDir, "stdout")
      stdoutAppender = FileAppender(process.getInputStream, stdout, conf)

      //将错误的输出流输出到工作目录的stderr文件里
      val stderr = new File(executorDir, "stderr")
      Files.write(header, stderr, UTF_8)
      stderrAppender = FileAppender(process.getErrorStream, stderr, conf)

      // Wait for it to exit; executor may exit with code 0 (when driver instructs it to shutdown)
      // or with nonzero exit code等候退出状态
      val exitCode = process.waitFor()
      state = ExecutorState.EXITED //设置执行者状态
      val message = "Command exited with code " + exitCode
      //向worker发送执行者状态更改信息
      worker.send(ExecutorStateChanged(appId, execId, state, Some(message), Some(exitCode)))
    } catch {
      case interrupted: InterruptedException => {
        logInfo("Runner thread for executor " + fullId + " interrupted")
        state = ExecutorState.KILLED
        killProcess(None)
      }
      case e: Exception => {
        logError("Error running executor", e)
        state = ExecutorState.FAILED
        killProcess(Some(e.toString))
      }
    }
  }
}
