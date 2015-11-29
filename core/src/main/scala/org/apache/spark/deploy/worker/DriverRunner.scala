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
import org.apache.hadoop.fs.Path

import org.apache.spark.{Logging, SparkConf, SecurityManager}
import org.apache.spark.deploy.{DriverDescription, SparkHadoopUtil}
import org.apache.spark.deploy.DeployMessages.DriverStateChanged
import org.apache.spark.deploy.master.DriverState
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.{Utils, Clock, SystemClock}

/**
 * Manages the execution of one driver, including automatically restarting the driver on failure.
 * This is currently only used in standalone cluster deploy mode.
 * worker上接受LaunchDriver事件后,产生该对象
 */
private[deploy] class DriverRunner(
    conf: SparkConf,//worker上的配置信息
    val driverId: String,//master传递过来的driverId,driver的唯一标示
    val workDir: File,//该worker上的工作目录
    val sparkHome: File,//获取spark的home File对象
    val driverDesc: DriverDescription,//master传递过来关于driver的描述
    val worker: RpcEndpointRef,//worker自己本身通道
    val workerUrl: String,//worker上的webUiUrl
    val securityManager: SecurityManager)
  extends Logging {

  @volatile private var process: Option[Process] = None //执行命令工作的进程对象
  @volatile private var killed = false

  // Populated once finished 最终完成的状态、异常、返回状态码
  private[worker] var finalState: Option[DriverState] = None
  private[worker] var finalException: Option[Exception] = None
  private var finalExitCode: Option[Int] = None

  // Decoupled for testing
  def setClock(_clock: Clock): Unit = {
    clock = _clock
  }

  def setSleeper(_sleeper: Sleeper): Unit = {
    sleeper = _sleeper
  }

  private var clock: Clock = new SystemClock()
  private var sleeper = new Sleeper {
    def sleep(seconds: Int): Unit = (0 until seconds).takeWhile(f => {Thread.sleep(1000); !killed})
  }

  /** Starts a thread to run and manage the driver. 
   *  开启一个driver,这个是入口类 
   **/
  private[worker] def start() = {
    new Thread("DriverRunner for " + driverId) {
      override def run() {
        try {
          val driverDir = createWorkingDirectory() //创建work/app目录
          val localJarFilename = downloadUserJar(driverDir) //下载user的jar包,返回本地的jar包所在路径

          def substituteVariables(argument: String): String = argument match {
            case "{{WORKER_URL}}" => workerUrl
            case "{{USER_JAR}}" => localJarFilename
            case other => other
          }

          // TODO: If we add ability to submit multiple jars they should also be added here
          //构建运行该driver的命令
          val builder = CommandUtils.buildProcessBuilder(driverDesc.command, securityManager,
            driverDesc.mem, sparkHome.getAbsolutePath, substituteVariables)
            
            //运行该driver
          launchDriver(builder, driverDir, driverDesc.supervise)
        }
        catch {
          case e: Exception => finalException = Some(e)
        }

        val state =
          if (killed) {
            DriverState.KILLED
          } else if (finalException.isDefined) {
            DriverState.ERROR
          } else {
            finalExitCode match {
              case Some(0) => DriverState.FINISHED
              case _ => DriverState.FAILED
            }
          }

        finalState = Some(state)

        //向worker发送driver状态更新命令
        worker.send(DriverStateChanged(driverId, state, finalException))
      }
    }.start()
  }

  /** Terminate this driver (or prevent it from ever starting if not yet started) */
  private[worker] def kill() {
    synchronized {
      process.foreach(p => p.destroy())
      killed = true
    }
  }

  /**
   * Creates the working directory for this driver.
   * Will throw an exception if there are errors preparing the directory.
   * 创建work/app目录
   */
  private def createWorkingDirectory(): File = {
    val driverDir = new File(workDir, driverId)
    if (!driverDir.exists() && !driverDir.mkdirs()) {
      throw new IOException("Failed to create directory " + driverDir)
    }
    driverDir
  }

  /**
   * Download the user jar into the supplied directory and return its local path.
   * Will throw an exception if there are errors downloading the jar.
   * 下载用户的jar包等资源在本地的工作目录下,如果下载过程中出现异常,则抛出异常
   * @driverDir :创建的work/app目录
   * 
   * 返回本地的jar包所在路径
   */
  private def downloadUserJar(driverDir: File): String = {
    val jarPath = new Path(driverDesc.jarUrl)

    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    val destPath = new File(driverDir.getAbsolutePath, jarPath.getName) //设置目的地
    val jarFileName = jarPath.getName //jar包名
    val localJarFile = new File(driverDir, jarFileName) //本地jar包
    val localJarFilename = localJarFile.getAbsolutePath //本地的jar包所在路径

    //如果本地没有该jar包
    if (!localJarFile.exists()) { // May already exist if running multiple workers on one node
      logInfo(s"Copying user jar $jarPath to $destPath")
      Utils.fetchFile(//下载jar包到
        driverDesc.jarUrl,
        driverDir,//下载到本地目录
        conf,
        securityManager,
        hadoopConf,
        System.currentTimeMillis(),
        useCache = false)
    }

    //如果下载后jar包依然不存在,则抛异常
    if (!localJarFile.exists()) { // Verify copy succeeded
      throw new Exception(s"Did not see expected jar $jarFileName in $driverDir")
    }

    localJarFilename
  }

  //运行该driver
  private def launchDriver(builder: ProcessBuilder, baseDir: File, supervise: Boolean) {
    builder.directory(baseDir) //设置工作目录,即work/app目录
    def initialize(process: Process): Unit = {
      // Redirect stdout and stderr to files,work/app/stdout文件作为标准输出
      val stdout = new File(baseDir, "stdout")
      CommandUtils.redirectStream(process.getInputStream, stdout)

      val stderr = new File(baseDir, "stderr") //work/app/stderr文件作为异常输出
      val header = "Launch Command: %s\n%s\n\n".format(
        builder.command.mkString("\"", "\" \"", "\""), "=" * 40)
      Files.append(header, stderr, UTF_8)
      CommandUtils.redirectStream(process.getErrorStream, stderr)
    }
    
    //运行命令执行driver
    runCommandWithRetry(ProcessBuilderLike(builder), initialize, supervise)
  }

  //运行命令执行driver
  def runCommandWithRetry(
      command: ProcessBuilderLike, initialize: Process => Unit, supervise: Boolean): Unit = {
    // Time to wait between submission retries.
    var waitSeconds = 1
    // A run of this many seconds resets the exponential back-off.
    val successfulRunDuration = 5

    var keepTrying = !killed

    while (keepTrying) {
      logInfo("Launch Command: " + command.command.mkString("\"", "\" \"", "\""))

      synchronized {
        if (killed) { return }
        process = Some(command.start())
        initialize(process.get)
      }

      val processStart = clock.getTimeMillis()
      val exitCode = process.get.waitFor()
      if (clock.getTimeMillis() - processStart > successfulRunDuration * 1000) {
        waitSeconds = 1
      }

      if (supervise && exitCode != 0 && !killed) {
        logInfo(s"Command exited with status $exitCode, re-launching after $waitSeconds s.")
        sleeper.sleep(waitSeconds)
        waitSeconds = waitSeconds * 2 // exponential back-off
      }

      keepTrying = supervise && exitCode != 0 && !killed
      finalExitCode = Some(exitCode)
    }
  }
}

private[deploy] trait Sleeper {
  def sleep(seconds: Int)
}

// Needed because ProcessBuilder is a final class and cannot be mocked
private[deploy] trait ProcessBuilderLike {
  def start(): Process
  def command: Seq[String]
}

private[deploy] object ProcessBuilderLike {
  def apply(processBuilder: ProcessBuilder): ProcessBuilderLike = new ProcessBuilderLike {
    override def start(): Process = processBuilder.start()
    override def command: Seq[String] = processBuilder.command()
  }
}
