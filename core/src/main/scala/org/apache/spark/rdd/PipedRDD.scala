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

package org.apache.spark.rdd

import java.io.File
import java.io.FilenameFilter
import java.io.IOException
import java.io.PrintWriter
import java.util.StringTokenizer

import scala.collection.JavaConversions._
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.reflect.ClassTag

import org.apache.spark.{Partition, SparkEnv, TaskContext}
import org.apache.spark.util.Utils


/**
 * An RDD that pipes the contents of each parent partition through an external command
 * (printing them one per line) and returns the output as a collection of strings.
 */
private[spark] class PipedRDD[T: ClassTag](
    prev: RDD[T],
    command: Seq[String],
    envVars: Map[String, String],
    printPipeContext: (String => Unit) => Unit,//打印上下文信息,参数String就是要输出的内容
    printRDDElement: (T, String => Unit) => Unit,
    separateWorkingDir: Boolean) //true表示要在工作目录中进行工作,因此要创建软连接到工作目录
  extends RDD[String](prev) {

  // Similar to Runtime.exec(), if we are given a single string, split it into words
  // using a standard StringTokenizer (i.e. by spaces)
  def this(
      prev: RDD[T],
      command: String,
      envVars: Map[String, String] = Map(),
      printPipeContext: (String => Unit) => Unit = null,
      printRDDElement: (T, String => Unit) => Unit = null,
      separateWorkingDir: Boolean = false) =
    this(prev, PipedRDD.tokenize(command), envVars, printPipeContext, printRDDElement,
      separateWorkingDir)


  override def getPartitions: Array[Partition] = firstParent[T].partitions

  /**
   * A FilenameFilter that accepts anything that isn't equal to the name passed in.
   * @param filterName of file or directory to leave out
   * 返回不等于参数名字的文件集合
   */
  class NotEqualsFileNameFilter(filterName: String) extends FilenameFilter {
    def accept(dir: File, name: String): Boolean = {
      !name.equals(filterName)
    }
  }

  //计算一个partition
  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    val pb = new ProcessBuilder(command)
    // Add the environmental variables to the process.
    val currentEnvVars = pb.environment() //获取环境变量
    envVars.foreach { case (variable, value) => currentEnvVars.put(variable, value) } //添加自定义的环境变量

    // for compatibility with Hadoop which sets these env variables
    // so the user code can access the input filename
    if (split.isInstanceOf[HadoopPartition]) {
      val hadoopSplit = split.asInstanceOf[HadoopPartition]
      currentEnvVars.putAll(hadoopSplit.getPipeEnvVars())
    }

    // When spark.worker.separated.working.directory option is turned on, each
    // task will be run in separate directory. This should be resolve file
    // access conflict issue
    val taskDirectory = "tasks" + File.separator + java.util.UUID.randomUUID.toString //生成一个任务目录
    var workInTaskDirectory = false //true表示在任务目录taskDirectory中进行工作环境
    logDebug("taskDirectory = " + taskDirectory)
    if (separateWorkingDir) {//true表示要在工作目录中进行工作,因此要创建软连接到工作目录
      val currentDir = new File(".")
      logDebug("currentDir = " + currentDir.getAbsolutePath())
      val taskDirFile = new File(taskDirectory)
      taskDirFile.mkdirs()//创建任务目录

      try {
        val tasksDirFilter = new NotEqualsFileNameFilter("tasks")

        // Need to add symlinks to jars, files, and directories.  On Yarn we could have
        // directories and other files not known to the SparkContext that were added via the
        // Hadoop distributed cache.  We also don't want to symlink to the /tasks directories we
        // are creating here.
        for (file <- currentDir.list(tasksDirFilter)) { //在当前目录下查找不属于tasks文件名字的文件集合
          val fileWithDir = new File(currentDir, file)
          Utils.symlink(new File(fileWithDir.getAbsolutePath()),
            new File(taskDirectory + File.separator + fileWithDir.getName()))//在任务目录创建软连接
        }
        pb.directory(taskDirFile)//创建环境为任务目录
        workInTaskDirectory = true
      } catch {
        case e: Exception => logError("Unable to setup task working directory: " + e.getMessage +
          " (" + taskDirectory + ")", e)
      }
    }

    val proc = pb.start() //开始执行任务
    val env = SparkEnv.get

    // Start a thread to print the process's stderr to ours 专门打印错误信息的线程
    new Thread("stderr reader for " + command) {
      override def run() {
        for (line <- Source.fromInputStream(proc.getErrorStream).getLines) {//获取执行的错误输出,并且打印
          // scalastyle:off println
          System.err.println(line) //打印输出信息
          // scalastyle:on println
        }
      }
    }.start()

    // Start a thread to feed the process input from our parent's iterator
    //真正去执行每一个parititon信息
    new Thread("stdin writer for " + command) {
      override def run() {
        TaskContext.setTaskContext(context)
        val out = new PrintWriter(proc.getOutputStream) //创建输入流,像该管道进行写入信息

        // scalastyle:off println
        // input the pipe context firstly
        if (printPipeContext != null) {
          printPipeContext(out.println(_)) //将管道的上下文信息,写入到进程中
        }
        for (elem <- firstParent[T].iterator(split, context)) {//循环该partition的每一行内容
          if (printRDDElement != null) {
            printRDDElement(elem, out.println(_))//第二个参数是将元素写入到进程中
          } else {
            out.println(elem)//将元素写入到处理进程中
          }
        }
        // scalastyle:on println
        out.close()
      }
    }.start()

    // Return an iterator that read lines from the process's stdout
    val lines = Source.fromInputStream(proc.getInputStream).getLines() //当进程处理完后,返回一个输出流
    new Iterator[String] {
      def next(): String = lines.next()
      def hasNext: Boolean = {
        if (lines.hasNext) {
          true
        } else {
          val exitStatus = proc.waitFor()
          if (exitStatus != 0) {
            throw new Exception("Subprocess exited with status " + exitStatus)
          }

          // cleanup task working directory if used
          if (workInTaskDirectory) {
            scala.util.control.Exception.ignoring(classOf[IOException]) {
              Utils.deleteRecursively(new File(taskDirectory))
            }
            logDebug("Removed task working directory " + taskDirectory)
          }

          false
        }
      }
    }
  }
}

private object PipedRDD {
  // Split a string into words using a standard StringTokenizer
  //把命令参数使用标准拆分方式,拆分成数组命令
  def tokenize(command: String): Seq[String] = {
    val buf = new ArrayBuffer[String]
    val tok = new StringTokenizer(command)
    while(tok.hasMoreElements) {
      buf += tok.nextToken()
    }
    buf
  }
}
