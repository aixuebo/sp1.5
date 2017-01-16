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
 *
定义脚本
#!/bin/sh
echo "Running shell script"
while read LINE; do //不断循环每一个数据
   echo ${LINE}
done
------------
val data = List("hi","hello","how","are","you")
val dataRDD = sc.makeRDD(data,2)
val scriptPath = "/server/app/test/spark_demo/demo/pipe_demo/run.sh"
val pipeRDD = dataRDD.pipe(scriptPath)
pipeRDD.collect()

输出 Array[String] = Array(Running shell script, hi, hello, Running shell script, how, are, you)
因为有两个executor任务执行,因此每一个任务开始的时候都会调用Running shell script
-------------------
val data = List("hi","hello","how","are","you")
val dataRDD = sc.makeRDD(data,2)
def aa(bb:(String => Unit)):Unit = {
  bb("hello world")
}
val scriptPath = "/server/app/test/spark_demo/demo/pipe_demo/run.sh"
val pipeRDD = dataRDD.pipe(List(scriptPath),printPipeContext=aa)
pipeRDD.collect()
输出 res6: Array[String] = Array(Running shell script, hello world, hi, hello, Running shell script, hello world, how, are, you)

该方法会在每一次调用脚本前输出hello world
----------------------
val data = List("hi","hello","how","are","you")
val dataRDD = sc.makeRDD(data,2)
def aa(bb:(String => Unit)):Unit = {
  bb("hello world")
}
def convert(str:String,pringStr:(String => Unit)):Unit = {
  pringStr("hello world"+str)
}
val scriptPath = "/server/app/test/spark_demo/demo/pipe_demo/run.sh"
val pipeRDD = dataRDD.pipe(List(scriptPath),printPipeContext=aa,printRDDElement=convert)
pipeRDD.collect()
输出 res9: Array[String] = Array(Running shell script, hello world, hello worldhi, hello worldhello, Running shell script, hello world, hello worldhow, hello worldare, hello worldyou)
该方法对每一个调用脚本前输出hello world,每一个RDD的元素都转换成hello world+元素内容

 */
private[spark] class PipedRDD[T: ClassTag](
    prev: RDD[T],//在该RDD上操作
    command: Seq[String],//要操作的命令行
    envVars: Map[String, String],//需要的环境变量集合
    printPipeContext: (String => Unit) => Unit,//参数是一个无返回值但是有String参数的函数,比如out.print(_) 应用在初始化调用command前,打印一些信息,比如打印环境变量,我就会设置printPipeContext函数.内容就是循环所有环境变量,每一个环境变量都调用参数函数,即out.print(_),这样下一个方法的输入流中就会有环境变量信息
    printRDDElement: (T, String => Unit) => Unit,//T表示partition中每一行元素,第二个参数是一个函数,函数参数String,无返回值,可以用于传递的不是T元素,而是T元素转换一下,转换后的String调用String => Unit方法,该方法最终里面会有out.print(_),这样传入进去的内容就有变化了.不再是原有的RDD的内容了
    separateWorkingDir: Boolean) //true表示要在工作目录中进行工作,因此要创建软连接到工作目录
  extends RDD[String](prev) {

  // Similar to Runtime.exec(), if we are given a single string, split it into words
  // using a standard StringTokenizer (i.e. by spaces)
  def this(
      prev: RDD[T],
      command: String,//将命令行参数按照空格拆分成数组
      envVars: Map[String, String] = Map(),
      printPipeContext: (String => Unit) => Unit = null,
      printRDDElement: (T, String => Unit) => Unit = null,
      separateWorkingDir: Boolean = false) =
    this(prev, PipedRDD.tokenize(command), envVars, printPipeContext, printRDDElement,
      separateWorkingDir)


  override def getPartitions: Array[Partition] = firstParent[T].partitions //使用父RDD对应的partition集合,即一对一的映射处理

  /**
   * A FilenameFilter that accepts anything that isn't equal to the name passed in.
   * @param filterName of file or directory to leave out
   * 返回不等于参数名字的文件集合,即过滤参数的文件名
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
      currentEnvVars.putAll(hadoopSplit.getPipeEnvVars())//即增加文件所在hdfs路径
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

        //不断循环每一个partition内容.每一行内容作为标准输入,调用到脚本命令中
        for (elem <- firstParent[T].iterator(split, context)) {//循环该partition的每一行内容
          if (printRDDElement != null) {
            printRDDElement(elem, out.println(_))//对原始内容进行处理,处理后的结果String 输出到 out.println(_)中
          } else {
            out.println(elem)//不需要对原始内容进行处理,直接输出到 out.println(_)中
          }
        }
        // scalastyle:on println
        out.close()
      }
    }.start()

    // Return an iterator that read lines from the process's stdout 从该进程的标准输出中,读取返回的信息,这个是一个迭代器
    val lines = Source.fromInputStream(proc.getInputStream).getLines() //当进程处理完后,返回一个输出流
    new Iterator[String] {
      def next(): String = lines.next()
      def hasNext: Boolean = {
        if (lines.hasNext) {//一行一行处理partition数据数据
          true
        } else {//如果partition没有数据了
          val exitStatus = proc.waitFor()//等待进程结束
          if (exitStatus != 0) {
            throw new Exception("Subprocess exited with status " + exitStatus)
          }

          // cleanup task working directory if used
          if (workInTaskDirectory) {
            scala.util.control.Exception.ignoring(classOf[IOException]) {
              Utils.deleteRecursively(new File(taskDirectory)) //删除临时目录
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
