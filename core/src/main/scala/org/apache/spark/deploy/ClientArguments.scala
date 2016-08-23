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

package org.apache.spark.deploy

import java.net.{URI, URISyntaxException}

import scala.collection.mutable.ListBuffer

import org.apache.log4j.Level
import org.apache.spark.util.{IntParam, MemoryParam, Utils}

/**
 * Command-line parser for the driver client.\
 * 该类用于解析客户端传递的参数,如何启动一个任务和杀死一个任务
 */
private[deploy] class ClientArguments(args: Array[String]) {
  import ClientArguments._

  var cmd: String = "" // 'launch' or 'kill'之一,一个是启动任务,一个是杀死任务
  var logLevel = Level.WARN //日志级别

  // launch parameters
  var masters: Array[String] = null //解析客户端启动时候所传的_master参数,将spark://abc,def方式转换成spark://abc, spark://def
  var jarUrl: String = "" //是主要执行的jar包存储路径,可以是HDFS路径,也可以是本地路径
  var mainClass: String = "" //执行jar包的入口类全路径
    
  private var _driverOptions = ListBuffer[String]() //启动mainClass主类所需要的参数集合,参数集合是List
  def driverOptions: Seq[String] = _driverOptions.toSeq

  var supervise: Boolean = DEFAULT_SUPERVISE //是否重新开启失败的driver
  var memory: Int = DEFAULT_MEMORY //多少内存,单位是M
  var cores: Int = DEFAULT_CORES //多少cpu

  // kill parameters
  var driverId: String = "" //当kill命令的时候,要知道kill哪个driver

  //解析参数
  parse(args.toList)

  private def parse(args: List[String]): Unit = args match {
    case ("--cores" | "-c") :: IntParam(value) :: tail =>
      cores = value //解析多少cpu
      parse(tail)

    case ("--memory" | "-m") :: MemoryParam(value) :: tail =>
      memory = value //内存参数,将字符串转换成M单位的内存
      parse(tail)

    case ("--supervise" | "-s") :: tail =>
      supervise = true //是否重新开启失败的driver
      parse(tail)

    case ("--help" | "-h") :: tail =>
      printUsageAndExit(0) //帮助

    case ("--verbose" | "-v") :: tail =>
      logLevel = Level.INFO //是否打印日志更详细一些
      parse(tail)

    case "launch" :: _master :: _jarUrl :: _mainClass :: tail =>
      cmd = "launch" 

      //_master是master节点ip:port方式
      //_jarUrl是主要执行的jar包存储路径
      //_mainClass 执行jar包的入口类全路径
      if (!ClientArguments.isValidJarUrl(_jarUrl)) {//jar包格式不正确.必须存在启动的jar包
        // scalastyle:off println
        println(s"Jar url '${_jarUrl}' is not in valid format.")
        println(s"Must be a jar file path in URL format " +
          "(e.g. hdfs://host:port/XX.jar, file:///XX.jar)")
        // scalastyle:on println
        printUsageAndExit(-1)
      }

      jarUrl = _jarUrl
      masters = Utils.parseStandaloneMasterUrls(_master) //解析客户端启动时候所传的_master参数,将spark://abc,def方式转换成spark://abc, spark://def
      mainClass = _mainClass
      _driverOptions ++= tail //设置启动主类需要的参数集合

    case "kill" :: _master :: _driverId :: tail =>
      cmd = "kill"
      masters = Utils.parseStandaloneMasterUrls(_master) //解析客户端启动时候所传的_master参数,将spark://abc,def方式转换成spark://abc, spark://def
      driverId = _driverId

    case _ =>
      printUsageAndExit(1)
  }

  /**
   * Print usage and exit JVM with the given exit code.
   */
  private def printUsageAndExit(exitCode: Int) {
    // TODO: It wouldn't be too hard to allow users to submit their app and dependency jars
    //       separately similar to in the YARN client.
    val usage =
     s"""
      |Usage: DriverClient [options] launch <active-master> <jar-url> <main-class> [driver options]
      |Usage: DriverClient kill <active-master> <driver-id>
      |
      |Options:
      |   -c CORES, --cores CORES        Number of cores to request (default: $DEFAULT_CORES)
      |   -m MEMORY, --memory MEMORY     Megabytes of memory to request (default: $DEFAULT_MEMORY)
      |   -s, --supervise                Whether to restart the driver on failure 是否重新开启失败的driver
      |                                  (default: $DEFAULT_SUPERVISE)
      |   -v, --verbose                  Print more debugging output 表示输出更多的日志
     """.stripMargin
    // scalastyle:off println
    System.err.println(usage)
    // scalastyle:on println
    System.exit(exitCode)
  }
}

private[deploy] object ClientArguments {
  val DEFAULT_CORES = 1 //默认CPU数
  val DEFAULT_MEMORY = Utils.DEFAULT_DRIVER_MEM_MB // MB 默认内存
  val DEFAULT_SUPERVISE = false //在失败的时候,是否重启driver

  //true表示启动的jar包存在
  def isValidJarUrl(s: String): Boolean = {
    try {
      val uri = new URI(s)
      uri.getScheme != null && uri.getPath != null && uri.getPath.endsWith(".jar")
    } catch {
      case _: URISyntaxException => false
    }
  }
}
