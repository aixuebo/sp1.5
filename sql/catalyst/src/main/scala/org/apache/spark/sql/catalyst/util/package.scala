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

package org.apache.spark.sql.catalyst

import java.io._

import org.apache.spark.util.Utils

package object util {

  /** Silences output to stderr or stdout for the duration of f
    * 执行f函数的时候,静悄悄的,不进行日志的输出
    **/
  def quietly[A](f: => A): A = {
    //保存现在的输出
    val origErr = System.err
    val origOut = System.out
    try {
      //设置输出日志为空
      System.setErr(new PrintStream(new OutputStream {
        def write(b: Int) = {}
      }))
      System.setOut(new PrintStream(new OutputStream {
        def write(b: Int) = {}
      }))

      f
    } finally {
      //还原以前的输出
      System.setErr(origErr)
      System.setOut(origOut)
    }
  }

  //将文件内容转换成字符串
  def fileToString(file: File, encoding: String = "UTF-8"): String = {
    val inStream = new FileInputStream(file)
    val outStream = new ByteArrayOutputStream
    try {
      var reading = true
      while ( reading ) {
        inStream.read() match {//一个字节一个字节读
          case -1 => reading = false
          case c => outStream.write(c)
        }
      }
      outStream.flush()
    }
    finally {
      inStream.close()
    }
    new String(outStream.toByteArray, encoding)
  }

  //用classloader去读取resource资源,返回资源对应的字节数组
  def resourceToBytes(
      resource: String,
      classLoader: ClassLoader = Utils.getSparkClassLoader): Array[Byte] = {
    val inStream = classLoader.getResourceAsStream(resource)
    val outStream = new ByteArrayOutputStream
    try {
      var reading = true
      while ( reading ) {
        inStream.read() match {
          case -1 => reading = false
          case c => outStream.write(c)
        }
      }
      outStream.flush()
    }
    finally {
      inStream.close()
    }
    outStream.toByteArray
  }

  //使用classloader加载资源,将资源内容转换为String
  def resourceToString(
      resource: String,
      encoding: String = "UTF-8",
      classLoader: ClassLoader = Utils.getSparkClassLoader): String = {
    new String(resourceToBytes(resource, classLoader), encoding)
  }

  //将String内容写入到文件file中
  def stringToFile(file: File, str: String): File = {
    val out = new PrintWriter(file)
    out.write(str)
    out.close()
    file
  }

  //字符串按照\n进行拆分成集合
  def sideBySide(left: String, right: String): Seq[String] = {
    sideBySide(left.split("\n"), right.split("\n"))
  }

  def sideBySide(left: Seq[String], right: Seq[String]): Seq[String] = {
    val maxLeftSize = left.map(_.size).max //left中存储的字符串里面,最长的字符串是多少个长度位置
    //让left和right最终set集合数量是相同的,即谁size最大,最终集合的长度就依赖谁,补充的数据是""
    val leftPadded = left ++ Seq.fill(math.max(right.size - left.size, 0))("")
    val rightPadded = right ++ Seq.fill(math.max(left.size - right.size, 0))("")

    //left和right每一个元素一比一组成元组
    //left和right元素相同 输出left right
    //left和right元素不相同 输出!left right
    leftPadded.zip(rightPadded).map {
      case (l, r) => (if (l == r) " " else "!") + l + (" " * ((maxLeftSize - l.size) + 3)) + r //注意left和right之间的空格是left与left最大的长度差距+3,即最大的left有3个空格,保证所有right都在相同的位置上出现
    }
  }

  //将异常转换成字符串
  def stackTraceToString(t: Throwable): String = {
    val out = new java.io.ByteArrayOutputStream
    val writer = new PrintWriter(out)
    t.printStackTrace(writer)
    writer.flush()
    new String(out.toByteArray)
  }

  //对参数进行toStr8ing转换,如果是null则返回null
  def stringOrNull(a: AnyRef): String = if (a == null) null else a.toString

  //执行f函数,并且打印耗时
  def benchmark[A](f: => A): A = {
    val startTime = System.nanoTime()
    val ret = f //执行f函数.返回ret
    val endTime = System.nanoTime()
    // scalastyle:off println
    println(s"${(endTime - startTime).toDouble / 1000000}ms")
    // scalastyle:on println
    ret
  }

  /* FIX ME
  implicit class debugLogging(a: Any) {
    def debugLogging() {
      org.apache.log4j.Logger.getLogger(a.getClass.getName).setLevel(org.apache.log4j.Level.DEBUG)
    }
  } */
}
