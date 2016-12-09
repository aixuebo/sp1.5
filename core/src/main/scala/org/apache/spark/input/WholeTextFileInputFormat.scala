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

package org.apache.spark.input

import scala.collection.JavaConversions._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext

/**
 * A [[org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat CombineFileInputFormat]] for
 * reading whole text files. Each file is read as key-value pair, where the key is the file path and
 * the value is the entire content of file.
  * 读取文件的全部内容,key是文件的路径path,value是文件内容
  *
  * 因为这种文件应该都是小文件,因此用到了CombineFileInputFormat类,该类可以让一个若干个文件组成一个数据块
 */

private[spark] class WholeTextFileInputFormat
  extends CombineFileInputFormat[String, String] with Configurable {

  //每一个文件是不允许拆分的,因为要读取全部文件内容
  override protected def isSplitable(context: JobContext, file: Path): Boolean = false

  override def createRecordReader(
      split: InputSplit,//该数据块是包含多个文件的分片对象
      context: TaskAttemptContext): RecordReader[String, String] = {

    val reader =
      new ConfigurableCombineFileRecordReader(split, context, classOf[WholeTextFileRecordReader])
    reader.setConf(getConf)
    reader
  }

  /**
   * Allow minPartitions set by end-user in order to keep compatibility with old Hadoop API,
   * which is set through setMaxSplitSize
    * 设置每一个分片的最大字节数
    *
    * 参数minPartitions 表示至少spark也要用这些个分区处理原始数据
   */
  def setMinPartitions(context: JobContext, minPartitions: Int) {
    val files = listStatus(context)//获取输入源对应的全部文件
    //用scala语法循环所有的文件,计算所有文件的字节
    val totalLen = files.map { file =>
      if (file.isDir) 0L else file.getLen
    }.sum

    //设置每一个split最大多少个字节
    val maxSplitSize = Math.ceil(totalLen * 1.0 /
      (if (minPartitions == 0) 1 else minPartitions)).toLong
    super.setMaxSplitSize(maxSplitSize)
  }
}
