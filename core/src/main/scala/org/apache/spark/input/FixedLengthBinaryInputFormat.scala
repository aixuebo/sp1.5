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

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}

import org.apache.spark.Logging
import org.apache.spark.deploy.SparkHadoopUtil

/**
 * Custom Input Format for reading and splitting flat binary files that contain records,
 * each of which are a fixed size in bytes. The fixed record size is specified through
 * a parameter recordLength in the Hadoop configuration.
  *
  * 从数据块中每次读取固定大小的字节
  * key是读取该数据块第几个固定大小字节,value是读取的固定大小字节内容
 */
private[spark] object FixedLengthBinaryInputFormat {
  /** Property name to set in Hadoop JobConfs for record length */
  val RECORD_LENGTH_PROPERTY = "org.apache.spark.input.FixedLengthBinaryInputFormat.recordLength"

  /** Retrieves the record length property from a Hadoop configuration */
  def getRecordLength(context: JobContext): Int = {
    SparkHadoopUtil.get.getConfigurationFromJobContext(context).get(RECORD_LENGTH_PROPERTY).toInt//从Configuration中获取int元素
  }
}

private[spark] class FixedLengthBinaryInputFormat
  extends FileInputFormat[LongWritable, BytesWritable]
  with Logging {

  private var recordLength = -1

  /**
   * Override of isSplitable to ensure initial computation of the record length
   */
  override def isSplitable(context: JobContext, filename: Path): Boolean = {
    if (recordLength == -1) {
      recordLength = FixedLengthBinaryInputFormat.getRecordLength(context)
    }
    if (recordLength <= 0) {
      logDebug("record length is less than 0, file cannot be split")
      false
    } else {
      true
    }
  }

  /**
   * This input format overrides computeSplitSize() to make sure that each split
   * only contains full records. Each InputSplit passed to FixedLengthBinaryRecordReader
   * will start at the first byte of a record, and the last byte will the last byte of a record.
   */
  override def computeSplitSize(blockSize: Long, minSize: Long, maxSize: Long): Long = {
    val defaultSize = super.computeSplitSize(blockSize, minSize, maxSize)//计算一个数据块多少个字节
    // If the default size is less than the length of a record, make it equal to it
    // Otherwise, make sure the split size is as close to possible as the default size,
    // but still contains a complete set of records, with the first record
    // starting at the first byte in the split and the last record ending with the last byte
    if (defaultSize < recordLength) { //必须保证每次读取recordLength固定长度的字节,因此计算的每一个数据块的大小必须是recordLength的倍数,因此设置1倍的recordLength
      recordLength.toLong
    } else {
      (Math.floor(defaultSize / recordLength) * recordLength).toLong //将数据块的大小设置为固定值的整数倍
    }
  }

  /**
   * Create a FixedLengthBinaryRecordReader
   */
  override def createRecordReader(split: InputSplit, context: TaskAttemptContext)
      : RecordReader[LongWritable, BytesWritable] = {
    new FixedLengthBinaryRecordReader
  }
}
