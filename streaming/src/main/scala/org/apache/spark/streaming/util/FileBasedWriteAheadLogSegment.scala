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
package org.apache.spark.streaming.util

/** Class for representing a segment of data in a write ahead log file
  * 写入日志的头文件
  * 表示一次写入的数据记录,记录这段数据在path路径下第几个字节位置开始写入的数据,一共写入了多少个字节
  */
private[streaming] case class FileBasedWriteAheadLogSegment(path: String, offset: Long, length: Int)
  extends WriteAheadLogRecordHandle
