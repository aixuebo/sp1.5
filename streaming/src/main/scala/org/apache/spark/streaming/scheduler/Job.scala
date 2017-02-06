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

package org.apache.spark.streaming.scheduler

import org.apache.spark.streaming.Time
import scala.util.Try

/**
 * Class representing a Spark computation. It may contain multiple Spark jobs.
 * driver需要从多个streaming中获取数据源,因此对应多个streaming,每一个streaming的一次任务就是一个job,
 * 因此该对象表示针对一个streaming的job
 *
 * 该对象与JobSet进行对比.由Job对象组成的JobSet集合
 */
private[streaming]
class Job(val time: Time, func: () => _) {//什么时间点产生的job,该job要做的任务是什么,func表示任务是不接收任何参数,无返回值的函数
  private var _id: String = _ //该job的唯一ID,组成streaming job $time.$outputOpId
  private var _outputOpId: Int = _ //该对象表示在一组JobSet中,该job的序号
  private var isSet = false //true表示已经设置了_outputOpId
  private var _result: Try[_] = null //该job运行后的结果

  def run() {//运行该job对应的func函数
    _result = Try(func())
  }

  //获取运行后的结果
  def result: Try[_] = {
    if (_result == null) {
      throw new IllegalStateException("Cannot access result before job finishes")
    }
    _result
  }

  /**
   * @return the global unique id of this Job.
   */
  def id: String = {
    if (!isSet) {
      throw new IllegalStateException("Cannot access id before calling setId")
    }
    _id
  }

  /**
   * @return the output op id of this Job. Each Job has a unique output op id in the same JobSet.在一个相同的jobSet中,存在一个唯一的jobid
   */
  def outputOpId: Int = {
    if (!isSet) {
      throw new IllegalStateException("Cannot access number before calling setId")
    }
    _outputOpId
  }

  def setOutputOpId(outputOpId: Int) {
    if (isSet) {
      throw new IllegalStateException("Cannot call setOutputOpId more than once")
    }
    isSet = true
    _id = s"streaming job $time.$outputOpId"
    _outputOpId = outputOpId
  }

  override def toString: String = id
}
