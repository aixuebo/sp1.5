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

package org.apache.spark.deploy.master

private[deploy] object DriverState extends Enumeration {

  type DriverState = Value

  // SUBMITTED: Submitted but not yet scheduled on a worker 已经接收该driver的提交,但是还没有调度该driver到一个worker上
  // RUNNING: Has been allocated to a worker to run 表示已经将driver分配到一个worker上去运行了
  // FINISHED: Previously ran and exited cleanly
  // RELAUNCHING: Exited non-zero or due to worker failure, but has not yet started running again
  // UNKNOWN: The state of the driver is temporarily not known due to master failure recovery
  // KILLED: A user manually killed this driver
  // FAILED: The driver exited non-zero and was not supervised
  // ERROR: Unable to run or restart due to an unrecoverable error (e.g. missing jar file)
  val SUBMITTED, //master受收到客户端submitDriver后,创建一个DriverInfo对象,就是该状态
  RUNNING, //在master的schedule方法里面,当一个worker的内存和cpu满足一个driver,则在该worker上启动该driver
  FINISHED, 
  RELAUNCHING, 
  UNKNOWN,
  KILLED, 
  FAILED,
  ERROR = Value
}
