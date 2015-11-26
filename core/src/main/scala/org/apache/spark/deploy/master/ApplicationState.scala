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

//该对象表示一个应用,一个driver可以产生多个应用,但是一个应用会产生多个执行者执行任务
private[master] object ApplicationState extends Enumeration {

  type ApplicationState = Value

  val WAITING, 
  RUNNING, //app正在运行,当app中有一个执行者在worker上被分配了,则表示运行中
  FINISHED, 
  FAILED, 
  KILLED, 
  UNKNOWN = Value //未知

  val MAX_NUM_RETRY = 10
}
