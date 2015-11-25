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

//表示一个应用众多执行者中的一个执行者,表示该执行者的状态
private[deploy] object ExecutorState extends Enumeration {

  //枚举从下标0开始计算
  val LAUNCHING,//启动中 
  LOADING,//装载中 
  RUNNING,//运行中 
  KILLED,//已经杀死 
  FAILED,//已经失败
  LOST, //丢失,比如worker被删除了,因此该worker上执行的所有进程都要被丢掉
  EXITED = Value //退出

  type ExecutorState = Value

  //true表示参数是完成状态
  def isFinished(state: ExecutorState): Boolean = Seq(KILLED, FAILED, LOST, EXITED).contains(state)
}
