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

package org.apache.spark.scheduler

import org.apache.spark.annotation.DeveloperApi

//该task要在哪些地方执行
@DeveloperApi
object TaskLocality extends Enumeration {
  // Process local is expected to be used ONLY within TaskSetManager for now.
  //executor内执行、node一个节点上执行、没有优先在哪个节点上执行(即没有推荐task在哪个节点或者executor上运行)、rack上执行、任意节点执行
  val PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY = Value

  type TaskLocality = Value

  /**
   *  条件<约束 则返回true
   * @param constraint 约束
   * @param condition 条件
   * @return
   */
  def isAllowed(constraint: TaskLocality, condition: TaskLocality): Boolean = {
    condition <= constraint
  }
}
