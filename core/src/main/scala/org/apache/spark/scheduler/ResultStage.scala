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

import org.apache.spark.rdd.RDD
import org.apache.spark.util.CallSite

/**
 * The ResultStage represents the final stage in a job.
 * 表示一个job最终产生的Stage,该阶段会包含各种子阶段
 */
private[spark] class ResultStage(
    id: Int,//该阶段的ID
    rdd: RDD[_],//该阶段依赖的RDD输入源
    numTasks: Int,//rdd的partition数量
    parents: List[Stage],//该阶段依赖哪些父Stage集合
    firstJobId: Int,//该阶段属于哪个jobid
    callSite: CallSite)
  extends Stage(id, rdd, numTasks, parents, firstJobId, callSite) {

  // The active job for this result stage. Will be empty if the job has already finished
  // (e.g., because the job was cancelled).
  //该job运行中的内存对象
  var resultOfJob: Option[ActiveJob] = None

  override def toString: String = "ResultStage " + id
}
