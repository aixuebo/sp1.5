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

import java.nio.ByteBuffer

import org.apache.spark.util.SerializableBuffer

/**
 * Description of a task that gets passed onto executors to be executed, usually created by
 * [[TaskSetManager.resourceOffer]].
 */
private[spark] class TaskDescription(
    val taskId: Long,//该任务的ID
    val attemptNumber: Int,//该任务的尝试次数
    val executorId: String,//该任务要分配到哪个executor上去执行
    val name: String,
    val index: Int,    // Index within this task's TaskSet 该任务是TaskSet的第几个任务
    _serializedTask: ByteBuffer) //该任务的序列化信息--即task具体执行的代码
  extends Serializable {

  // Because ByteBuffers are not serializable, wrap the task in a SerializableBuffer
  private val buffer = new SerializableBuffer(_serializedTask)

  def serializedTask: ByteBuffer = buffer.value

  override def toString: String = "TaskDescription(TID=%d, index=%d)".format(taskId, index)
}
