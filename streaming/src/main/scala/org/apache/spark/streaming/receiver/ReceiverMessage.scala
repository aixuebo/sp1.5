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

package org.apache.spark.streaming.receiver

import org.apache.spark.streaming.Time

/** Messages sent to the Receiver.
  * 发送到Receiver的信息
  **/
private[streaming] sealed trait ReceiverMessage extends Serializable

private[streaming] object StopReceiver extends ReceiverMessage //停止Receiver

private[streaming] case class CleanupOldBlocks(threshTime: Time) extends ReceiverMessage //清理Receiver的老一些的数据块信息

private[streaming] case class UpdateRateLimit(elementsPerSecond: Long)
                   extends ReceiverMessage //更新Receiver的带宽速度
