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

package org.apache.spark.shuffle

import org.apache.spark.{ShuffleDependency, Aggregator, Partitioner}
import org.apache.spark.serializer.Serializer

/**
 * A basic ShuffleHandle implementation that just captures registerShuffle's parameters.
 */
private[spark] class BaseShuffleHandle[K, V, C](
    shuffleId: Int,//shuffle的唯一标示ID
    val numMaps: Int,//该RDD数据源有多少个partition,即多少个map任务去执行
    val dependency: ShuffleDependency[K, V, C]) //最终需要被reduce的信息内容
  extends ShuffleHandle(shuffleId)
