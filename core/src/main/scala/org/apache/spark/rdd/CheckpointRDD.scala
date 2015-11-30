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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark.{Partition, SparkContext, TaskContext}

/**
 * An RDD partition used to recover checkpointed data.
 * 代表一个可以支持RDD存储/还原的partition对象
 */
private[spark] class CheckpointRDDPartition(val index: Int) extends Partition

/**
 * An RDD that recovers checkpointed data from storage.
 * T表示还原的RDD的泛型对象,例如RDD[Double],表示的就是checkPoint是针对该RDD进行处理的,RDD处理的元素是Double类型的
 * 
 * 该类表示RDD支持checkPoint功能,即可以将RDD根据partition的内容存储到hdfs等地方,并且可以支持从hdfs等地方还原RDD信息
 * 
 * 参见:ReliableCheckpointRDD实现类
 */
private[spark] abstract class CheckpointRDD[T: ClassTag](@transient sc: SparkContext)
  extends RDD[T](sc, Nil) {//Nil表示该RDD没有父RDD做依赖

  // CheckpointRDD should not be checkpointed again
  override def doCheckpoint(): Unit = { }
  override def checkpoint(): Unit = { }
  override def localCheckpoint(): this.type = this

  // Note: There is a bug in MiMa that complains about `AbstractMethodProblem`s in the
  // base [[org.apache.spark.rdd.RDD]] class if we do not override the following methods.
  // scalastyle:off
  //读取目录,返回每一个partition文件对应的CheckpointRDDPartition对象集合,该CheckpointRDDPartition元素会表示一个partition分区,即包含partition的ID
  protected override def getPartitions: Array[Partition] = ???
  //读取第几个partition文件的内容.
  //返回值是该partition的一行一行的数据
  override def compute(p: Partition, tc: TaskContext): Iterator[T] = ???
  // scalastyle:on

}
