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

import org.apache.spark.Partition

/**
 * Enumeration to manage state transitions of an RDD through checkpointing
 * [ Initialized --> checkpointing in progress --> checkpointed ].
 */
private[spark] object CheckpointState extends Enumeration {
  type CheckpointState = Value
  val Initialized, //进行初始化checkPoint操作
  CheckpointingInProgress,//正在进行RDD的 checkPoint操作
  Checkpointed = Value //该RDD已经完成checkPoint操作了
}

/**
 * This class contains all the information related to RDD checkpointing. Each instance of this
 * class is associated with a RDD. It manages process of checkpointing of the associated RDD,
 * as well as, manages the post-checkpoint state by providing the updated partitions,
 * iterator and preferred locations of the checkpointed RDD.
 * 对RDD创建一个新的可支持checkPoint的RDD对象
 */
private[spark] abstract class RDDCheckpointData[T: ClassTag](@transient rdd: RDD[T])
  extends Serializable {

  import CheckpointState._

  // The checkpoint state of the associated RDD.
  protected var cpState = Initialized //该RDD的checkPoint状态

  // The RDD that contains our checkpointed data 获取该RDD的checkPoint对象
  private var cpRDD: Option[CheckpointRDD[T]] = None

  // TODO: are we sure we need to use a global lock in the following methods?

  /**
   * Return whether the checkpoint data for this RDD is already persisted.
   * true表示该RDD已经完成的checkPoint操作
   */
  def isCheckpointed: Boolean = RDDCheckpointData.synchronized {
    cpState == Checkpointed
  }

  /**
   * Materialize this RDD and persist its content.
   * This is called immediately after the first action invoked on this RDD has completed.
   */
  final def checkpoint(): Unit = {
    // Guard against multiple threads checkpointing the same RDD by
    // atomically flipping the state of this RDDCheckpointData
    RDDCheckpointData.synchronized {
      if (cpState == Initialized) {
        cpState = CheckpointingInProgress
      } else {
        return
      }
    }

    //真正去做工作
    val newRDD = doCheckpoint()

    // Update our state and truncate the RDD lineage
    RDDCheckpointData.synchronized {
      cpRDD = Some(newRDD)
      cpState = Checkpointed //说明该进度已经结束
      rdd.markCheckpointed()
    }
  }

  /**
   * Materialize this RDD and persist its content.
   *
   * Subclasses should override this method to define custom checkpointing behavior.
   * @return the checkpoint RDD created in the process.
   * 将RDD进程checkPoint操作,产生新的RDD对象
   */
  protected def doCheckpoint(): CheckpointRDD[T]

  /**
   * Return the RDD that contains our checkpointed data.
   * This is only defined if the checkpoint state is `Checkpointed`.
   * 获取该RDD的checkPoint对象
   */
  def checkpointRDD: Option[CheckpointRDD[T]] = RDDCheckpointData.synchronized { cpRDD }

  /**
   * Return the partitions of the resulting checkpoint RDD.
   * For tests only.
   * 返回checkpoint的RDD对应的所有partition集合信息
   */
  def getPartitions: Array[Partition] = RDDCheckpointData.synchronized {
    cpRDD.map(_.partitions).getOrElse { Array.empty }
  }

}

/**
 * Global lock for synchronizing checkpoint operations.
 * 同步checkPoint锁对象
 */
private[spark] object RDDCheckpointData
