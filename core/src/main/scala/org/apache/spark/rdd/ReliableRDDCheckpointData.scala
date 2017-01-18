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

import org.apache.hadoop.fs.Path

import org.apache.spark._
import org.apache.spark.util.SerializableConfiguration

/**
 * An implementation of checkpointing that writes the RDD data to reliable storage.
 * This allows drivers to be restarted on failure with previously computed state.
 * 该类是针对RDD进行抽象处理的,相当于RDD的checkPoint的管理类
 */
private[spark] class ReliableRDDCheckpointData[T: ClassTag](@transient rdd: RDD[T])
  extends RDDCheckpointData[T](rdd) with Logging {

  // The directory to which the associated RDD has been checkpointed to
  // This is assumed to be a non-local path that points to some reliable storage
  //返回该RDD写入到哪个目录下,返回被写入的目录,格式checkpointDir/rdd-$rddId
  private val cpDir: String =
    ReliableRDDCheckpointData.checkpointPath(rdd.context, rdd.id)
      .map(_.toString)
      .getOrElse { throw new SparkException("Checkpoint dir must be specified.") }

  /**
   * Return the directory to which this RDD was checkpointed.
   * If the RDD is not checkpointed yet, return None.
   * 返回该RDD进行checkPoint的目录,如果该RDD还没有进行checkpointed,则返回null
   */
  def getCheckpointDir: Option[String] = RDDCheckpointData.synchronized {
    if (isCheckpointed) { //如果已经完成了checkPoint,则获取该RDD的目录
      Some(cpDir.toString)
    } else {
      None
    }
  }

  /**
   * Materialize this RDD and write its content to a reliable DFS.
   * This is called immediately after the first action invoked on this RDD has completed.
   * 真正去做checkPoint操作,即将该rdd的结果集存储到HDFS上即可
   */
  protected override def doCheckpoint(): CheckpointRDD[T] = {
    // Create the output path for the checkpoint 创建RDD的输出目录
    val path = new Path(cpDir)
    val fs = path.getFileSystem(rdd.context.hadoopConfiguration)
    if (!fs.mkdirs(path)) {// 创建目录
      throw new SparkException(s"Failed to create checkpoint path $cpDir")
    }

    // Save to file, and reload it as an RDD 加载RDD对应的hadoop配置对象
    val broadcastedConf = rdd.context.broadcast(
      new SerializableConfiguration(rdd.context.hadoopConfiguration))
    // TODO: This is expensive because it computes the RDD again unnecessarily (SPARK-8582) 这是一个非常耗时的操作
    rdd.context.runJob(rdd, ReliableCheckpointRDD.writeCheckpointFile[T](cpDir, broadcastedConf) _) //将每一个partition的信息写入到hdfs上的输出目录中,每一个partition在目录中创建一个文件输出流part-00001,向该文件输出流中写入partition内容
    
    val newRDD = new ReliableCheckpointRDD[T](rdd.context, cpDir) //将hdfs上的内容创建成一个新的RDD,说明每一次的结果其实都存储到HDFS上了,下次直接获取到记录即可,不需要计算中间过程了
    if (newRDD.partitions.length != rdd.partitions.length) {//校验partition数量一定是一对一的
      throw new SparkException(
        s"Checkpoint RDD $newRDD(${newRDD.partitions.length}) has different " +
          s"number of partitions from original RDD $rdd(${rdd.partitions.length})")
    }

    // Optionally clean our checkpoint files if the reference is out of scope
    if (rdd.conf.getBoolean("spark.cleaner.referenceTracking.cleanCheckpoints", false)) {
      rdd.context.cleaner.foreach { cleaner =>
        cleaner.registerRDDCheckpointDataForCleanup(newRDD, rdd.id)
      }
    }

    logInfo(s"Done checkpointing RDD ${rdd.id} to $cpDir, new parent is RDD ${newRDD.id}") //打印日志,说明已经完成了checkpointing,原来的rddid是什么,结果集导入到什么hdfs的目录下了,新产生的rddid是什么

    newRDD //返回新的RDDid
  }

}

private[spark] object ReliableRDDCheckpointData {

  /** Return the path of the directory to which this RDD's checkpoint data is written. 
   *  返回该RDD写入到哪个目录下
   *  返回被写入的目录,格式checkpointDir/rdd-$rddId
   **/
  def checkpointPath(sc: SparkContext, rddId: Int): Option[Path] = {
    sc.checkpointDir.map { dir => new Path(dir, s"rdd-$rddId") }
  }

  /** Clean up the files associated with the checkpoint data for this RDD.
   * 情况该RDD所有的checkPoint文件  
   **/
  def cleanCheckpoint(sc: SparkContext, rddId: Int): Unit = {
    //找到RDD下所有的文件,依次进行删除
    checkpointPath(sc, rddId).foreach { path =>
      val fs = path.getFileSystem(sc.hadoopConfiguration)
      if (fs.exists(path)) {
        fs.delete(path, true)
      }
    }
  }
}
