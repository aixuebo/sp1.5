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

import java.io.IOException

import scala.reflect.ClassTag

import org.apache.hadoop.fs.Path

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.util.{SerializableConfiguration, Utils}

/**
 * An RDD that reads from checkpoint files previously written to reliable storage.
 * 从缓存点读取数据,生成一个RDD对象,该缓存点是以前RDD写到文件中的
 * @T表示还原的RDD的泛型对象,例如RDD[Double],表示的就是checkPoint是针对该RDD进行处理的,RDD处理的元素是Double类型的
 */
private[spark] class ReliableCheckpointRDD[T: ClassTag](
    @transient sc: SparkContext,
    val checkpointPath: String) //做数据缓存的目录
  extends CheckpointRDD[T](sc) {

  @transient private val hadoopConf = sc.hadoopConfiguration //hadoop的配置对象
  @transient private val cpath = new Path(checkpointPath)
  @transient private val fs = cpath.getFileSystem(hadoopConf) //根据path,获取hadoop的操作系统对象
  
  private val broadcastedConf = sc.broadcast(new SerializableConfiguration(hadoopConf)) //将hadoop的Configuration信息序列化与反序列化

  // Fail fast if checkpoint directory does not exist,目录必须存在
  require(fs.exists(cpath), s"Checkpoint directory does not exist: $checkpointPath")

  /**
   * Return the path of the checkpoint directory this RDD reads data from.
   * 返回checkpoint目录
   */
  override def getCheckpointFile: Option[String] = Some(checkpointPath)

  /**
   * Return partitions described by the files in the checkpoint directory.
   *
   * Since the original RDD may belong to a prior application, there is no way to know a
   * priori the number of partitions to expect. This method assumes that the original set of
   * checkpoint files are fully preserved in a reliable storage across application lifespans.
   * 读取目录,返回每一个partition文件对应的CheckpointRDDPartition对象集合,该CheckpointRDDPartition元素会表示一个partition分区,即包含partition的ID
   */
  protected override def getPartitions: Array[Partition] = {
    // listStatus can throw exception if path does not exist.
    //获取目录下所有的part-开头的文件,就是校验点缓存下来的文件,每一个part-开头的文件对应的是原始RDD对应的一个个partition对象
    val inputFiles = fs.listStatus(cpath)
      .map(_.getPath)
      .filter(_.getName.startsWith("part-"))
      .sortBy(_.toString) //并且按照partition顺序排序
      
    // Fail fast if input files are invalid 校验RDD的partition文件必须是part-00001格式
    inputFiles.zipWithIndex.foreach { case (path, i) =>
      if (!path.toString.endsWith(ReliableCheckpointRDD.checkpointFileName(i))) {
        throw new SparkException(s"Invalid checkpoint file: $path")
      }
    }
    
    //每一个partiton对应一个CheckpointRDDPartition对象
    Array.tabulate(inputFiles.length)(i => new CheckpointRDDPartition(i))
  }

  /**
   * Return the locations of the checkpoint file associated with the given partition.
   * 返回该partition所属文件的路径集合,即从该集合内便可以访问到该partition的内容
   */
  protected override def getPreferredLocations(split: Partition): Seq[String] = {
    val status = fs.getFileStatus(
      new Path(checkpointPath, ReliableCheckpointRDD.checkpointFileName(split.index))) //获取该partiton对应的文件对象
    val locations = fs.getFileBlockLocations(status, 0, status.getLen) //获取该文件对应的所有路径集合
    locations.headOption.toList.flatMap(_.getHosts).filter(_ != "localhost")//过滤不是localhost的集合
  }

  /**
   * Read the content of the checkpoint file associated with the given partition.
   * 读取第几个partition文件的内容.
   * 返回值是该partition的一行一行的数据
   */
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val file = new Path(checkpointPath, ReliableCheckpointRDD.checkpointFileName(split.index))
    ReliableCheckpointRDD.readCheckpointFile(file, broadcastedConf, context)
  }

}

//可靠的RDD检查点
private[spark] object ReliableCheckpointRDD extends Logging {

  /**
   * Return the checkpoint file name for the given partition.
   * 返回给定partition序号对应的文件名,文件名格式为part-00001形式
   */
  private def checkpointFileName(partitionIndex: Int): String = {
    "part-%05d".format(partitionIndex)
  }

  /**
   * Write this partition's values to a checkpoint file.
   * @blockSize 向hadoop内写入数据时,每一个数据块的大小
   * 将partition的每一个记录写入path/partitionId对应的文件中
   * 
   * @ctx 该任务的上下文对象
   * @iterator 该任务所属的partition每一行信息读取迭代器
   */
  def writeCheckpointFile[T: ClassTag](
      path: String,//输出目录
      broadcastedConf: Broadcast[SerializableConfiguration],//hadoop的配置文件对象
      blockSize: Int = -1)(ctx: TaskContext, iterator: Iterator[T]) {
    val env = SparkEnv.get
    val outputDir = new Path(path)
    val fs = outputDir.getFileSystem(broadcastedConf.value.value) //根据path和hadoop环境对象,返回操作系统对象

    val finalOutputName = ReliableCheckpointRDD.checkpointFileName(ctx.partitionId()) //根据partitionId生成最终的文件名,例如part-00001形式
    val finalOutputPath = new Path(outputDir, finalOutputName) //具体输出的全路径
    val tempOutputPath =
      new Path(outputDir, s".$finalOutputName-attempt-${ctx.attemptNumber()}") //path/part-00001-attempt-尝试次数 组成临时输出目录

    if (fs.exists(tempOutputPath)) {
      throw new IOException(s"Checkpoint failed: temporary path $tempOutputPath already exists")
    }
    val bufferSize = env.conf.getInt("spark.buffer.size", 65536)

    val fileOutputStream = if (blockSize < 0) { //创建输出流
      fs.create(tempOutputPath, false, bufferSize)
    } else {
      // This is mainly for testing purpose 后两个参数,文件的默认备份数量,以及数据块的大小
      fs.create(tempOutputPath, false, bufferSize, fs.getDefaultReplication, blockSize)
    }
    
    //将迭代器传入序列化工具里,将其内容一条一条的写入到输出流中
    val serializer = env.serializer.newInstance()
    val serializeStream = serializer.serializeStream(fileOutputStream)
    Utils.tryWithSafeFinally {
      serializeStream.writeAll(iterator)
    } {
      serializeStream.close()
    }

    if (!fs.rename(tempOutputPath, finalOutputPath)) { //将临时目录切换成正式目录
      if (!fs.exists(finalOutputPath)) {
        logInfo(s"Deleting tempOutputPath $tempOutputPath")
        fs.delete(tempOutputPath, false)
        throw new IOException("Checkpoint failed: failed to save output of task: " +
          s"${ctx.attemptNumber()} and final output path does not exist: $finalOutputPath")
      } else {
        // Some other copy of this task must've finished before us and renamed it
        logInfo(s"Final output path $finalOutputPath already exists; not overwriting it")
        fs.delete(tempOutputPath, false)
      }
    }
  }

  /**
   * Read the content of the specified checkpoint file.
   * 从partition文件中还原RDD的某一个partition数据
   * @context 任务的上下文对象
   * 
   * 返回该partition中每一个记录的迭代对象
   */
  def readCheckpointFile[T](
      path: Path,//读取文件的全路径
      broadcastedConf: Broadcast[SerializableConfiguration],//hadoop的配置文件信息
      context: TaskContext): Iterator[T] = {//返回该partition中每一个记录的迭代对象
    val env = SparkEnv.get
    val fs = path.getFileSystem(broadcastedConf.value.value)
    val bufferSize = env.conf.getInt("spark.buffer.size", 65536)
    val fileInputStream = fs.open(path, bufferSize) //读取文件
    
    //使用反序列化工具处理原始输入流
    val serializer = env.serializer.newInstance()
    val deserializeStream = serializer.deserializeStream(fileInputStream)

    // Register an on-task-completion callback to close the input stream.
    //为任务上下文添加一个监听,当该task完成的时候调用该监听,关闭输入源
    context.addTaskCompletionListener(context => deserializeStream.close())

    deserializeStream.asIterator.asInstanceOf[Iterator[T]]
  }

}
