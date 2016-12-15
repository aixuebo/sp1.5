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

import java.text.SimpleDateFormat
import java.util.Date

import scala.reflect.ClassTag

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{CombineFileSplit, FileSplit}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.input.WholeTextFileInputFormat
import org.apache.spark._
import org.apache.spark.executor.DataReadMethod
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.rdd.NewHadoopRDD.NewHadoopMapPartitionsWithSplitRDD
import org.apache.spark.util.{SerializableConfiguration, ShutdownHookManager, Utils}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.storage.StorageLevel

//代表一个数据块,用于一个map去跑
private[spark] class NewHadoopPartition(
    rddId: Int,//该数据块属于哪个rdd任务
    val index: Int,//该rdd的第几个数据块
    @transient rawSplit: InputSplit with Writable)//原始数据块内容
  extends Partition {

  val serializableHadoopSplit = new SerializableWritable(rawSplit)
  override def hashCode(): Int = 41 * (41 + rddId) + index
}

/**
 * :: DeveloperApi ::
 * An RDD that provides core functionality for reading data stored in Hadoop (e.g., files in HDFS,
 * sources in HBase, or S3), using the new MapReduce API (`org.apache.hadoop.mapreduce`).
 *
 * Note: Instantiating this class directly is not recommended, please use
 * [[org.apache.spark.SparkContext.newAPIHadoopRDD()]]
 *
 * @param sc The SparkContext to associate the RDD with.
 * @param inputFormatClass Storage format of the data to be read.
 * @param keyClass Class of the key associated with the inputFormatClass.
 * @param valueClass Class of the value associated with the inputFormatClass.
 * @param conf The Hadoop configuration.
 */
@DeveloperApi
class NewHadoopRDD[K, V](
    sc : SparkContext,//上下文对象
    inputFormatClass: Class[_ <: InputFormat[K, V]],//对输入源如何读取数据块以及拆分数据块
    keyClass: Class[K],//输入源的key和value类型
    valueClass: Class[V],
    @transient conf: Configuration)
  extends RDD[(K, V)](sc, Nil) //rdd存储的泛型元素是元组
  with SparkHadoopMapReduceUtil
  with Logging {

  // A Hadoop Configuration can be about 10 KB, which is pretty big, so broadcast it
  private val confBroadcast = sc.broadcast(new SerializableConfiguration(conf))
  // private val serializableConf = new SerializableWritable(conf)

  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  @transient protected val jobId = new JobID(jobTrackerId, id)//时间+RDD的id作为job的id

  private val shouldCloneJobConf = sparkContext.conf.getBoolean("spark.hadoop.cloneConf", false) //是否拷贝一个conf对象

  def getConf: Configuration = {
    val conf: Configuration = confBroadcast.value.value
    if (shouldCloneJobConf) {
      // Hadoop Configuration objects are not thread-safe, which may lead to various problems if
      // one job modifies a configuration while another reads it (SPARK-2546, SPARK-10611).  This
      // problem occurs somewhat rarely because most jobs treat the configuration as though it's
      // immutable.  One solution, implemented here, is to clone the Configuration object.
      // Unfortunately, this clone can be very expensive.  To avoid unexpected performance
      // regressions for workloads and Hadoop versions that do not suffer from these thread-safety
      // issues, this cloning is disabled by default.
      NewHadoopRDD.CONFIGURATION_INSTANTIATION_LOCK.synchronized {
        logDebug("Cloning Hadoop Configuration")
        new Configuration(conf)
      }
    } else {
      conf
    }
  }

  //返回所有的数据块切分后的集合
  override def getPartitions: Array[Partition] = {
    val inputFormat = inputFormatClass.newInstance//实例化inputFormat类
    inputFormat match {
      case configurable: Configurable =>
        configurable.setConf(conf)
      case _ =>
    }
    val jobContext = newJobContext(conf, jobId)
    val rawSplits = inputFormat.getSplits(jobContext).toArray //拆分成多少个数据块
    val result = new Array[Partition](rawSplits.size)//每一个数据块作为一个partition被处理
    for (i <- 0 until rawSplits.size) {
      result(i) = new NewHadoopPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
    }
    result
  }

  //如何读取一个数据块内容,返回数据块内容的迭代器
  override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[(K, V)] = {
    val iter = new Iterator[(K, V)] {
      val split = theSplit.asInstanceOf[NewHadoopPartition]//获取该数据切片内容
      logInfo("Input split: " + split.serializableHadoopSplit)
      val conf = getConf

      val inputMetrics = context.taskMetrics
        .getInputMetricsForReadMethod(DataReadMethod.Hadoop)

      // Find a function that will return the FileSystem bytes read by this thread. Do this before
      // creating RecordReader, because RecordReader's constructor might read some bytes
      val bytesReadCallback = inputMetrics.bytesReadCallback.orElse {
        split.serializableHadoopSplit.value match {
          case _: FileSplit | _: CombineFileSplit =>
            SparkHadoopUtil.get.getFSBytesReadOnThreadCallback()
          case _ => None
        }
      }
      inputMetrics.setBytesReadCallback(bytesReadCallback)

      //创建任务上下文,目的是因为hadoop的reader读取一个数据块需要该参数,该参数在spark中本身没意义
      val attemptId = newTaskAttemptID(jobTrackerId, id, isMap = true, split.index, 0)
      val hadoopAttemptContext = newTaskAttemptContext(conf, attemptId)
      val format = inputFormatClass.newInstance
      format match {
        case configurable: Configurable =>
          configurable.setConf(conf)
        case _ =>
      }

      //读取一个数据块的reader
      private var reader = format.createRecordReader(
        split.serializableHadoopSplit.value, hadoopAttemptContext)
      reader.initialize(split.serializableHadoopSplit.value, hadoopAttemptContext)//初始化该reader

      // Register an on-task-completion callback to close the input stream.
      context.addTaskCompletionListener(context => close())
      var havePair = false //true表示该next数据已经使用完 ,要重新在获取下一个数据了
      var finished = false //true表示已经完成
      var recordsSinceMetricsUpdate = 0

      override def hasNext: Boolean = {
        if (!finished && !havePair) {
          finished = !reader.nextKeyValue //如果读取结束了,则!false为true,即已经完成
          if (finished) {//完成读取该数据块数据
            // Close and release the reader here; close() will also be called when the task
            // completes, but for tasks that read from many files, it helps to release the
            // resources early.
            close()
          }
          havePair = !finished //设置havePair为true,表示有新的next数据了
        }
        !finished
      }

      //读取一行内容,返回key-vlaue信息
      override def next(): (K, V) = {
        if (!hasNext) {//false说明没有数据了,抛异常
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false //说明已经读取完数据了
        if (!finished) {
          inputMetrics.incRecordsRead(1)//增加一条记录
        }
        (reader.getCurrentKey, reader.getCurrentValue) //返回key和value信息
      }

      //关闭输入流
      private def close() {
        if (reader != null) {
          // Close the reader and release it. Note: it's very important that we don't close the
          // reader more than once, since that exposes us to MAPREDUCE-5918 when running against
          // Hadoop 1.x and older Hadoop 2.x releases. That bug can lead to non-deterministic
          // corruption issues when reading compressed input.
          try {
            reader.close()
          } catch {
            case e: Exception =>
              if (!ShutdownHookManager.inShutdown()) {
                logWarning("Exception in RecordReader.close()", e)
              }
          } finally {
            reader = null
          }
          if (bytesReadCallback.isDefined) {
            inputMetrics.updateBytesRead()
          } else if (split.serializableHadoopSplit.value.isInstanceOf[FileSplit] ||
                     split.serializableHadoopSplit.value.isInstanceOf[CombineFileSplit]) {
            // If we can't get the bytes read from the FS stats, fall back to the split size,
            // which may be inaccurate.
            try {
              inputMetrics.incBytesRead(split.serializableHadoopSplit.value.getLength)
            } catch {
              case e: java.io.IOException =>
                logWarning("Unable to get input size to set InputMetrics for task", e)
            }
          }
        }
      }
    }
    new InterruptibleIterator(context, iter)
  }

  /** Maps over a partition, providing the InputSplit that was used as the base of the partition. */
  @DeveloperApi
  def mapPartitionsWithInputSplit[U: ClassTag](
      f: (InputSplit, Iterator[(K, V)]) => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U] = {
    new NewHadoopMapPartitionsWithSplitRDD(this, f, preservesPartitioning)
  }

  override def getPreferredLocations(hsplit: Partition): Seq[String] = {
    val split = hsplit.asInstanceOf[NewHadoopPartition].serializableHadoopSplit.value
    val locs = HadoopRDD.SPLIT_INFO_REFLECTIONS match {
      case Some(c) =>
        try {
          val infos = c.newGetLocationInfo.invoke(split).asInstanceOf[Array[AnyRef]]
          Some(HadoopRDD.convertSplitLocationInfo(infos))
        } catch {
          case e : Exception =>
            logDebug("Failed to use InputSplit#getLocationInfo.", e)
            None
        }
      case None => None
    }
    locs.getOrElse(split.getLocations.filter(_ != "localhost"))
  }

  override def persist(storageLevel: StorageLevel): this.type = {
    if (storageLevel.deserialized) {
      logWarning("Caching NewHadoopRDDs as deserialized objects usually leads to undesired" +
        " behavior because Hadoop's RecordReader reuses the same Writable object for all records." +
        " Use a map transformation to make copies of the records.")
    }
    super.persist(storageLevel)
  }

}

private[spark] object NewHadoopRDD {
  /**
   * Configuration's constructor is not threadsafe (see SPARK-1097 and HADOOP-10456).
   * Therefore, we synchronize on this lock before calling new Configuration().
   */
  val CONFIGURATION_INSTANTIATION_LOCK = new Object()

  /**
   * Analogous to [[org.apache.spark.rdd.MapPartitionsRDD]], but passes in an InputSplit to
   * the given function rather than the index of the partition.
   */
  private[spark] class NewHadoopMapPartitionsWithSplitRDD[U: ClassTag, T: ClassTag](
      prev: RDD[T],
      f: (InputSplit, Iterator[T]) => Iterator[U],
      preservesPartitioning: Boolean = false)
    extends RDD[U](prev) {

    override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

    override def getPartitions: Array[Partition] = firstParent[T].partitions

    override def compute(split: Partition, context: TaskContext): Iterator[U] = {
      val partition = split.asInstanceOf[NewHadoopPartition]
      val inputSplit = partition.serializableHadoopSplit.value
      f(inputSplit, firstParent[T].iterator(split, context))
    }
  }
}

//一次读取一整个文件内容
private[spark] class WholeTextFileRDD(
    sc : SparkContext,
    inputFormatClass: Class[_ <: WholeTextFileInputFormat],
    keyClass: Class[String],//key是文件的path
    valueClass: Class[String],//value是文件的全部内容
    @transient conf: Configuration,
    minPartitions: Int)
  extends NewHadoopRDD[String, String](sc, inputFormatClass, keyClass, valueClass, conf) {

  //返回分区集合
  override def getPartitions: Array[Partition] = {
    val inputFormat = inputFormatClass.newInstance
    val conf = getConf
    inputFormat match {
      case configurable: Configurable =>
        configurable.setConf(conf)
      case _ =>
    }
    val jobContext = newJobContext(conf, jobId)
    inputFormat.setMinPartitions(jobContext, minPartitions)
    val rawSplits = inputFormat.getSplits(jobContext).toArray //返回数据块分区数量
    val result = new Array[Partition](rawSplits.size)
    for (i <- 0 until rawSplits.size) {
      result(i) = new NewHadoopPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
    }
    result
  }
}

