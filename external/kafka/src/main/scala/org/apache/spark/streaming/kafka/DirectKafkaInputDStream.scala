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

package org.apache.spark.streaming.kafka

import scala.annotation.tailrec
import scala.collection.mutable
import scala.reflect.ClassTag

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder

import org.apache.spark.{Logging, SparkException}
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.streaming.scheduler.{RateController, StreamInputInfo}
import org.apache.spark.streaming.scheduler.rate.RateEstimator

/**
 *  A stream of {@link org.apache.spark.streaming.kafka.KafkaRDD} where
 * each given Kafka topic/partition corresponds to an RDD partition.
 * 返回一个spark的RDD,读取partition的startoffset到目前最大的offset之间的数据
 * The spark configuration spark.streaming.kafka.maxRatePerPartition gives the maximum number
 *  of messages
 *  spark的配置spark.streaming.kafka.maxRatePerPartition控制着流量
 *
 * per second that each '''partition''' will accept. 每一秒 每一个partition都会被接收一次
 *
 * Starting offsets are specified in advance,在高级语法中开始位置可以指定
 * and this DStream is not responsible for committing offsets,
 * so that you can control exactly-once semantics.
 * For an easy interface to Kafka-managed offsets,
 *  see {@link org.apache.spark.streaming.kafka.KafkaCluster}
 * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
 * configuration parameters</a>.
 *   Requires "metadata.broker.list" or "bootstrap.servers" to be set with Kafka broker(s),
 *   NOT zookeeper servers, specified in host1:port1,host2:port2 form.
 * @param fromOffsets per-topic/partition Kafka offsets defining the (inclusive)
 *  starting point of the stream
 * @param messageHandler function for translating each message into the desired type
 *
 * 创建一个stream,直接从kafka节点上拉去信息,不需要任何receiver,
 *
 * DirectKafkaInputDStream类是读取kafka的数据,每一个RDD去kafka指定offset位置读取数据内容,定期执行上一次offset位置和最新位置之间的差就是每一个RDD的partition去读取数据的内容,因此该类需要操作kafka的好多底层API,因此该类使用KafkaRDD处理数据
 * 而ReliableKafkaReceiver 这种是创建一个流,时刻接收kafaka的数据,然后存储到HDFS上,定期从HDFS上给spark streaming数据,不使用KafkaRDD处理数据
 */
private[streaming]
class DirectKafkaInputDStream[
  K: ClassTag,
  V: ClassTag,
  U <: Decoder[K]: ClassTag,
  T <: Decoder[V]: ClassTag,
  R: ClassTag](
    @transient ssc_ : StreamingContext,
    val kafkaParams: Map[String, String],//kafka的环境
    val fromOffsets: Map[TopicAndPartition, Long],//抓去kafka每一个partition的开始位置
    messageHandler: MessageAndMetadata[K, V] => R //messageHandler函数,传入参数是kafka返回的一个对象,包含key-value topic-partition 以及该信息的offset序号,将该参数转换成R对象
  ) extends InputDStream[R](ssc_) with Logging {
  val maxRetries = context.sparkContext.getConf.getInt(
    "spark.streaming.kafka.maxRetries", 1)//最大尝试次数

  // Keep this consistent with how other streams are named (e.g. "Flume polling stream [2]")
  private[streaming] override def name: String = s"Kafka direct stream [$id]"

  //如何存储描述信息----防止出问题的时候可以回滚
  protected[streaming] override val checkpointData =
    new DirectKafkaInputDStreamCheckpointData


  /**
   * Asynchronously maintains & sends new rate limits to the receiver through the receiver tracker.
   * 流量控制器
   */
  override protected[streaming] val rateController: Option[RateController] = {
    if (RateController.isBackPressureEnabled(ssc.conf)) {
      Some(new DirectKafkaRateController(id,
        RateEstimator.create(ssc.conf, ssc_.graph.batchDuration)))
    } else {
      None
    }
  }

  protected val kc = new KafkaCluster(kafkaParams) //创建连接集群的对象,因为该类需要详细的操纵kafka的数据

  private val maxRateLimitPerPartition: Int = context.sparkContext.getConf.getInt(
      "spark.streaming.kafka.maxRatePerPartition", 0)//表示每一个topic-partition允许的流量上限

  //返回此时最大允许的流量--注意此时是每一个partition的流量值
  protected def maxMessagesPerPartition: Option[Long] = {
    val estimatedRateLimit = rateController.map(_.getLatestRate().toInt) //此时全部的流量限制
    val numPartitions = currentOffsets.keys.size //多少个topic-partition

    //获取每一个partition最终要抓去多少个offset回来
    val effectiveRateLimitPerPartition = estimatedRateLimit
      .filter(_ > 0)
      .map { limit =>
        if (maxRateLimitPerPartition > 0) {
          Math.min(maxRateLimitPerPartition, (limit / numPartitions)) //(limit / numPartitions) 表示此时全部流量/多少个partition,即每一个partition允许多少个offset
        } else {
          limit / numPartitions
        }
      }.getOrElse(maxRateLimitPerPartition)

    if (effectiveRateLimitPerPartition > 0) {
      val secsPerBatch = context.graph.batchDuration.milliseconds.toDouble / 1000
      Some((secsPerBatch * effectiveRateLimitPerPartition).toLong) //设置此时该秒内的流量,即每一个partition在这一秒的流量
    } else {
      None
    }
  }

  //当前每一个topic-partition已经读取到哪个位置了
  protected var currentOffsets = fromOffsets //Map[TopicAndPartition, Long],//抓去kafka每一个partition的开始位置

  /**
   * 获取leader节点最新的offset序号
   * @param retries 最大尝试次数
   * @return
   */
  @tailrec
  protected final def latestLeaderOffsets(retries: Int): Map[TopicAndPartition, LeaderOffset] = {
    val o = kc.getLatestLeaderOffsets(currentOffsets.keySet)
    // Either.fold would confuse @tailrec, do it manually
    if (o.isLeft) {//获取异常信息
      val err = o.left.get.toString
      if (retries <= 0) {
        throw new SparkException(err)
      } else {
        log.error(err)
        Thread.sleep(kc.config.refreshLeaderBackoffMs)
        latestLeaderOffsets(retries - 1) //继续尝试
      }
    } else {
      o.right.get
    }
  }

  // limits the maximum number of messages per partition
  //参数是leader节点最新的offset序号
  //设置上限--因为需要流量控制,有时候不一定能抓去到最新的offset位置
  protected def clamp(
    leaderOffsets: Map[TopicAndPartition, LeaderOffset]): Map[TopicAndPartition, LeaderOffset] = {
    maxMessagesPerPartition.map { mmp => //mmp是流量控制值
      leaderOffsets.map { case (tp, lo) =>
        tp -> lo.copy(offset = Math.min(currentOffsets(tp) + mmp, lo.offset)) //设置每一个partition的最终能读取到哪个offset位置
      }
    }.getOrElse(leaderOffsets) //默认是每一个partition更新到最新的offset位置
  }

  override def compute(validTime: Time): Option[KafkaRDD[K, V, U, T, R]] = {
    val untilOffsets = clamp(latestLeaderOffsets(maxRetries)) //设置上限---最多读取到哪个位置
    val rdd = KafkaRDD[K, V, U, T, R](
      context.sparkContext, kafkaParams, currentOffsets, untilOffsets, messageHandler)

    //看一下每一个topic-partition对应的抓去区间
    // Report the record number and metadata of this batch interval to InputInfoTracker.
    val offsetRanges = currentOffsets.map { case (tp, fo) =>
      val uo = untilOffsets(tp) //目前该topic-partition对应的最大offset位置
      OffsetRange(tp.topic, tp.partition, fo, uo.offset)//因此可以组装成一个区间
    }

    //描述信息
    val description = offsetRanges.filter { offsetRange =>
      // Don't display empty ranges.
      offsetRange.fromOffset != offsetRange.untilOffset //说明这个topic-partition是有数据去抓去的
    }.map { offsetRange =>
      s"topic: ${offsetRange.topic}\tpartition: ${offsetRange.partition}\t" +
        s"offsets: ${offsetRange.fromOffset} to ${offsetRange.untilOffset}"
    }.mkString("\n")


    // Copy offsetRanges to immutable.List to prevent from being modified by the user
    //要抓去的kafka每一个topic-partition哪些offset数据
    val metadata = Map(
      "offsets" -> offsetRanges.toList,
      StreamInputInfo.METADATA_KEY_DESCRIPTION -> description) //描述信息

    val inputInfo = StreamInputInfo(id, rdd.count, metadata) //组装成一个描述信息
    ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)

    currentOffsets = untilOffsets.map(kv => kv._1 -> kv._2.offset) //更新开始位置
    Some(rdd) //返回组装后的rdd
  }

  override def start(): Unit = {
  }

  def stop(): Unit = {
  }

  private[streaming]
  class DirectKafkaInputDStreamCheckpointData extends DStreamCheckpointData(this) {
    def batchForTime: mutable.HashMap[Time, Array[(String, Int, Long, Long)]] = {//value表示topic-partition-开始offset-util的offset
      data.asInstanceOf[mutable.HashMap[Time, Array[OffsetRange.OffsetRangeTuple]]]
    }

    override def update(time: Time) {
      batchForTime.clear()
      generatedRDDs.foreach { kv =>
        val a = kv._2.asInstanceOf[KafkaRDD[K, V, U, T, R]].offsetRanges.map(_.toTuple).toArray
        batchForTime += kv._1 -> a
      }
    }

    override def cleanup(time: Time) { }

    override def restore() {
      // this is assuming that the topics don't change during execution, which is true currently
      val topics = fromOffsets.keySet
      val leaders = KafkaCluster.checkErrors(kc.findLeaders(topics))

      batchForTime.toSeq.sortBy(_._1)(Time.ordering).foreach { case (t, b) =>
         logInfo(s"Restoring KafkaRDD for time $t ${b.mkString("[", ", ", "]")}")
         generatedRDDs += t -> new KafkaRDD[K, V, U, T, R](
           context.sparkContext, kafkaParams, b.map(OffsetRange(_)), leaders, messageHandler)
      }
    }
  }

  /**
   * A RateController to retrieve the rate from RateEstimator.
   */
  private[streaming] class DirectKafkaRateController(id: Int, estimator: RateEstimator)
    extends RateController(id, estimator) {
    override def publish(rate: Long): Unit = ()
  }
}
