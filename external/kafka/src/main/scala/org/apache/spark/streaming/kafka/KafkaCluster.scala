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

import scala.util.control.NonFatal
import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import java.util.Properties
import kafka.api._
import kafka.common.{ErrorMapping, OffsetAndMetadata, OffsetMetadataAndError, TopicAndPartition}
import kafka.consumer.{ConsumerConfig, SimpleConsumer}
import org.apache.spark.SparkException

/**
 * Convenience methods for interacting with a Kafka cluster.
 * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
 * configuration parameters</a>.
 *   Requires "metadata.broker.list" or "bootstrap.servers" to be set with Kafka broker(s),
 *   NOT zookeeper servers, specified in host1:port1,host2:port2 form
 * 该类通过kafka的配置信息,确定集群的环境
 *
 * 与kafka集群交流,返回对应的kafka信息
 */
private[spark]
class KafkaCluster(val kafkaParams: Map[String, String]) //参数是kafka的参数信息
  extends Serializable {
  import KafkaCluster.{Err, LeaderOffset, SimpleConsumerConfig}

  // ConsumerConfig isn't serializable
  @transient private var _config: SimpleConsumerConfig = null //对kafka的ConsumerConfig对象进行简单封装

  def config: SimpleConsumerConfig = this.synchronized {
    if (_config == null) {
      _config = SimpleConsumerConfig(kafkaParams)
    }
    _config
  }

  def connect(host: String, port: Int): SimpleConsumer =
    new SimpleConsumer(host, port, config.socketTimeoutMs,
      config.socketReceiveBufferBytes, config.clientId)

  //连接topic-partition的leader节点
  def connectLeader(topic: String, partition: Int): Either[Err, SimpleConsumer] =
    findLeader(topic, partition).right.map(hp => connect(hp._1, hp._2))

  // Metadata api
  // scalastyle:off
  // https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-MetadataAPI
  // scalastyle:on
 //找到topic-partition的leader节点,返回host port元组
  def findLeader(topic: String, partition: Int): Either[Err, (String, Int)] = {
    val req = TopicMetadataRequest(TopicMetadataRequest.CurrentVersion,
      0, config.clientId, Seq(topic)) //请求对象
    val errs = new Err
    withBrokers(Random.shuffle(config.seedBrokers), errs) { consumer => //表示连接的一个hsot:port之后产生的消费者
      val resp: TopicMetadataResponse = consumer.send(req) //发送请求
      resp.topicsMetadata.find(_.topic == topic).flatMap { tm: TopicMetadata =>
        tm.partitionsMetadata.find(_.partitionId == partition)
      }.foreach { pm: PartitionMetadata =>
        pm.leader.foreach { leader =>
          return Right((leader.host, leader.port))
        }
      }
    }
    Left(errs)
  }

  //找到一组topic-partition对应的leader节点的host port元组
  def findLeaders(
      topicAndPartitions: Set[TopicAndPartition]
    ): Either[Err, Map[TopicAndPartition, (String, Int)]] = {
    val topics = topicAndPartitions.map(_.topic) //所有要找的topic
    val response = getPartitionMetadata(topics).right //返回值
    val answer = response.flatMap { tms: Set[TopicMetadata] =>
      val leaderMap = tms.flatMap { tm: TopicMetadata =>
        tm.partitionsMetadata.flatMap { pm: PartitionMetadata =>
          val tp = TopicAndPartition(tm.topic, pm.partitionId)
          if (topicAndPartitions(tp)) {
            pm.leader.map { l =>
              tp -> (l.host -> l.port)
            }
          } else {
            None
          }
        }
      }.toMap

      if (leaderMap.keys.size == topicAndPartitions.size) {
        Right(leaderMap)
      } else {
        val missing = topicAndPartitions.diff(leaderMap.keySet)
        val err = new Err
        err.append(new SparkException(s"Couldn't find leaders for ${missing}"))
        Left(err)
      }
    }
    answer
  }

  //获取topic集合对应的partition集合
  def getPartitions(topics: Set[String]): Either[Err, Set[TopicAndPartition]] = {
    getPartitionMetadata(topics).right.map { r =>
      r.flatMap { tm: TopicMetadata =>
        tm.partitionsMetadata.map { pm: PartitionMetadata =>
          TopicAndPartition(tm.topic, pm.partitionId)
        }
      }
    }
  }

  //获取所有topic的元数据
  def getPartitionMetadata(topics: Set[String]): Either[Err, Set[TopicMetadata]] = {
    val req = TopicMetadataRequest(
      TopicMetadataRequest.CurrentVersion, 0, config.clientId, topics.toSeq)
    val errs = new Err
    withBrokers(Random.shuffle(config.seedBrokers), errs) { consumer =>
      val resp: TopicMetadataResponse = consumer.send(req)
      val respErrs = resp.topicsMetadata.filter(m => m.errorCode != ErrorMapping.NoError)

      if (respErrs.isEmpty) {
        return Right(resp.topicsMetadata.toSet)
      } else {
        respErrs.foreach { m =>
          val cause = ErrorMapping.exceptionFor(m.errorCode)
          val msg = s"Error getting partition metadata for '${m.topic}'. Does the topic exist?"
          errs.append(new SparkException(msg, cause))
        }
      }
    }
    Left(errs)
  }

  // Leader offset api
  // scalastyle:off
  // https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetAPI
  // scalastyle:on
  //获取leader节点最新的offset序号
  def getLatestLeaderOffsets(
      topicAndPartitions: Set[TopicAndPartition]
    ): Either[Err, Map[TopicAndPartition, LeaderOffset]] =
    getLeaderOffsets(topicAndPartitions, OffsetRequest.LatestTime)

  def getEarliestLeaderOffsets(
      topicAndPartitions: Set[TopicAndPartition]
    ): Either[Err, Map[TopicAndPartition, LeaderOffset]] =
    getLeaderOffsets(topicAndPartitions, OffsetRequest.EarliestTime)

  def getLeaderOffsets(
      topicAndPartitions: Set[TopicAndPartition],
      before: Long
    ): Either[Err, Map[TopicAndPartition, LeaderOffset]] = {
    getLeaderOffsets(topicAndPartitions, before, 1).right.map { r =>
      r.map { kv =>
        // mapValues isnt serializable, see SI-7005
        kv._1 -> kv._2.head
      }
    }
  }

  //参数是K-v结构的map,将他翻转一下,按照V进行group by操作
  private def flip[K, V](m: Map[K, V]): Map[V, Seq[K]] =
    m.groupBy(_._2).map { kv =>
      kv._1 -> kv._2.keys.toSeq
    }

  //返回每一个topic-partition 应该从哪个序号offset开始抓去
  def getLeaderOffsets(
      topicAndPartitions: Set[TopicAndPartition],
      before: Long,
      maxNumOffsets: Int
    ): Either[Err, Map[TopicAndPartition, Seq[LeaderOffset]]] = {
    findLeaders(topicAndPartitions).right.flatMap { tpToLeader => //每一个topic-partition的leader 所在host和port 以及topic-partition对象
      //key是host-port,这个是partition的leader节点  value是该host-port上topic-partition集合
      val leaderToTp: Map[(String, Int), Seq[TopicAndPartition]] = flip(tpToLeader) //每一个host上有哪些topic-partition
      val leaders = leaderToTp.keys //所有的leader节点集合[(host,port)]

      //每一个topic-partition 应该从哪个序号offset开始抓去
      var result = Map[TopicAndPartition, Seq[LeaderOffset]]()
      val errs = new Err
      //循环所有的leader节点,与每一个leader节点进行连接,连接成功后返回consumer对象去进行消费
      withBrokers(leaders, errs) { consumer =>

        //在host-port上的topic-partition集合
        val partitionsToGetOffsets: Seq[TopicAndPartition] =
          leaderToTp((consumer.host, consumer.port))

        //创建请求对象,向leader中请求每一个topic-partition在before之后的第一条信息的序号offset
        val reqMap = partitionsToGetOffsets.map { tp: TopicAndPartition =>
          tp -> PartitionOffsetRequestInfo(before, maxNumOffsets)
        }.toMap

        val req = OffsetRequest(reqMap) //封装请求
        val resp = consumer.getOffsetsBefore(req) //发送请求
        val respMap = resp.partitionErrorAndOffsets //获得结果

        partitionsToGetOffsets.foreach { tp: TopicAndPartition =>
          respMap.get(tp).foreach { por: PartitionOffsetsResponse =>
            if (por.error == ErrorMapping.NoError) {
              if (por.offsets.nonEmpty) {
                //向result中添加返回值
                result += tp -> por.offsets.map { off =>
                  LeaderOffset(consumer.host, consumer.port, off) //获取该host-port上 对应topic-partition已经该消费哪个offset了
                }
              } else {
                errs.append(new SparkException(
                  s"Empty offsets for ${tp}, is ${before} before log beginning?"))
              }
            } else {
              errs.append(ErrorMapping.exceptionFor(por.error))
            }
          }
        }

        if (result.keys.size == topicAndPartitions.size) {
          return Right(result)
        }

      }
      val missing = topicAndPartitions.diff(result.keySet)
      errs.append(new SparkException(s"Couldn't find leader offsets for ${missing}"))
      Left(errs)
    }
  }

  // Consumer offset api
  // scalastyle:off
  // https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
  // scalastyle:on

  // this 0 here indicates api version, in this case the original ZK backed api.
  private def defaultConsumerApiVersion: Short = 0

  /** Requires Kafka >= 0.8.1.1 */
  def getConsumerOffsets(
      groupId: String,
      topicAndPartitions: Set[TopicAndPartition]
    ): Either[Err, Map[TopicAndPartition, Long]] =
    getConsumerOffsets(groupId, topicAndPartitions, defaultConsumerApiVersion)

  def getConsumerOffsets(
      groupId: String,
      topicAndPartitions: Set[TopicAndPartition],
      consumerApiVersion: Short
    ): Either[Err, Map[TopicAndPartition, Long]] = {
    getConsumerOffsetMetadata(groupId, topicAndPartitions, consumerApiVersion).right.map { r =>
      r.map { kv =>
        kv._1 -> kv._2.offset
      }
    }
  }

  /** Requires Kafka >= 0.8.1.1 */
  def getConsumerOffsetMetadata(
      groupId: String,
      topicAndPartitions: Set[TopicAndPartition]
    ): Either[Err, Map[TopicAndPartition, OffsetMetadataAndError]] =
    getConsumerOffsetMetadata(groupId, topicAndPartitions, defaultConsumerApiVersion)

  def getConsumerOffsetMetadata(
      groupId: String,
      topicAndPartitions: Set[TopicAndPartition],
      consumerApiVersion: Short
    ): Either[Err, Map[TopicAndPartition, OffsetMetadataAndError]] = {
    var result = Map[TopicAndPartition, OffsetMetadataAndError]()
    val req = OffsetFetchRequest(groupId, topicAndPartitions.toSeq, consumerApiVersion)
    val errs = new Err
    withBrokers(Random.shuffle(config.seedBrokers), errs) { consumer =>
      val resp = consumer.fetchOffsets(req)
      val respMap = resp.requestInfo
      val needed = topicAndPartitions.diff(result.keySet)
      needed.foreach { tp: TopicAndPartition =>
        respMap.get(tp).foreach { ome: OffsetMetadataAndError =>
          if (ome.error == ErrorMapping.NoError) {
            result += tp -> ome
          } else {
            errs.append(ErrorMapping.exceptionFor(ome.error))
          }
        }
      }
      if (result.keys.size == topicAndPartitions.size) {
        return Right(result)
      }
    }
    val missing = topicAndPartitions.diff(result.keySet)
    errs.append(new SparkException(s"Couldn't find consumer offsets for ${missing}"))
    Left(errs)
  }

  /** Requires Kafka >= 0.8.1.1 */
  def setConsumerOffsets(
      groupId: String,
      offsets: Map[TopicAndPartition, Long]
    ): Either[Err, Map[TopicAndPartition, Short]] =
    setConsumerOffsets(groupId, offsets, defaultConsumerApiVersion)

  def setConsumerOffsets(
      groupId: String,
      offsets: Map[TopicAndPartition, Long],
      consumerApiVersion: Short
    ): Either[Err, Map[TopicAndPartition, Short]] = {
    val meta = offsets.map { kv =>
      kv._1 -> OffsetAndMetadata(kv._2)
    }
    setConsumerOffsetMetadata(groupId, meta, consumerApiVersion)
  }

  /** Requires Kafka >= 0.8.1.1 */
  def setConsumerOffsetMetadata(
      groupId: String,
      metadata: Map[TopicAndPartition, OffsetAndMetadata]
    ): Either[Err, Map[TopicAndPartition, Short]] =
    setConsumerOffsetMetadata(groupId, metadata, defaultConsumerApiVersion)

  def setConsumerOffsetMetadata(
      groupId: String,
      metadata: Map[TopicAndPartition, OffsetAndMetadata],
      consumerApiVersion: Short
    ): Either[Err, Map[TopicAndPartition, Short]] = {
    var result = Map[TopicAndPartition, Short]()
    val req = OffsetCommitRequest(groupId, metadata, consumerApiVersion)
    val errs = new Err
    val topicAndPartitions = metadata.keySet
    withBrokers(Random.shuffle(config.seedBrokers), errs) { consumer =>
      val resp = consumer.commitOffsets(req)
      val respMap = resp.commitStatus
      val needed = topicAndPartitions.diff(result.keySet)
      needed.foreach { tp: TopicAndPartition =>
        respMap.get(tp).foreach { err: Short =>
          if (err == ErrorMapping.NoError) {
            result += tp -> err
          } else {
            errs.append(ErrorMapping.exceptionFor(err))
          }
        }
      }
      if (result.keys.size == topicAndPartitions.size) {
        return Right(result)
      }
    }
    val missing = topicAndPartitions.diff(result.keySet)
    errs.append(new SparkException(s"Couldn't set offsets for ${missing}"))
    Left(errs)
  }

  // Try a call against potentially multiple brokers, accumulating errors
  //withBrokers 第一个参数是borker节点集合,后面跟了一个函数fn,
  //函数fn表示参数是SimpleConsumer,返回值是any
  private def withBrokers(brokers: Iterable[(String, Int)], errs: Err)
    (fn: SimpleConsumer => Any): Unit = {
    brokers.foreach { hp =>
      var consumer: SimpleConsumer = null
      try {
        consumer = connect(hp._1, hp._2) //连接一个host-port
        fn(consumer) //返回连接后的对象,直接给函数fn
      } catch {
        case NonFatal(e) =>
          errs.append(e)
      } finally {
        if (consumer != null) {
          consumer.close()
        }
      }
    }
  }
}

private[spark]
object KafkaCluster {
  type Err = ArrayBuffer[Throwable]

  /** If the result is right, return it, otherwise throw SparkException
    * 返回结果集或者是异常
    **/
  def checkErrors[T](result: Either[Err, T]): T = {
    result.fold(
      errs => throw new SparkException(errs.mkString("\n")),
      ok => ok
    )
  }

  private[spark]
  case class LeaderOffset(host: String, port: Int, offset: Long)

  /**
   * High-level kafka consumers connect to ZK.  ConsumerConfig assumes this use case.
   * Simple consumers connect directly to brokers, but need many of the same configs.
   * This subclass won't warn about missing ZK params, or presence of broker params.
   */
  private[spark]
  class SimpleConsumerConfig private(brokers: String, originalProps: Properties)
      extends ConsumerConfig(originalProps) {
    //返回broker节点集合,String是host,int是port
    val seedBrokers: Array[(String, Int)] = brokers.split(",").map { hp =>
      val hpa = hp.split(":") //host:port
      if (hpa.size == 1) {
        throw new SparkException(s"Broker not the in correct format of <host>:<port> [$brokers]")
      }
      (hpa(0), hpa(1).toInt)
    }
  }

  private[spark]
  object SimpleConsumerConfig {
    /**
     * Make a consumer config without requiring group.id or zookeeper.connect,
     * since communicating with brokers also needs common settings such as timeout
     */
    def apply(kafkaParams: Map[String, String]): SimpleConsumerConfig = {
      // These keys are from other pre-existing kafka configs for specifying brokers, accept either
      //broker节点集合,eg localhost:9092,anotherhost:9092
      val brokers = kafkaParams.get("metadata.broker.list")
        .orElse(kafkaParams.get("bootstrap.servers"))
        .getOrElse(throw new SparkException(
          "Must specify metadata.broker.list or bootstrap.servers"))

      val props = new Properties()
      kafkaParams.foreach { case (key, value) =>
        // prevent warnings on parameters ConsumerConfig doesn't know about
        if (key != "metadata.broker.list" && key != "bootstrap.servers") {
          props.put(key, value)
        }
      }

      Seq("zookeeper.connect", "group.id").foreach { s =>
        if (!props.containsKey(s)) {
          props.setProperty(s, "")
        }
      }

      new SimpleConsumerConfig(brokers, props)
    }
  }
}
