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

import java.lang.{Integer => JInt, Long => JLong}
import java.util.{List => JList, Map => JMap, Set => JSet}

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{Decoder, DefaultDecoder, StringDecoder}

import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaInputDStream, JavaPairInputDStream, JavaPairReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.util.WriteAheadLogUtils
import org.apache.spark.{SparkContext, SparkException}

/**
 * 外界的入口类
 * 官方demo
 val kafkaParams = Map[String, Object](
  "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
  "key.deserializer" -> classOf[StringDeserializer],
  "value.deserializer" -> classOf[StringDeserializer],
  "group.id" -> "use_a_separate_group_id_for_each_stream",
  "auto.offset.reset" -> "latest",
  "enable.auto.commit" -> (false: java.lang.Boolean)
)

val topics = Array("topicA", "topicB")
val stream = KafkaUtils.createDirectStream[String, String](
  streamingContext,
  PreferConsistent,
  Subscribe[String, String](topics, kafkaParams)
)

stream.map(record => (record.key, record.value))
 */
object KafkaUtils {
  /**
   * Create an input stream that pulls messages from Kafka Brokers.
   * @param ssc       StreamingContext object
   * @param zkQuorum  Zookeeper quorum (hostname:port,hostname:port,..)
   * @param groupId   The group id for this consumer
   * @param topics    Map of (topic_name -> numPartitions) to consume. Each partition is consumed
   *                  in its own thread
   * @param storageLevel  Storage level to use for storing the received objects
   *                      (default: StorageLevel.MEMORY_AND_DISK_SER_2)
   */
  def createStream(
      ssc: StreamingContext,
      zkQuorum: String,
      groupId: String,
      topics: Map[String, Int],
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): ReceiverInputDStream[(String, String)] = {
    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> zkQuorum, "group.id" -> groupId,
      "zookeeper.connection.timeout.ms" -> "10000")
    createStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics, storageLevel)
  }

  /**
   * Create an input stream that pulls messages from Kafka Brokers.
   * @param ssc         StreamingContext object
   * @param kafkaParams Map of kafka configuration parameters,
   *                    see http://kafka.apache.org/08/configuration.html
   * @param topics      Map of (topic_name -> numPartitions) to consume. Each partition is consumed
   *                    in its own thread.
   * @param storageLevel Storage level to use for storing the received objects
   */
  def createStream[K: ClassTag, V: ClassTag, U <: Decoder[_]: ClassTag, T <: Decoder[_]: ClassTag](
      ssc: StreamingContext,
      kafkaParams: Map[String, String],
      topics: Map[String, Int],
      storageLevel: StorageLevel
    ): ReceiverInputDStream[(K, V)] = {
    val walEnabled = WriteAheadLogUtils.enableReceiverLog(ssc.conf)
    new KafkaInputDStream[K, V, U, T](ssc, kafkaParams, topics, walEnabled, storageLevel)
  }

  /**
   * Create an input stream that pulls messages from Kafka Brokers.
   * Storage level of the data will be the default StorageLevel.MEMORY_AND_DISK_SER_2.
   * @param jssc      JavaStreamingContext object
   * @param zkQuorum  Zookeeper quorum (hostname:port,hostname:port,..)
   * @param groupId   The group id for this consumer
   * @param topics    Map of (topic_name -> numPartitions) to consume. Each partition is consumed
   *                  in its own thread
   */
  def createStream(
      jssc: JavaStreamingContext,
      zkQuorum: String,
      groupId: String,
      topics: JMap[String, JInt]
    ): JavaPairReceiverInputDStream[String, String] = {
    createStream(jssc.ssc, zkQuorum, groupId, Map(topics.mapValues(_.intValue()).toSeq: _*))
  }

  /**
   * Create an input stream that pulls messages from Kafka Brokers.
   * @param jssc      JavaStreamingContext object
   * @param zkQuorum  Zookeeper quorum (hostname:port,hostname:port,..).
   * @param groupId   The group id for this consumer.
   * @param topics    Map of (topic_name -> numPartitions) to consume. Each partition is consumed
   *                  in its own thread.
   * @param storageLevel RDD storage level.
   */
  def createStream(
      jssc: JavaStreamingContext,
      zkQuorum: String,
      groupId: String,
      topics: JMap[String, JInt],
      storageLevel: StorageLevel
    ): JavaPairReceiverInputDStream[String, String] = {
    createStream(jssc.ssc, zkQuorum, groupId, Map(topics.mapValues(_.intValue()).toSeq: _*),
      storageLevel)
  }

  /**
   * Create an input stream that pulls messages from Kafka Brokers.
   * @param jssc      JavaStreamingContext object
   * @param keyTypeClass Key type of DStream
   * @param valueTypeClass value type of Dstream
   * @param keyDecoderClass Type of kafka key decoder
   * @param valueDecoderClass Type of kafka value decoder
   * @param kafkaParams Map of kafka configuration parameters,
   *                    see http://kafka.apache.org/08/configuration.html
   * @param topics  Map of (topic_name -> numPartitions) to consume. Each partition is consumed
   *                in its own thread
   * @param storageLevel RDD storage level.
   */
  def createStream[K, V, U <: Decoder[_], T <: Decoder[_]](
      jssc: JavaStreamingContext,
      keyTypeClass: Class[K],
      valueTypeClass: Class[V],
      keyDecoderClass: Class[U],
      valueDecoderClass: Class[T],
      kafkaParams: JMap[String, String],
      topics: JMap[String, JInt],
      storageLevel: StorageLevel
    ): JavaPairReceiverInputDStream[K, V] = {
    implicit val keyCmt: ClassTag[K] = ClassTag(keyTypeClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueTypeClass)

    implicit val keyCmd: ClassTag[U] = ClassTag(keyDecoderClass)
    implicit val valueCmd: ClassTag[T] = ClassTag(valueDecoderClass)

    createStream[K, V, U, T](
      jssc.ssc, kafkaParams.toMap, Map(topics.mapValues(_.intValue()).toSeq: _*), storageLevel)
  }

  /** get leaders for the given offset ranges, or throw an exception
    * 返回给定参数需要的topic-partition对应的leader的host和port
    **/
  private def leadersForRanges(
      kc: KafkaCluster,
      offsetRanges: Array[OffsetRange]): Map[TopicAndPartition, (String, Int)] = {
    val topics = offsetRanges.map(o => TopicAndPartition(o.topic, o.partition)).toSet
    val leaders = kc.findLeaders(topics)
    KafkaCluster.checkErrors(leaders)
  }

  /** Make sure offsets are available in kafka, or throw an exception
    * 确保在kafka中topic-partition的offset的有效区间
    **/
  private def checkOffsets(
      kc: KafkaCluster,
      offsetRanges: Array[OffsetRange]): Unit = {
    val topics = offsetRanges.map(_.topicAndPartition).toSet //topic-partition集合
    val result = for {
      low <- kc.getEarliestLeaderOffsets(topics).right //计算topic-partition的最小和最大offset
      high <- kc.getLatestLeaderOffsets(topics).right
    } yield {
      offsetRanges.filterNot { o =>
        low(o.topicAndPartition).offset <= o.fromOffset &&
        o.untilOffset <= high(o.topicAndPartition).offset
      }
    }
    val badRanges = KafkaCluster.checkErrors(result)
    if (!badRanges.isEmpty) {
      throw new SparkException("Offsets not available on leader: " + badRanges.mkString(","))
    }
  }

  /**
   * Create a RDD from Kafka using offset ranges for each topic and partition.
   * 创建一个RDD,读取一个offset区间的内容
   * @param sc SparkContext object
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *    configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
   *    to be set with Kafka broker(s) (NOT zookeeper servers) specified in
   *    host1:port1,host2:port2 form.
   * @param offsetRanges Each OffsetRange in the batch corresponds to a
   *   range of offsets for a given Kafka topic/partition
   */
  def createRDD[
    K: ClassTag,
    V: ClassTag,
    KD <: Decoder[K]: ClassTag,
    VD <: Decoder[V]: ClassTag](
      sc: SparkContext,
      kafkaParams: Map[String, String],
      offsetRanges: Array[OffsetRange]
    ): RDD[(K, V)] = sc.withScope {
    val messageHandler = (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message) //如何处理每一条kafka的数据,结果是只是返回kafka的key和value,组成元组
    val kc = new KafkaCluster(kafkaParams)
    val leaders = leadersForRanges(kc, offsetRanges)
    checkOffsets(kc, offsetRanges)
    new KafkaRDD[K, V, KD, VD, (K, V)](sc, kafkaParams, offsetRanges, leaders, messageHandler)
  }

  /**
   * Create a RDD from Kafka using offset ranges for each topic and partition. This allows you
   * specify the Kafka leader to connect to (to optimize fetching) and access the message as well
   * as the metadata.
   * 允许指定kafka的leader
   *
   * @param sc SparkContext object
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *    configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
   *    to be set with Kafka broker(s) (NOT zookeeper servers) specified in
   *    host1:port1,host2:port2 form.
   * @param offsetRanges Each OffsetRange in the batch corresponds to a
   *   range of offsets for a given Kafka topic/partition
   * @param leaders Kafka brokers for each TopicAndPartition in offsetRanges.  May be an empty map,
   *   in which case leaders will be looked up on the driver.
   * @param messageHandler Function for translating each message and metadata into the desired type
   */
  def createRDD[
    K: ClassTag,
    V: ClassTag,
    KD <: Decoder[K]: ClassTag,
    VD <: Decoder[V]: ClassTag,
    R: ClassTag](
      sc: SparkContext,
      kafkaParams: Map[String, String],
      offsetRanges: Array[OffsetRange],
      leaders: Map[TopicAndPartition, Broker],//自己定义了leader
      messageHandler: MessageAndMetadata[K, V] => R
    ): RDD[R] = sc.withScope {
    val kc = new KafkaCluster(kafkaParams) //连接到kafka集群
    val leaderMap = if (leaders.isEmpty) {
      leadersForRanges(kc, offsetRanges)//查找leader
    } else {
      // This could be avoided by refactoring KafkaRDD.leaders and KafkaCluster to use Broker
      leaders.map {
        case (tp: TopicAndPartition, Broker(host, port)) => (tp, (host, port))//从自己定义的leader里面获取信息
      }.toMap
    }
    val cleanedHandler = sc.clean(messageHandler)
    checkOffsets(kc, offsetRanges)
    new KafkaRDD[K, V, KD, VD, R](sc, kafkaParams, offsetRanges, leaderMap, cleanedHandler)
  }

  /**
   * Create a RDD from Kafka using offset ranges for each topic and partition.
   *
   * @param jsc JavaSparkContext object
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *    configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
   *    to be set with Kafka broker(s) (NOT zookeeper servers) specified in
   *    host1:port1,host2:port2 form.
   * @param offsetRanges Each OffsetRange in the batch corresponds to a
   *   range of offsets for a given Kafka topic/partition
   */
  def createRDD[K, V, KD <: Decoder[K], VD <: Decoder[V]](
      jsc: JavaSparkContext,
      keyClass: Class[K],
      valueClass: Class[V],
      keyDecoderClass: Class[KD],
      valueDecoderClass: Class[VD],
      kafkaParams: JMap[String, String],
      offsetRanges: Array[OffsetRange]
    ): JavaPairRDD[K, V] = jsc.sc.withScope {
    implicit val keyCmt: ClassTag[K] = ClassTag(keyClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueClass)
    implicit val keyDecoderCmt: ClassTag[KD] = ClassTag(keyDecoderClass)
    implicit val valueDecoderCmt: ClassTag[VD] = ClassTag(valueDecoderClass)
    new JavaPairRDD(createRDD[K, V, KD, VD](
      jsc.sc, Map(kafkaParams.toSeq: _*), offsetRanges))
  }

  /**
   * Create a RDD from Kafka using offset ranges for each topic and partition. This allows you
   * specify the Kafka leader to connect to (to optimize fetching) and access the message as well
   * as the metadata.
   *
   * @param jsc JavaSparkContext object
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *    configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
   *    to be set with Kafka broker(s) (NOT zookeeper servers) specified in
   *    host1:port1,host2:port2 form.
   * @param offsetRanges Each OffsetRange in the batch corresponds to a
   *   range of offsets for a given Kafka topic/partition
   * @param leaders Kafka brokers for each TopicAndPartition in offsetRanges.  May be an empty map,
   *   in which case leaders will be looked up on the driver.
   * @param messageHandler Function for translating each message and metadata into the desired type
   */
  def createRDD[K, V, KD <: Decoder[K], VD <: Decoder[V], R](
      jsc: JavaSparkContext,
      keyClass: Class[K],
      valueClass: Class[V],
      keyDecoderClass: Class[KD],
      valueDecoderClass: Class[VD],
      recordClass: Class[R],
      kafkaParams: JMap[String, String],
      offsetRanges: Array[OffsetRange],
      leaders: JMap[TopicAndPartition, Broker],
      messageHandler: JFunction[MessageAndMetadata[K, V], R]
    ): JavaRDD[R] = jsc.sc.withScope {
    implicit val keyCmt: ClassTag[K] = ClassTag(keyClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueClass)
    implicit val keyDecoderCmt: ClassTag[KD] = ClassTag(keyDecoderClass)
    implicit val valueDecoderCmt: ClassTag[VD] = ClassTag(valueDecoderClass)
    implicit val recordCmt: ClassTag[R] = ClassTag(recordClass)
    val leaderMap = Map(leaders.toSeq: _*)
    createRDD[K, V, KD, VD, R](
      jsc.sc, Map(kafkaParams.toSeq: _*), offsetRanges, leaderMap, messageHandler.call _)
  }

  /**
   * Create an input stream that directly pulls messages from Kafka Brokers
   * without using any receiver. This stream can guarantee that each message
   * from Kafka is included in transformations exactly once (see points below).
   * 创建一个stream,直接从kafka节点上拉去信息,不需要任何receiver,
   * 这个数据流能保证从kafka抓去回来的每一个信息 在转换过程中,明确以下几点
   * Points to note:
   *  - No receivers: This stream does not use any receiver. It directly queries Kafka 不使用任何receiver,因此直接查询的kafka
   *  - Offsets: This does not use Zookeeper to store offsets. The consumed offsets are tracked
   *    by the stream itself. For interoperability with Kafka monitoring tools that depend on
   *    Zookeeper, you have to update Kafka/Zookeeper yourself from the streaming application.
   *    You can access the offsets used in each batch from the generated RDDs (see
   *    [[org.apache.spark.streaming.kafka.HasOffsetRanges]]).
   *    不使用zookeeper存储offset序号,消费者的序号是流自己跟踪的,
   *  - Failure Recovery: To recover from driver failures, you have to enable checkpointing
   *    in the [[StreamingContext]]. The information on consumed offset can be
   *    recovered from the checkpoint. See the programming guide for details (constraints, etc.).
   *  - End-to-end semantics: This stream ensures that every records is effectively received and
   *    transformed exactly once, but gives no guarantees on whether the transformed data are
   *    outputted exactly once. For end-to-end exactly-once semantics, you have to either ensure
   *    that the output operation is idempotent, or use transactions to output records atomically.
   *    See the programming guide for more details.
   *
   * @param ssc StreamingContext object
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *    configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
   *    to be set with Kafka broker(s) (NOT zookeeper servers) specified in
   *    host1:port1,host2:port2 form.
   * @param fromOffsets Per-topic/partition Kafka offsets defining the (inclusive)
   *    starting point of the stream
   * @param messageHandler Function for translating each message and metadata into the desired type 如何处理每一个kafka的数据
   */
  def createDirectStream[
    K: ClassTag,
    V: ClassTag,
    KD <: Decoder[K]: ClassTag,
    VD <: Decoder[V]: ClassTag,
    R: ClassTag] (
      ssc: StreamingContext,
      kafkaParams: Map[String, String],
      fromOffsets: Map[TopicAndPartition, Long],
      messageHandler: MessageAndMetadata[K, V] => R
  ): InputDStream[R] = {
    val cleanedHandler = ssc.sc.clean(messageHandler)
    new DirectKafkaInputDStream[K, V, KD, VD, R](
      ssc, kafkaParams, fromOffsets, cleanedHandler)
  }

  /**
   * Create an input stream that directly pulls messages from Kafka Brokers
   * without using any receiver. This stream can guarantee that each message
   * from Kafka is included in transformations exactly once (see points below).
   *
   * Points to note:
   *  - No receivers: This stream does not use any receiver. It directly queries Kafka
   *  - Offsets: This does not use Zookeeper to store offsets. The consumed offsets are tracked
   *    by the stream itself. For interoperability with Kafka monitoring tools that depend on
   *    Zookeeper, you have to update Kafka/Zookeeper yourself from the streaming application.
   *    You can access the offsets used in each batch from the generated RDDs (see
   *    [[org.apache.spark.streaming.kafka.HasOffsetRanges]]).
   *  - Failure Recovery: To recover from driver failures, you have to enable checkpointing
   *    in the [[StreamingContext]]. The information on consumed offset can be
   *    recovered from the checkpoint. See the programming guide for details (constraints, etc.).
   *  - End-to-end semantics: This stream ensures that every records is effectively received and
   *    transformed exactly once, but gives no guarantees on whether the transformed data are
   *    outputted exactly once. For end-to-end exactly-once semantics, you have to either ensure
   *    that the output operation is idempotent, or use transactions to output records atomically.
   *    See the programming guide for more details.
   *
   * @param ssc StreamingContext object
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *   configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
   *   to be set with Kafka broker(s) (NOT zookeeper servers), specified in
   *   host1:port1,host2:port2 form.
   *   If not starting from a checkpoint, "auto.offset.reset" may be set to "largest" or "smallest"
   *   to determine where the stream starts (defaults to "largest")
   * @param topics Names of the topics to consume
   * demo的入口
   */
  def createDirectStream[
    K: ClassTag,
    V: ClassTag,
    KD <: Decoder[K]: ClassTag,
    VD <: Decoder[V]: ClassTag] (
      ssc: StreamingContext,
      kafkaParams: Map[String, String],//kafka参数
      topics: Set[String] //要获取哪些topic集合
  ): InputDStream[(K, V)] = {//K和V表示提供的返回值类型
    val messageHandler = (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message) //如何处理接收到的kafka信息,他将其转换成key,value的元组
    val kc = new KafkaCluster(kafkaParams)
    val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)

    val result = for {
      topicPartitions <- kc.getPartitions(topics).right //获取topic集合对应的partition集合
      leaderOffsets <- (if (reset == Some("smallest")) {//获取leader节点最老的offset序号
        kc.getEarliestLeaderOffsets(topicPartitions)
      } else {
        kc.getLatestLeaderOffsets(topicPartitions)//获取leader节点最新的offset序号
      }).right
    } yield {
      val fromOffsets = leaderOffsets.map { case (tp, lo) =>
          (tp, lo.offset)
      }
      new DirectKafkaInputDStream[K, V, KD, VD, (K, V)](
        ssc, kafkaParams, fromOffsets, messageHandler)
    }
    KafkaCluster.checkErrors(result)
  }

  /**
   * Create an input stream that directly pulls messages from Kafka Brokers
   * without using any receiver. This stream can guarantee that each message
   * from Kafka is included in transformations exactly once (see points below).
   *
   * Points to note:
   *  - No receivers: This stream does not use any receiver. It directly queries Kafka
   *  - Offsets: This does not use Zookeeper to store offsets. The consumed offsets are tracked
   *    by the stream itself. For interoperability with Kafka monitoring tools that depend on
   *    Zookeeper, you have to update Kafka/Zookeeper yourself from the streaming application.
   *    You can access the offsets used in each batch from the generated RDDs (see
   *    [[org.apache.spark.streaming.kafka.HasOffsetRanges]]).
   *  - Failure Recovery: To recover from driver failures, you have to enable checkpointing
   *    in the [[StreamingContext]]. The information on consumed offset can be
   *    recovered from the checkpoint. See the programming guide for details (constraints, etc.).
   *  - End-to-end semantics: This stream ensures that every records is effectively received and
   *    transformed exactly once, but gives no guarantees on whether the transformed data are
   *    outputted exactly once. For end-to-end exactly-once semantics, you have to either ensure
   *    that the output operation is idempotent, or use transactions to output records atomically.
   *    See the programming guide for more details.
   *
   * @param jssc JavaStreamingContext object
   * @param keyClass Class of the keys in the Kafka records
   * @param valueClass Class of the values in the Kafka records
   * @param keyDecoderClass Class of the key decoder
   * @param valueDecoderClass Class of the value decoder
   * @param recordClass Class of the records in DStream
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *   configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
   *   to be set with Kafka broker(s) (NOT zookeeper servers), specified in
   *   host1:port1,host2:port2 form.
   * @param fromOffsets Per-topic/partition Kafka offsets defining the (inclusive)
   *    starting point of the stream
   * @param messageHandler Function for translating each message and metadata into the desired type
   */
  def createDirectStream[K, V, KD <: Decoder[K], VD <: Decoder[V], R](
      jssc: JavaStreamingContext,
      keyClass: Class[K],
      valueClass: Class[V],
      keyDecoderClass: Class[KD],
      valueDecoderClass: Class[VD],
      recordClass: Class[R],
      kafkaParams: JMap[String, String],
      fromOffsets: JMap[TopicAndPartition, JLong],
      messageHandler: JFunction[MessageAndMetadata[K, V], R]
    ): JavaInputDStream[R] = {
    implicit val keyCmt: ClassTag[K] = ClassTag(keyClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueClass)
    implicit val keyDecoderCmt: ClassTag[KD] = ClassTag(keyDecoderClass)
    implicit val valueDecoderCmt: ClassTag[VD] = ClassTag(valueDecoderClass)
    implicit val recordCmt: ClassTag[R] = ClassTag(recordClass)
    val cleanedHandler = jssc.sparkContext.clean(messageHandler.call _)
    createDirectStream[K, V, KD, VD, R](
      jssc.ssc,
      Map(kafkaParams.toSeq: _*),
      Map(fromOffsets.mapValues { _.longValue() }.toSeq: _*),
      cleanedHandler
    )
  }

  /**
   * Create an input stream that directly pulls messages from Kafka Brokers
   * without using any receiver. This stream can guarantee that each message
   * from Kafka is included in transformations exactly once (see points below).
   *
   * Points to note:
   *  - No receivers: This stream does not use any receiver. It directly queries Kafka
   *  - Offsets: This does not use Zookeeper to store offsets. The consumed offsets are tracked
   *    by the stream itself. For interoperability with Kafka monitoring tools that depend on
   *    Zookeeper, you have to update Kafka/Zookeeper yourself from the streaming application.
   *    You can access the offsets used in each batch from the generated RDDs (see
   *    [[org.apache.spark.streaming.kafka.HasOffsetRanges]]).
   *  - Failure Recovery: To recover from driver failures, you have to enable checkpointing
   *    in the [[StreamingContext]]. The information on consumed offset can be
   *    recovered from the checkpoint. See the programming guide for details (constraints, etc.).
   *  - End-to-end semantics: This stream ensures that every records is effectively received and
   *    transformed exactly once, but gives no guarantees on whether the transformed data are
   *    outputted exactly once. For end-to-end exactly-once semantics, you have to either ensure
   *    that the output operation is idempotent, or use transactions to output records atomically.
   *    See the programming guide for more details.
   *
   * @param jssc JavaStreamingContext object
   * @param keyClass Class of the keys in the Kafka records
   * @param valueClass Class of the values in the Kafka records
   * @param keyDecoderClass Class of the key decoder
   * @param valueDecoderClass Class type of the value decoder
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *   configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
   *   to be set with Kafka broker(s) (NOT zookeeper servers), specified in
   *   host1:port1,host2:port2 form.
   *   If not starting from a checkpoint, "auto.offset.reset" may be set to "largest" or "smallest"
   *   to determine where the stream starts (defaults to "largest")
   * @param topics Names of the topics to consume
   */
  def createDirectStream[K, V, KD <: Decoder[K], VD <: Decoder[V]](
      jssc: JavaStreamingContext,
      keyClass: Class[K],
      valueClass: Class[V],
      keyDecoderClass: Class[KD],
      valueDecoderClass: Class[VD],
      kafkaParams: JMap[String, String],
      topics: JSet[String]
    ): JavaPairInputDStream[K, V] = {
    implicit val keyCmt: ClassTag[K] = ClassTag(keyClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueClass)
    implicit val keyDecoderCmt: ClassTag[KD] = ClassTag(keyDecoderClass)
    implicit val valueDecoderCmt: ClassTag[VD] = ClassTag(valueDecoderClass)
    createDirectStream[K, V, KD, VD](
      jssc.ssc,
      Map(kafkaParams.toSeq: _*),
      Set(topics.toSeq: _*)
    )
  }
}

/**
 * This is a helper class that wraps the KafkaUtils.createStream() into more
 * Python-friendly class and function so that it can be easily
 * instantiated and called from Python's KafkaUtils (see SPARK-6027).
 *
 * The zero-arg constructor helps instantiate this class from the Class object
 * classOf[KafkaUtilsPythonHelper].newInstance(), and the createStream()
 * takes care of known parameters instead of passing them from Python
 */
private[kafka] class KafkaUtilsPythonHelper {
  def createStream(
      jssc: JavaStreamingContext,
      kafkaParams: JMap[String, String],
      topics: JMap[String, JInt],
      storageLevel: StorageLevel): JavaPairReceiverInputDStream[Array[Byte], Array[Byte]] = {
    KafkaUtils.createStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](
      jssc,
      classOf[Array[Byte]],
      classOf[Array[Byte]],
      classOf[DefaultDecoder],
      classOf[DefaultDecoder],
      kafkaParams,
      topics,
      storageLevel)
  }

  def createRDD(
      jsc: JavaSparkContext,
      kafkaParams: JMap[String, String],
      offsetRanges: JList[OffsetRange],
      leaders: JMap[TopicAndPartition, Broker]): JavaPairRDD[Array[Byte], Array[Byte]] = {
    val messageHandler = new JFunction[MessageAndMetadata[Array[Byte], Array[Byte]],
      (Array[Byte], Array[Byte])] {
      def call(t1: MessageAndMetadata[Array[Byte], Array[Byte]]): (Array[Byte], Array[Byte]) =
        (t1.key(), t1.message())
    }

    val jrdd = KafkaUtils.createRDD[
      Array[Byte],
      Array[Byte],
      DefaultDecoder,
      DefaultDecoder,
      (Array[Byte], Array[Byte])](
        jsc,
        classOf[Array[Byte]],
        classOf[Array[Byte]],
        classOf[DefaultDecoder],
        classOf[DefaultDecoder],
        classOf[(Array[Byte], Array[Byte])],
        kafkaParams,
        offsetRanges.toArray(new Array[OffsetRange](offsetRanges.size())),
        leaders,
        messageHandler
      )
    new JavaPairRDD(jrdd.rdd)
  }

  def createDirectStream(
      jssc: JavaStreamingContext,
      kafkaParams: JMap[String, String],
      topics: JSet[String],
      fromOffsets: JMap[TopicAndPartition, JLong]
    ): JavaPairInputDStream[Array[Byte], Array[Byte]] = {

    if (!fromOffsets.isEmpty) {
      import scala.collection.JavaConversions._
      val topicsFromOffsets = fromOffsets.keySet().map(_.topic)
      if (topicsFromOffsets != topics.toSet) {
        throw new IllegalStateException(s"The specified topics: ${topics.toSet.mkString(" ")} " +
          s"do not equal to the topic from offsets: ${topicsFromOffsets.mkString(" ")}")
      }
    }

    if (fromOffsets.isEmpty) {
      KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](
        jssc,
        classOf[Array[Byte]],
        classOf[Array[Byte]],
        classOf[DefaultDecoder],
        classOf[DefaultDecoder],
        kafkaParams,
        topics)
    } else {
      val messageHandler = new JFunction[MessageAndMetadata[Array[Byte], Array[Byte]],
        (Array[Byte], Array[Byte])] {
        def call(t1: MessageAndMetadata[Array[Byte], Array[Byte]]): (Array[Byte], Array[Byte]) =
          (t1.key(), t1.message())
      }

      val jstream = KafkaUtils.createDirectStream[
        Array[Byte],
        Array[Byte],
        DefaultDecoder,
        DefaultDecoder,
        (Array[Byte], Array[Byte])](
          jssc,
          classOf[Array[Byte]],
          classOf[Array[Byte]],
          classOf[DefaultDecoder],
          classOf[DefaultDecoder],
          classOf[(Array[Byte], Array[Byte])],
          kafkaParams,
          fromOffsets,
          messageHandler)
      new JavaPairInputDStream(jstream.inputDStream)
    }
  }

  def createOffsetRange(topic: String, partition: JInt, fromOffset: JLong, untilOffset: JLong
    ): OffsetRange = OffsetRange.create(topic, partition, fromOffset, untilOffset)

  def createTopicAndPartition(topic: String, partition: JInt): TopicAndPartition =
    TopicAndPartition(topic, partition)

  def createBroker(host: String, port: JInt): Broker = Broker(host, port)

  def offsetRangesOfKafkaRDD(rdd: RDD[_]): JList[OffsetRange] = {
    val parentRDDs = rdd.getNarrowAncestors
    val kafkaRDDs = parentRDDs.filter(rdd => rdd.isInstanceOf[KafkaRDD[_, _, _, _, _]])

    require(
      kafkaRDDs.length == 1,
      "Cannot get offset ranges, as there may be multiple Kafka RDDs or no Kafka RDD associated" +
        "with this RDD, please call this method only on a Kafka RDD.")

    val kafkaRDD = kafkaRDDs.head.asInstanceOf[KafkaRDD[_, _, _, _, _]]
    kafkaRDD.offsetRanges.toSeq
  }
}
