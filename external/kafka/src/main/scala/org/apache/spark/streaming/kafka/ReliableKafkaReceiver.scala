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

import java.util.Properties
import java.util.concurrent.{ThreadPoolExecutor, ConcurrentHashMap}

import scala.collection.{Map, mutable}
import scala.reflect.{ClassTag, classTag}

import kafka.common.TopicAndPartition
import kafka.consumer.{Consumer, ConsumerConfig, ConsumerConnector, KafkaStream}
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import kafka.utils.{VerifiableProperties, ZKGroupTopicDirs, ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient

import org.apache.spark.{Logging, SparkEnv}
import org.apache.spark.storage.{StorageLevel, StreamBlockId}
import org.apache.spark.streaming.receiver.{BlockGenerator, BlockGeneratorListener, Receiver}
import org.apache.spark.util.ThreadUtils

/**
 * ReliableKafkaReceiver offers the ability to reliably store data into BlockManager without loss.
 * It is turned off by default and will be enabled when spark.streaming.receiver.writeAheadLog.enable is true.
 * 该类提供了一个可靠的将接收到的数据存储到BlockManager中,不会丢失数据,默认情况下他被设置为关闭的,并且开启可靠功能时,必须将 spark.streaming.receiver.writeAheadLog.enable属性设置为true,即日志开启
 *
 * The difference compared to KafkaReceiver
 * is that this receiver manages topic-partition/offset itself and updates the offset information
 * after data is reliably stored as write-ahead log.
 * 与KafkaReceiver比较来说,接收器管理者更新offset信息是在数据可靠的存储在write-ahead日志之后
 *
 * Offsets will only be updated when data is reliably stored, so the potential data loss problem of KafkaReceiver can be eliminated.
 * 当数据被可靠的存储到write-ahead日志后,才会更新Offsets,所以KafkaReceiver方式 潜在的数据丢失的可能 被消除了
 * Note: ReliableKafkaReceiver will set auto.commit.enable to false to turn off automatic offset
 * commit mechanism in Kafka consumer. So setting this configuration manually within kafkaParams
 * will not take effect.
 * 表示一个可靠的kafka接收器
 *
 * DirectKafkaInputDStream类是读取kafka的数据,每一个RDD去kafka指定offset位置读取数据内容,定期执行上一次offset位置和最新位置之间的差就是每一个RDD的partition去读取数据的内容,因此该类需要操作kafka的好多底层API,因此该类使用KafkaRDD处理数据
 * 而ReliableKafkaReceiver 这种是创建一个流,时刻接收kafaka的数据,然后存储到HDFS上,定期从HDFS上给spark streaming数据,不使用KafkaRDD处理数据
 */
private[streaming]
class ReliableKafkaReceiver[
  K: ClassTag,//kafka内的key和value类型
  V: ClassTag,
  U <: Decoder[_]: ClassTag,//如何反序列化接收到的kafka的key和value
  T <: Decoder[_]: ClassTag](
    kafkaParams: Map[String, String],//kafka的配置信息
    topics: Map[String, Int],//表示每一个要获取的topic 对应多少个线程去读取
    storageLevel: StorageLevel)
    extends Receiver[(K, V)](storageLevel) with Logging {//接收到的key和value就是kafka的内容

  private val groupId = kafkaParams("group.id")
  private val AUTO_OFFSET_COMMIT = "auto.commit.enable" //自动提交kafka,必须设置为false
  private def conf = SparkEnv.get.conf

  /** High level consumer to connect to Kafka. */
  private var consumerConnector: ConsumerConnector = null //与kafka建立连接

  /** zkClient to connect to Zookeeper to commit the offsets.
    * 真正去调用kafka的zookeeper,去提交offste位置
    **/
  private var zkClient: ZkClient = null

  /**
   * A HashMap to manage the offset for each topic/partition, this HashMap is called in
   * synchronized block, so mutable HashMap will not meet concurrency issue.
   * 更新topic-partition 已经添加到offset位置了
   * 即一个spark的数据块对应了一个这个集合
   */
  private var topicPartitionOffsetMap: mutable.HashMap[TopicAndPartition, Long] = null

  /** A concurrent HashMap to store the stream block id and related offset snapshot.
    *  当产生一个新数据块的时候,备份一下此时每一个partition对应的offset位置快照
    **/
  private var blockOffsetMap: ConcurrentHashMap[StreamBlockId, Map[TopicAndPartition, Long]] = null

  /**
   * Manage the BlockGenerator in receiver itself for better managing block store and offset
   * commit.
   */
  private var blockGenerator: BlockGenerator = null

  /** Thread pool running the handlers for receiving message from multiple topics and partitions.
    * 线程池,用于多线程去获取kafka的数据内容
    **/
  private var messageHandlerThreadPool: ThreadPoolExecutor = null

  override def onStart(): Unit = {
    logInfo(s"Starting Kafka Consumer Stream with group: $groupId")

    // Initialize the topic-partition / offset hash map.
    topicPartitionOffsetMap = new mutable.HashMap[TopicAndPartition, Long]

    // Initialize the stream block id / offset snapshot hash map.
    blockOffsetMap = new ConcurrentHashMap[StreamBlockId, Map[TopicAndPartition, Long]]()

    // Initialize the block generator for storing Kafka message.
    blockGenerator = supervisor.createBlockGenerator(new GeneratedBlockHandler)

    if (kafkaParams.contains(AUTO_OFFSET_COMMIT) && kafkaParams(AUTO_OFFSET_COMMIT) == "true") {//自动提交kafka,必须设置为false
      logWarning(s"$AUTO_OFFSET_COMMIT should be set to false in ReliableKafkaReceiver, " +
        "otherwise we will manually set it to false to turn off auto offset commit in Kafka") //必须设置为false,我们会手动设置false的,关闭kafka的字段提交offset功能
    }

    val props = new Properties()
    kafkaParams.foreach(param => props.put(param._1, param._2))
    // Manually set "auto.commit.enable" to "false" no matter user explicitly set it to true,
    // we have to make sure this property is set to false to turn off auto commit mechanism in
    // Kafka.
    props.setProperty(AUTO_OFFSET_COMMIT, "false")//手动设置自动关闭提交offset功能

    val consumerConfig = new ConsumerConfig(props)

    assert(!consumerConfig.autoCommitEnable)

    logInfo(s"Connecting to Zookeeper: ${consumerConfig.zkConnect}")
    consumerConnector = Consumer.create(consumerConfig) //连接到kafka上
    logInfo(s"Connected to Zookeeper: ${consumerConfig.zkConnect}")

    //创建zookeeper对象
    zkClient = new ZkClient(consumerConfig.zkConnect, consumerConfig.zkSessionTimeoutMs,
      consumerConfig.zkConnectionTimeoutMs, ZKStringSerializer)

    //创建线程池,线程数量就是所有topics需要的线程数量和,即topics.values.sum
    messageHandlerThreadPool = ThreadUtils.newDaemonFixedThreadPool(
      topics.values.sum, "KafkaMessageHandler")

    blockGenerator.start()

    //如何对kafka的key和value进行反序列化操作
    val keyDecoder = classTag[U].runtimeClass.getConstructor(classOf[VerifiableProperties])
      .newInstance(consumerConfig.props)
      .asInstanceOf[Decoder[K]]

    val valueDecoder = classTag[T].runtimeClass.getConstructor(classOf[VerifiableProperties])
      .newInstance(consumerConfig.props)
      .asInstanceOf[Decoder[V]]

    //与kafka建立连接,返回值Map<String,List<KafkaStream>> 返回值,表示key是一个topic,value是读取该topic的线程流集合
    val topicMessageStreams = consumerConnector.createMessageStreams(
      topics, keyDecoder, valueDecoder)

    //为每一个kafka的topic建立的线程流,分配一个MessageHandler去处理,在多线程环境下处理
    topicMessageStreams.values.foreach { streams =>
      streams.foreach { stream =>
        messageHandlerThreadPool.submit(new MessageHandler(stream))
      }
    }
  }

  override def onStop(): Unit = {
    if (messageHandlerThreadPool != null) {//关闭线程池
      messageHandlerThreadPool.shutdown()
      messageHandlerThreadPool = null
    }

    if (consumerConnector != null) {//关闭与kafka的连接
      consumerConnector.shutdown()
      consumerConnector = null
    }

    if (zkClient != null) {//关闭zookeeper
      zkClient.close()
      zkClient = null
    }

    if (blockGenerator != null) {
      blockGenerator.stop()
      blockGenerator = null
    }

    if (topicPartitionOffsetMap != null) {
      topicPartitionOffsetMap.clear()
      topicPartitionOffsetMap = null
    }

    if (blockOffsetMap != null) {
      blockOffsetMap.clear()
      blockOffsetMap = null
    }
  }

  /** Store a Kafka message and the associated metadata as a tuple.
    * 存储一个kafka的一条数据
    * 参数msgAndMetadata 就表示客户端接收到的kafka的一条数据,包括该数据对应的topic-partitiobn  key-value 以及offset
    **/
  private def storeMessageAndMetadata(
      msgAndMetadata: MessageAndMetadata[K, V]): Unit = {
    val topicAndPartition = TopicAndPartition(msgAndMetadata.topic, msgAndMetadata.partition)
    val data = (msgAndMetadata.key, msgAndMetadata.message) //key-value对应的数据
    val metadata = (topicAndPartition, msgAndMetadata.offset) //元数据--该key-value属于哪个topic-partition,以及属于第几个offset数据
    blockGenerator.addDataWithCallback(data, metadata) //存储数据
  }

  /** Update stored offset
    * 更新topic-partition 已经添加到offset位置了
    **/
  private def updateOffset(topicAndPartition: TopicAndPartition, offset: Long): Unit = {
    topicPartitionOffsetMap.put(topicAndPartition, offset)
  }

  /**
   * Remember the current offsets for each topic and partition. This is called when a block is
   * generated.
   * 当产生一个新数据块的时候,备份一下此时每一个partition对应的offset位置快照
   */
  private def rememberBlockOffsets(blockId: StreamBlockId): Unit = {
    // Get a snapshot of current offset map and store with related block id.
    val offsetSnapshot = topicPartitionOffsetMap.toMap
    blockOffsetMap.put(blockId, offsetSnapshot)
    topicPartitionOffsetMap.clear()
  }

  /**
   * Store the ready-to-be-stored block and commit the related offsets to zookeeper. This method
   * will try a fixed number of times to push the block. If the push fails, the receiver is stopped.
   */
  private def storeBlockAndCommitOffset(
      blockId: StreamBlockId, arrayBuffer: mutable.ArrayBuffer[_]): Unit = {
    var count = 0 //最大尝试次数
    var pushed = false //是否推送成功
    var exception: Exception = null
    while (!pushed && count <= 3) {//没推送成功就不断推送
      try {
        store(arrayBuffer.asInstanceOf[mutable.ArrayBuffer[(K, V)]])
        pushed = true
      } catch {
        case ex: Exception =>
          count += 1
          exception = ex
      }
    }
    if (pushed) {
      Option(blockOffsetMap.get(blockId)).foreach(commitOffset) //获取该数据块对应的partition的offset集合,去提交
      blockOffsetMap.remove(blockId) //删除该数据块内存信息
    } else {
      stop("Error while storing block into Spark", exception)
    }
  }

  /**
   * Commit the offset of Kafka's topic/partition, the commit mechanism follow Kafka 0.8.x's
   * metadata schema in Zookeeper.
   * 提交该partition对应的kafka的offset位置,因为该数据块已经存储到spark中了
   */
  private def commitOffset(offsetMap: Map[TopicAndPartition, Long]): Unit = {
    if (zkClient == null) {
      val thrown = new IllegalStateException("Zookeeper client is unexpectedly null")
      stop("Zookeeper client is not initialized before commit offsets to ZK", thrown)
      return
    }

    for ((topicAndPart, offset) <- offsetMap) {
      try {
        val topicDirs = new ZKGroupTopicDirs(groupId, topicAndPart.topic)
        val zkPath = s"${topicDirs.consumerOffsetDir}/${topicAndPart.partition}"

        ZkUtils.updatePersistentPath(zkClient, zkPath, offset.toString) //更新zookeeper的offset位置
      } catch {
        case e: Exception =>
          logWarning(s"Exception during commit offset $offset for topic" +
            s"${topicAndPart.topic}, partition ${topicAndPart.partition}", e)
      }

      logInfo(s"Committed offset $offset for topic ${topicAndPart.topic}, " +
        s"partition ${topicAndPart.partition}")
    }
  }

  /** Class to handle received Kafka message.
    * 处理一个线程对应的kafka partition的数据流
    **/
  private final class MessageHandler(stream: KafkaStream[K, V]) extends Runnable {
    override def run(): Unit = {
      while (!isStopped) {
        try {
          val streamIterator = stream.iterator()
          while (streamIterator.hasNext) {//不断循环,直到内容没有为止,而kafka值不会停止的,没有数据则阻塞而已,因此会一直循环下去
            storeMessageAndMetadata(streamIterator.next)//存储一个kafka的内容
          }
        } catch {
          case e: Exception =>
            reportError("Error handling message", e)
        }
      }
    }
  }

  /** Class to handle blocks generated by the block generator. */
  private final class GeneratedBlockHandler extends BlockGeneratorListener {

    def onAddData(data: Any, metadata: Any): Unit = {
      // Update the offset of the data that was added to the generator
      if (metadata != null) {
        val (topicAndPartition, offset) = metadata.asInstanceOf[(TopicAndPartition, Long)] //获取data对应的topic-partition以及offset序号
        updateOffset(topicAndPartition, offset)
      }
    }

    //当产生一个新的spark数据块的时候调用该方法 demo kafka中当产生一个新数据块的时候,备份一下此时每一个partition对应的offset位置快照
    def onGenerateBlock(blockId: StreamBlockId): Unit = {
      // Remember the offsets of topics/partitions when a block has been generated
      rememberBlockOffsets(blockId)
    }

    def onPushBlock(blockId: StreamBlockId, arrayBuffer: mutable.ArrayBuffer[_]): Unit = {
      // Store block and commit the blocks offset
      storeBlockAndCommitOffset(blockId, arrayBuffer)
    }

    //出现异常的时候.产生的时间
    def onError(message: String, throwable: Throwable): Unit = {
      reportError(message, throwable)
    }
  }
}
