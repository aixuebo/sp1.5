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

import scala.collection.Map
import scala.reflect.{classTag, ClassTag}

import kafka.consumer.{KafkaStream, Consumer, ConsumerConfig, ConsumerConnector}
import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties

import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.util.ThreadUtils

/**
 * Input stream that pulls messages from a Kafka Broker.
 *
 * @param kafkaParams Map of kafka configuration parameters.
 *                    See: http://kafka.apache.org/configuration.html
 * @param topics Map of (topic_name -> numPartitions) to consume. Each partition is consumed
 * in its own thread.
 * @param storageLevel RDD storage level.
 * 是实现了一个receiver
 *
 *
 * 泛型初始化 demo :createStream[String, String, StringDecoder, StringDecoder](参数集合),这样就初始化泛型类型了
 *
 *
 * DirectKafkaInputDStream类是读取kafka的数据,每一个RDD去kafka指定offset位置读取数据内容,定期执行上一次offset位置和最新位置之间的差就是每一个RDD的partition去读取数据的内容,因此该类需要操作kafka的好多底层API,因此该类使用KafkaRDD处理数据
 * 而ReliableKafkaReceiver 这种是创建一个流,时刻接收kafaka的数据,然后存储到HDFS上,定期从HDFS上给spark streaming数据,不使用KafkaRDD处理数据
 */
private[streaming]
class KafkaInputDStream[
  K: ClassTag,//kafka中存储的key和value
  V: ClassTag,
  U <: Decoder[_]: ClassTag,//如何解析kafka中key和value的信息
  T <: Decoder[_]: ClassTag](
    @transient ssc_ : StreamingContext,
    kafkaParams: Map[String, String],//kafka的环境参数
    topics: Map[String, Int],//表示每一个要获取的topic 对应多少个线程去读取
    useReliableReceiver: Boolean,//true表示可靠的接收器
    storageLevel: StorageLevel
  ) extends ReceiverInputDStream[(K, V)](ssc_) with Logging { //说明最终接收到的信息是K,V类型的数据

  def getReceiver(): Receiver[(K, V)] = {
    if (!useReliableReceiver) {//非可靠的接收器
      new KafkaReceiver[K, V, U, T](kafkaParams, topics, storageLevel)
    } else {
      new ReliableKafkaReceiver[K, V, U, T](kafkaParams, topics, storageLevel)
    }
  }
}

//现场去作为消费者,获取指定topic集合的数据,因为kafka会在没有数据的时候阻塞,因此会一直调用,什么时候停止暂时不知道,需要后续继续看
//接收到的kafka数据都会调用store方法,存储起来
//该对象是不可靠的传输,因为他可能会在接收到kafka的数据后,没有到达spark的存储系统前,丢失,因此是不可靠的,spark提供了可靠的方式ReliableKafkaReceiver
private[streaming]
class KafkaReceiver[
  K: ClassTag,
  V: ClassTag,
  U <: Decoder[_]: ClassTag,
  T <: Decoder[_]: ClassTag](
    kafkaParams: Map[String, String],//kafka需要的参数
    topics: Map[String, Int],//表示每一个要获取的topic 对应多少个线程去读取
    storageLevel: StorageLevel
  ) extends Receiver[(K, V)](storageLevel) with Logging {

  // Connection to Kafka 如何连接到kafka
  var consumerConnector: ConsumerConnector = null

  def onStop() {
    if (consumerConnector != null) {
      consumerConnector.shutdown()
      consumerConnector = null
    }
  }

  def onStart() {

    logInfo("Starting Kafka Consumer Stream with group: " + kafkaParams("group.id"))

    // Kafka connection properties
    val props = new Properties()
    kafkaParams.foreach(param => props.put(param._1, param._2)) //将map类型的kafka参数转换成Properties类型

    val zkConnect = kafkaParams("zookeeper.connect")
    // Create the connection to the cluster
    logInfo("Connecting to Zookeeper: " + zkConnect)
    val consumerConfig = new ConsumerConfig(props) //创建kafka客户端环境
    consumerConnector = Consumer.create(consumerConfig)//创建kafka的客户端连接
    logInfo("Connected to " + zkConnect)

    //如何反序列化kafka得到的数据
    val keyDecoder = classTag[U].runtimeClass.getConstructor(classOf[VerifiableProperties])
      .newInstance(consumerConfig.props)
      .asInstanceOf[Decoder[K]]
    val valueDecoder = classTag[T].runtimeClass.getConstructor(classOf[VerifiableProperties])
      .newInstance(consumerConfig.props)
      .asInstanceOf[Decoder[V]]

    // Create threads for each topic/message Stream we are listening
    //Map<String,List<KafkaStream>> 返回值,表示key是一个topic,value是读取该topic的线程流集合
    val topicMessageStreams = consumerConnector.createMessageStreams(
      topics, keyDecoder, valueDecoder)

    //创建线程池,线程数量就是所有topics需要的线程数量和,即topics.values.sum
    val executorPool =
      ThreadUtils.newDaemonFixedThreadPool(topics.values.sum, "KafkaMessageHandler")
    try {
      // Start the messages handler for each partition
      //循环每一个具体的流,每一个流对应一个MessageHandler对象
      topicMessageStreams.values.foreach { streams =>
        streams.foreach { stream => executorPool.submit(new MessageHandler(stream)) }
      }
    } finally {
      executorPool.shutdown() // Just causes threads to terminate after work is done
    }
  }

  // Handles Kafka messages
  //处理一个线程返回来的kafka的信息
  private class MessageHandler(stream: KafkaStream[K, V])
    extends Runnable {
    def run() {
      logInfo("Starting MessageHandler.")
      try {
        val streamIterator = stream.iterator() //迭代返回来的kafka的信息内容
        while (streamIterator.hasNext()) {//不断循环,直到内容没有为止,而kafka值不会停止的,没有数据则阻塞而已,因此会一直循环下去
          val msgAndMetadata = streamIterator.next() //拿回来的就是Key和value信息
          store((msgAndMetadata.key, msgAndMetadata.message))
        }
      } catch {
        case e: Throwable => reportError("Error handling message; exiting", e)
      }
    }
  }
}
