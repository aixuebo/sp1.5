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

import scala.collection.mutable.ArrayBuffer
import scala.reflect.{classTag, ClassTag}

import org.apache.spark.{Logging, Partition, SparkContext, SparkException, TaskContext}
import org.apache.spark.partial.{PartialResult, BoundedDouble}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.NextIterator

import kafka.api.{FetchRequestBuilder, FetchResponse}
import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.message.{MessageAndMetadata, MessageAndOffset}
import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties

/**
 * A batch-oriented interface for consuming from Kafka.
 * Starting and ending offsets are specified in advance,
 * so that you can control exactly-once semantics.
 * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
 * configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers" to be set
 * with Kafka broker(s) specified in host1:port1,host2:port2 form.
 * @param offsetRanges offset ranges that define the Kafka data belonging to this RDD
 * @param messageHandler function for translating each message into the desired type
 *
 * kafka上的信息已经全部都有了,只是做简单的RDD的partition切分即可
 * 即已经知道了每一个topic-partition对应的leader节点在哪里,即leaders
 * 也知道了要分多少个partition,每一个partition获取的offset区间,即offsetRanges
 *
 *
 * 该类用于DirectKafkaInputDStream方式直接读取kafka数据
 */
private[kafka]
class KafkaRDD[
  K: ClassTag,//kafka中数据的key
  V: ClassTag,//kafka中数据的value
  U <: Decoder[_]: ClassTag,
  T <: Decoder[_]: ClassTag,
  R: ClassTag] private[spark] ( //R是最终RDD的元素,当然R也可以是元组
    sc: SparkContext,
    kafkaParams: Map[String, String],//kafka需要的参数
    val offsetRanges: Array[OffsetRange],//读取每一个partition的范围
    leaders: Map[TopicAndPartition, (String, Int)],//每一个partition的leader节点
    messageHandler: MessageAndMetadata[K, V] => R //应该如何处理一个kafka获取到的key-value信息,将其转换成R对象
  ) extends RDD[R](sc, Nil) with Logging with HasOffsetRanges {


  override def getPartitions: Array[Partition] = {//每一个partition对应一个RDD的partition
    offsetRanges.zipWithIndex.map { case (o, i) =>
        val (host, port) = leaders(TopicAndPartition(o.topic, o.partition))//leader所在host和port
        new KafkaRDDPartition(i, o.topic, o.partition, o.fromOffset, o.untilOffset, host, port)
    }.toArray
  }

  //总计算数量
  override def count(): Long = offsetRanges.map(_.count).sum

  override def countApprox(
      timeout: Long,
      confidence: Double = 0.95
  ): PartialResult[BoundedDouble] = {
    val c = count
    new PartialResult(new BoundedDouble(c, 1.0, c, c), true)
  }

  override def isEmpty(): Boolean = count == 0L

  //获取前num个R元素
  override def take(num: Int): Array[R] = {
    //非空的partition集合
    val nonEmptyPartitions = this.partitions
      .map(_.asInstanceOf[KafkaRDDPartition])
      .filter(_.count > 0) //过滤>0的partition

    if (num < 1 || nonEmptyPartitions.size < 1) {
      return new Array[R](0)
    }

    // Determine in advance how many messages need to be taken from each partition
    //默认值是Map[Int, Int]() 存储一个map的元素,key是partition的id,value是要在该partition上获取多少个元素
    val parts = nonEmptyPartitions.foldLeft(Map[Int, Int]()) { (result, part) =>
      val remain = num - result.values.sum //还剩余多少个元素需要拿到
      if (remain > 0) {
        val taken = Math.min(remain, part.count)//获取真正要拿的数量
        result + (part.index -> taken.toInt) //存储一个map的元素,key是partition的id,value是要在该partition上获取多少个元素
      } else {
        result
      }
    }

    val buf = new ArrayBuffer[R]//最终填充结果的数据
    val res = context.runJob(
      this,
      (tc: TaskContext, it: Iterator[R]) => it.take(parts(tc.partitionId)).toArray,//每一个partition中获取多少个数据
      parts.keys.toArray)
    res.foreach(buf ++= _)//将结果填充到buf中
    buf.toArray
  }

  //该partition的leader所在的host节点
  override def getPreferredLocations(thePart: Partition): Seq[String] = {
    val part = thePart.asInstanceOf[KafkaRDDPartition]
    // TODO is additional hostname resolution necessary here
    Seq(part.host)
  }

  private def errBeginAfterEnd(part: KafkaRDDPartition): String =
    s"Beginning offset ${part.fromOffset} is after the ending offset ${part.untilOffset} " +
      s"for topic ${part.topic} partition ${part.partition}. " +
      "You either provided an invalid fromOffset, or the Kafka topic has been damaged"

  private def errRanOutBeforeEnd(part: KafkaRDDPartition): String =
    s"Ran out of messages before reaching ending offset ${part.untilOffset} " +
    s"for topic ${part.topic} partition ${part.partition} start ${part.fromOffset}." +
    " This should not happen, and indicates that messages may have been lost"

  private def errOvershotEnd(itemOffset: Long, part: KafkaRDDPartition): String =
    s"Got ${itemOffset} > ending offset ${part.untilOffset} " +
    s"for topic ${part.topic} partition ${part.partition} start ${part.fromOffset}." +
    " This should not happen, and indicates a message may have been skipped"

  //真正去kafka上读取属于该partition的offset数据内容
  override def compute(thePart: Partition, context: TaskContext): Iterator[R] = {
    val part = thePart.asInstanceOf[KafkaRDDPartition]
    assert(part.fromOffset <= part.untilOffset, errBeginAfterEnd(part))
    if (part.fromOffset == part.untilOffset) {//开始位置和结束位置相同,说明不需要抓取该topic-partition数据
      log.info(s"Beginning offset ${part.fromOffset} is the same as ending offset " +
        s"skipping ${part.topic} ${part.partition}")
      Iterator.empty
    } else {//返回数据
      new KafkaRDDIterator(part, context)
    }
  }

  //真正去读取一个kafka的partition信息
  private class KafkaRDDIterator(
      part: KafkaRDDPartition,
      context: TaskContext) extends NextIterator[R] {

    context.addTaskCompletionListener{ context => closeIfNeeded() }

    //打印日志,说明要计算topic-partition的哪些数据
    log.info(s"Computing topic ${part.topic}, partition ${part.partition} " +
      s"offsets ${part.fromOffset} -> ${part.untilOffset}")

    val kc = new KafkaCluster(kafkaParams)
    val keyDecoder = classTag[U].runtimeClass.getConstructor(classOf[VerifiableProperties])
      .newInstance(kc.config.props)
      .asInstanceOf[Decoder[K]]
    val valueDecoder = classTag[T].runtimeClass.getConstructor(classOf[VerifiableProperties])
      .newInstance(kc.config.props)
      .asInstanceOf[Decoder[V]]
    val consumer = connectLeader //找到leader所在节点的连接器
    var requestOffset = part.fromOffset //初始化是开始位置,其实代表的是已经处理到哪个位置了
    var iter: Iterator[MessageAndOffset] = null //最终的kafka内容的迭代器

    // The idea is to use the provided preferred host, except on task retry atttempts,
    // to minimize number of kafka metadata requests
    //找到leader所在节点的连接器
    private def connectLeader: SimpleConsumer = {
      if (context.attemptNumber > 0) {
        kc.connectLeader(part.topic, part.partition).fold(
          errs => throw new SparkException(
            s"Couldn't connect to leader for topic ${part.topic} ${part.partition}: " +
              errs.mkString("\n")),
          consumer => consumer
        )
      } else {
        kc.connect(part.host, part.port)
      }
    }

    //处理抓取kafka数据时候的异常
    private def handleFetchErr(resp: FetchResponse) {
      if (resp.hasError) {//说明抓去的结果有异常
        val err = resp.errorCode(part.topic, part.partition)
        if (err == ErrorMapping.LeaderNotAvailableCode ||
          err == ErrorMapping.NotLeaderForPartitionCode) {
          log.error(s"Lost leader for topic ${part.topic} partition ${part.partition}, " +
            s" sleeping for ${kc.config.refreshLeaderBackoffMs}ms")
          Thread.sleep(kc.config.refreshLeaderBackoffMs)
        }
        // Let normal rdd retry sort out reconnect attempts
        throw ErrorMapping.exceptionFor(err)
      }
    }

    //去抓去kafka的数据
    private def fetchBatch: Iterator[MessageAndOffset] = {
      //创造抓去请求
      val req = new FetchRequestBuilder()
        .addFetch(part.topic, part.partition, requestOffset, kc.config.fetchMessageMaxBytes) //从requestOffset位置开始抓去.
        .build()
      val resp = consumer.fetch(req) //发送抓去请求
      handleFetchErr(resp) //如果抓去结果有异常,则去处理
      // kafka may return a batch that starts before the requested offset
      resp.messageSet(part.topic, part.partition)
        .iterator
        .dropWhile(_.offset < requestOffset) //将删除元素直到找到第一个匹配谓词函数的元素
    }

    override def close(): Unit = {
      if (consumer != null) {
        consumer.close()
      }
    }

    override def getNext(): R = {
      if (iter == null || !iter.hasNext) {//说明要进行kafka的抓取数据操作
        iter = fetchBatch
      }
      if (!iter.hasNext) {//说明抓取后依然没有数据,则停止抓取,完成操作
        assert(requestOffset == part.untilOffset, errRanOutBeforeEnd(part)) //校验是否处理完成,即处理的数据是否是应该处理的partition的范围截止为止
        finished = true
        null.asInstanceOf[R]
      } else {
        val item = iter.next()
        if (item.offset >= part.untilOffset) {
          assert(item.offset == part.untilOffset, errOvershotEnd(item.offset, part))
          finished = true
          null.asInstanceOf[R]
        } else {
          requestOffset = item.nextOffset
          messageHandler(new MessageAndMetadata(
            part.topic, part.partition, item.message, item.offset, keyDecoder, valueDecoder)) //应该如何处理一个kafka获取到的key-value信息,将其转换成R对象
        }
      }
    }
  }
}

private[kafka]
object KafkaRDD {
  import KafkaCluster.LeaderOffset

  /**
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   * configuration parameters</a>.
   *   Requires "metadata.broker.list" or "bootstrap.servers" to be set with Kafka broker(s),
   *   NOT zookeeper servers, specified in host1:port1,host2:port2 form.
   * @param fromOffsets per-topic/partition Kafka offsets defining the (inclusive)
   *  starting point of the batch
   * @param untilOffsets per-topic/partition Kafka offsets defining the (exclusive)
   *  ending point of the batch
   * @param messageHandler function for translating each message into the desired type
   */
  def apply[
    K: ClassTag,
    V: ClassTag,
    U <: Decoder[_]: ClassTag,
    T <: Decoder[_]: ClassTag,
    R: ClassTag](
      sc: SparkContext,
      kafkaParams: Map[String, String],
      fromOffsets: Map[TopicAndPartition, Long],
      untilOffsets: Map[TopicAndPartition, LeaderOffset],
      messageHandler: MessageAndMetadata[K, V] => R
    ): KafkaRDD[K, V, U, T, R] = {
    val leaders = untilOffsets.map { case (tp, lo) =>
        tp -> (lo.host, lo.port)
    }.toMap //每一个partition的leader节点

    val offsetRanges = fromOffsets.map { case (tp, fo) =>
        val uo = untilOffsets(tp)
        OffsetRange(tp.topic, tp.partition, fo, uo.offset)
    }.toArray

    new KafkaRDD[K, V, U, T, R](sc, kafkaParams, offsetRanges, leaders, messageHandler)
  }
}
