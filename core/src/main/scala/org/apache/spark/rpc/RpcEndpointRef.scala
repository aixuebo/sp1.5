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

package org.apache.spark.rpc

import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.spark.util.RpcUtils
import org.apache.spark.{SparkException, Logging, SparkConf}

/**
 * A reference for a remote [[RpcEndpoint]]. [[RpcEndpointRef]] is thread-safe.
 * 代表一个客户端
 * 
 * 实现类AkkaRpcEnv的AkkaRpcEndpointRef类
 * 
 * 例如:在master中与每一个application都存在一个该对象,通过该对象,即master服务器可以通过该对象与application进行通信
 *
 * 实现类 org.apache.spark.rpc.akka.AkkaRpcEnv的AkkaRpcEndpointRef内部类
 */
private[spark] abstract class RpcEndpointRef(@transient conf: SparkConf)
  extends Serializable with Logging {

  private[this] val maxRetries = RpcUtils.numRetries(conf) //最大尝试次数
  private[this] val retryWaitMs = RpcUtils.retryWaitMs(conf) //等待时间间隔
  private[this] val defaultAskTimeout = RpcUtils.askRpcTimeout(conf) //超时时间

  /**
   * return the address for the [[RpcEndpointRef]]
   * 返回ActorRef的地址
   */
  def address: RpcAddress

  def name: String

  /**
   * Sends a one-way asynchronous message. Fire-and-forget semantics.
   * tell是发完就走,纯粹的异步发送
   */
  def send(message: Any): Unit

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
   * receive the reply within the specified timeout.
   *
   * This method only sends the message once and never retries.
   * 仅会发送一次信息,ask是发完等Future
   */
  def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T]

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
   * receive the reply within a default timeout.
   *
   * This method only sends the message once and never retries.
   * 要发送信息给调用方,返回T类型数据,是未来者模式获取返回值
   * 仅会发送一次信息
   */
  def ask[T: ClassTag](message: Any): Future[T] = ask(message, defaultAskTimeout)

  /**
   * Send a message to the corresponding [[RpcEndpoint]] and get its result within a default
   * timeout, or throw a SparkException if this fails even after the default number of retries.
   * The default `timeout` will be used in every trial of calling `sendWithReply`. Because this
   * method retries, the message handling in the receiver side should be idempotent.
   *
   * Note: this is a blocking action which may cost a lot of time,  so don't call it in an message
   * loop of [[RpcEndpoint]].
   *
   * @param message the message to send
   * @tparam T type of the reply message
   * @return the reply message from the corresponding [[RpcEndpoint]]
   * 尝试多次发送该信息
   * 该方法表示发送信息message,期待返回值是类型T
   */
  def askWithRetry[T: ClassTag](message: Any): T = askWithRetry(message, defaultAskTimeout)

  /**
   * Send a message to the corresponding [[RpcEndpoint.receive]] and get its result within a
   * specified timeout, throw a SparkException if this fails even after the specified number of
   * retries. `timeout` will be used in every trial of calling `sendWithReply`. Because this method
   * retries, the message handling in the receiver side should be idempotent.
   *
   * Note: this is a blocking action which may cost a lot of time, so don't call it in an message
   * loop of [[RpcEndpoint]].
   *
   * @param message the message to send
   * @param timeout the timeout duration
   * @tparam T type of the reply message
   * @return the reply message from the corresponding [[RpcEndpoint]]
   * 尝试多次去发送信息
   */
  def askWithRetry[T: ClassTag](message: Any, timeout: RpcTimeout): T = {
    // TODO: Consider removing multiple attempts
    var attempts = 0
    var lastException: Exception = null
    while (attempts < maxRetries) {//尝试多次
      attempts += 1
      try {
        val future = ask[T](message, timeout)//进行一次发送
        val result = timeout.awaitResult(future)//超时范围内等待返回结果
        if (result == null) {//如果结果为null,则抛异常
          throw new SparkException("RpcEndpoint returned null")
        }
        return result //如果在尝试次数内,获得到结果了
      } catch {
        case ie: InterruptedException => throw ie
        case e: Exception =>
          lastException = e //如果是正常的异常,则打印日志,下一次接续尝试
          logWarning(s"Error sending message [message = $message] in $attempts attempts", e)
      }

      if (attempts < maxRetries) {
        Thread.sleep(retryWaitMs) //休息时间间隔
      }
    }

    throw new SparkException(//说明尝试很多次后,依然发送失败
      s"Error sending message [message = $message]", lastException)
  }

}
