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

package org.apache.spark.util

import scala.collection.JavaConversions.mapAsJavaMap

import akka.actor.{ActorRef, ActorSystem, ExtendedActorSystem}
import akka.pattern.ask

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}

import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkEnv, SparkException}
import org.apache.spark.rpc.RpcTimeout

/**
 * Various utility classes for working with Akka.
 * Akka工作的各种有效工具类
 */
private[spark] object AkkaUtils extends Logging {

  /**
   * Creates an ActorSystem ready for remoting, with various Spark features. Returns both the
   * ActorSystem itself and its port (which is hard to get from Akka).
   * 创建Actor系统,以及系统所在port
   * Note: the `name` parameter is important, as even if a client sends a message to right
   * host + port, if the system name is incorrect, Akka will drop the message.
   * 注意,name参数非常重要,如果一个客户端发送一个信息给正确的host+port,但是给定的name无法找到,即错误的name,则Akka服务器会丢掉该信息
   * 原因是actor是基于内部的收件箱操作的
   * If indestructible is set to true, the Actor System will continue running in the event
   * of a fatal exception. This is used by [[org.apache.spark.executor.Executor]].
   */
  def createActorSystem(
      name: String,//actorSystem的Name
      host: String,//服务器所在host
      port: Int,//服务器所在port
      conf: SparkConf,
      securityManager: SecurityManager): (ActorSystem, Int) = {
    //定义一个函数,传入参数int实际的port,返回值是ActorSystem, Int
    val startService: Int => (ActorSystem, Int) = { actualPort =>
      doCreateActorSystem(name, host, actualPort, conf, securityManager)
    }
    //真正创建一个server,以及真实的端口
    Utils.startServiceOnPort(port, startService, conf, name)
  }

  /**
   * 创建一个服务端,在host port上进行监听,
   * 返回真正要监听的端口号以及Actor系统
   */
  private def doCreateActorSystem(
      name: String,//actorSystem的Name
      host: String,
      port: Int,
      conf: SparkConf,
      securityManager: SecurityManager): (ActorSystem, Int) = {

    val akkaThreads = conf.getInt("spark.akka.threads", 4)
    val akkaBatchSize = conf.getInt("spark.akka.batchSize", 15)
    val akkaTimeoutS = conf.getTimeAsSeconds("spark.akka.timeout",
      conf.get("spark.network.timeout", "120s"))
    val akkaFrameSize = maxFrameSizeBytes(conf) //设置传递一个message允许的字节上限,参数单位是M,将其转换成字节  
    val akkaLogLifecycleEvents = conf.getBoolean("spark.akka.logLifecycleEvents", false)
    val lifecycleEvents = if (akkaLogLifecycleEvents) "on" else "off"
    if (!akkaLogLifecycleEvents) {
      // As a workaround for Akka issue #3787, we coerce the "EndpointWriter" log to be silent.
      // See: https://www.assembla.com/spaces/akka/tickets/3787#/
      Option(Logger.getLogger("akka.remote.EndpointWriter")).map(l => l.setLevel(Level.FATAL))
    }

    val logAkkaConfig = if (conf.getBoolean("spark.akka.logAkkaConfig", false)) "on" else "off"

    val akkaHeartBeatPausesS = conf.getTimeAsSeconds("spark.akka.heartbeat.pauses", "6000s") //传输失败后,时间间隔
    val akkaHeartBeatIntervalS = conf.getTimeAsSeconds("spark.akka.heartbeat.interval", "1000s")

    val secretKey = securityManager.getSecretKey()
    val isAuthOn = securityManager.isAuthenticationEnabled()
    if (isAuthOn && secretKey == null) {
      throw new Exception("Secret key is null with authentication on")
    }
    val requireCookie = if (isAuthOn) "on" else "off"
    val secureCookie = if (isAuthOn) secretKey else ""
    logDebug(s"In createActorSystem, requireCookie is: $requireCookie")

    val akkaSslConfig = securityManager.akkaSSLOptions.createAkkaConfig
        .getOrElse(ConfigFactory.empty())

    //conf.getAkkaConf.toMap[String, String],表示以akka.开头的key,就是akka的配置文件信息,因此获取以 akka.开头的key组成的配置信息元组集合
    val akkaConf = ConfigFactory.parseMap(conf.getAkkaConf.toMap[String, String])
      .withFallback(akkaSslConfig).withFallback(ConfigFactory.parseString(
      s"""
      |akka.daemonic = on
      |akka.loggers = [""akka.event.slf4j.Slf4jLogger""]
      |akka.stdout-loglevel = "ERROR"
      |akka.jvm-exit-on-fatal-error = off
      |akka.remote.require-cookie = "$requireCookie"
      |akka.remote.secure-cookie = "$secureCookie"
      |akka.remote.transport-failure-detector.heartbeat-interval = $akkaHeartBeatIntervalS s
      |akka.remote.transport-failure-detector.acceptable-heartbeat-pause = $akkaHeartBeatPausesS s
      |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
      |akka.remote.netty.tcp.transport-class = "akka.remote.transport.netty.NettyTransport"
      |akka.remote.netty.tcp.hostname = "$host"
      |akka.remote.netty.tcp.port = $port
      |akka.remote.netty.tcp.tcp-nodelay = on
      |akka.remote.netty.tcp.connection-timeout = $akkaTimeoutS s
      |akka.remote.netty.tcp.maximum-frame-size = ${akkaFrameSize}B
      |akka.remote.netty.tcp.execution-pool-size = $akkaThreads
      |akka.actor.default-dispatcher.throughput = $akkaBatchSize
      |akka.log-config-on-start = $logAkkaConfig
      |akka.remote.log-remote-lifecycle-events = $lifecycleEvents
      |akka.log-dead-letters = $lifecycleEvents
      |akka.log-dead-letters-during-shutdown = $lifecycleEvents
      """.stripMargin))

    val actorSystem = ActorSystem(name, akkaConf)
    val provider = actorSystem.asInstanceOf[ExtendedActorSystem].provider
    val boundPort = provider.getDefaultAddress.port.get
    (actorSystem, boundPort)
  }

  //传递一个message允许的字节上限
  private val AKKA_MAX_FRAME_SIZE_IN_MB = Int.MaxValue / 1024 / 1024

  /** Returns the configured max frame size for Akka messages in bytes. 
    * 返回一个message允许的上限,单位是字节
   **/
  def maxFrameSizeBytes(conf: SparkConf): Int = {
    val frameSizeInMB = conf.getInt("spark.akka.frameSize", 128)//参数单位是M,最后需要转换成字节
    if (frameSizeInMB > AKKA_MAX_FRAME_SIZE_IN_MB) {
      throw new IllegalArgumentException(
        s"spark.akka.frameSize should not be greater than $AKKA_MAX_FRAME_SIZE_IN_MB MB")
    }
    frameSizeInMB * 1024 * 1024 //参数单位是M,将其转换成字节  
  }

  /** Space reserved for extra data in an Akka message besides serialized task or task result.
    * 除了序列化任务或任务结果之外，还为Akka消息中的额外数据保留的空间
    **/
  val reservedSizeBytes = 200 * 1024

  /**
   * Send a message to the given actor and get its result within a default timeout, or
   * throw a SparkException if this fails.
   * 向给定的actor发送message,在单位时间内采用future模式获取返回值,
   * 在尝试N次后依然失败.则抛异常
   */
  def askWithReply[T](
      message: Any,
      actor: ActorRef,
      timeout: RpcTimeout): T = {
    askWithReply[T](message, actor, maxAttempts = 1, retryInterval = Int.MaxValue, timeout)
  }

  /**
   * Send a message to the given actor and get its result within a default timeout, or
   * throw a SparkException if this fails even after the specified number of retries.
   * 向给定的actor发送message,在单位时间内采用future模式获取返回值,
   * 在尝试N次后依然失败.则抛异常
   */
  def askWithReply[T](
      message: Any,//要发送的信息
      actor: ActorRef,//发送给哪个Actor
      maxAttempts: Int,//最大尝试次数
      retryInterval: Long,//尝试间隔
      timeout: RpcTimeout): T = {//超时时间  T表示返回值
    // TODO: Consider removing multiple attempts
    if (actor == null) {//不知道发向哪个actor服务器,因此抛异常
      throw new SparkException(s"Error sending message [message = $message]" +
        " as actor is null ")
    }
    var attempts = 0 //已经尝试发送过几次
    var lastException: Exception = null //记录上一次异常
    while (attempts < maxAttempts) {//说明尝试次数尚未使用完
      attempts += 1
      try {
        val future = actor.ask(message)(timeout.duration) //发送message信息.并且等待future结果
        val result = timeout.awaitResult(future)//在单位时间内等待结果
        if (result == null) {//说明结果没有等到,则抛异常
          throw new SparkException("Actor returned null")
        }
        return result.asInstanceOf[T]//说明结果已经等到,则结束
      } catch {
        case ie: InterruptedException => throw ie //真的有异常了,抛出异常
        case e: Exception =>
          lastException = e //记录上一次异常,并且打印信息,继续循环尝试
          logWarning(s"Error sending message [message = $message] in $attempts attempts", e)
      }
      if (attempts < maxAttempts) {//休息一阵子
        Thread.sleep(retryInterval)
      }
    }

    //尝试N次后依然失败,则发送最后一个接受到的异常信息
    throw new SparkException(
      s"Error sending message [message = $message]", lastException)
  }

  //获取远程driver的ActorRef对象
  def makeDriverRef(name: String, conf: SparkConf, actorSystem: ActorSystem): ActorRef = {
    val driverActorSystemName = SparkEnv.driverActorSystemName//driver指定的Actor系统名字
    val driverHost: String = conf.get("spark.driver.host", "localhost")
    val driverPort: Int = conf.getInt("spark.driver.port", 7077)
    Utils.checkHost(driverHost, "Expected hostname")//断言driverHost一定仅仅有host,不能有port
    val url = address(protocol(actorSystem), driverActorSystemName, driverHost, driverPort, name)
    val timeout = RpcUtils.lookupRpcTimeout(conf)
    logInfo(s"Connecting to $name: $url")
    timeout.awaitResult(actorSystem.actorSelection(url).resolveOne(timeout.duration))
  }

  //生成一个服务器地址对象ActorRef,该对象是客户端需要持有的,向该对象发送信息
  def makeExecutorRef(
      name: String,//actor的name全路径
      conf: SparkConf,
      host: String,
      port: Int,
      actorSystem: ActorSystem): ActorRef = {
    val executorActorSystemName = SparkEnv.executorActorSystemName //指定的Actor系统名字
    //校验参数一定是host,不能有port
    Utils.checkHost(host, "Expected hostname")
    //生成真实交流的地址,即actor的收件箱
    val url = address(protocol(actorSystem), executorActorSystemName, host, port, name)//生成类似akka://systemName@host:port/user/actorName格式的akka的path
    //返回连接服务器的超时时间
    val timeout = RpcUtils.lookupRpcTimeout(conf)
    logInfo(s"Connecting to $name: $url") //打印连接该url对应的Actor
    //创建服务器对象
    timeout.awaitResult(actorSystem.actorSelection(url).resolveOne(timeout.duration))//可以找到url对应的Actor的代理对象
  }

  //获取协议名称akka.ssl.tcp或者akka.tcp
  def protocol(actorSystem: ActorSystem): String = {
    val akkaConf = actorSystem.settings.config
    val sslProp = "akka.remote.netty.tcp.enable-ssl"
    protocol(akkaConf.hasPath(sslProp) && akkaConf.getBoolean(sslProp))
  }

  //获取协议名称akka.ssl.tcp或者akka.tcp
  def protocol(ssl: Boolean = false): String = {
    if (ssl) {
      "akka.ssl.tcp"
    } else {
      "akka.tcp"
    }
  }

  //生成真实交流的地址,即actor的收件箱
  //例如 akka://systemName@host:port/user/actorName
  def address(
      protocol: String,
      systemName: String,
      host: String,
      port: Int,
      actorName: String): String = {
    s"$protocol://$systemName@$host:$port/user/$actorName"
  }

}
