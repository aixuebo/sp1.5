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

import java.net.URI
import java.util.concurrent.TimeoutException

import scala.concurrent.{Awaitable, Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.util.{RpcUtils, Utils}


/**
 * A RpcEnv implementation must have a [[RpcEnvFactory]] implementation with an empty constructor
 * so that it can be created via Reflection.
 * 一个RPC的环境 是采用工场模式创建的,工场必须是有一个空的构造函数,因为他要被用于反射机制
 */
private[spark] object RpcEnv {

  //通过配置返回RPC环境工场实例
  private def getRpcEnvFactory(conf: SparkConf): RpcEnvFactory = {
    // Add more RpcEnv implementations here
    //RPC环境工场,采用Akka框架实现
    //该Map设置了全部的RCP工场实现类关联关系
    val rpcEnvNames = Map("akka" -> "org.apache.spark.rpc.akka.AkkaRpcEnvFactory")
    //该配置获取要选择哪种实现方式,默认是akka工场实现
    val rpcEnvName = conf.get("spark.rpc", "akka")
    //获取RCP环境工场类实现,默认获取org.apache.spark.rpc.akka.AkkaRpcEnvFactory
    val rpcEnvFactoryClassName = rpcEnvNames.getOrElse(rpcEnvName.toLowerCase, rpcEnvName)
    //对RPC工场创建实例
    Utils.classForName(rpcEnvFactoryClassName).newInstance().asInstanceOf[RpcEnvFactory]
  }

  def create(
      name: String,//名称
      host: String,//RPC服务器所在host
      port: Int,//RPC服务器所在port
      conf: SparkConf,
      securityManager: SecurityManager): RpcEnv = {
    // Using Reflection to create the RpcEnv to avoid to depend on Akka directly
    val config = RpcEnvConfig(conf, name, host, port, securityManager) //生成服务器端的配置对象
    getRpcEnvFactory(conf).create(config) //通过RCO工场实例,创建一个RPC环境对象
  }

}


/**
 * An RPC environment. [[RpcEndpoint]]s need to register itself with a name to [[RpcEnv]] to
 * receives messages. Then [[RpcEnv]] will process messages sent from [[RpcEndpointRef]] or remote
 * nodes, and deliver them to corresponding [[RpcEndpoint]]s. For uncaught exceptions caught by
 * [[RpcEnv]], [[RpcEnv]] will use [[RpcCallContext.sendFailure]] to send exceptions back to the
 * sender, or logging them if no such sender or `NotSerializableException`.
 * RpcEndpoint 需要去注册他自己(使用一个name)到RpcEnv上,去接受信息,然后RpcEnv去处理从RpcEndpointRef或者远程节点信息,并且分发他们去关联的RpcEndpoint。
 * [[RpcEnv]] also provides some methods to retrieve [[RpcEndpointRef]]s given name or uri.
 * 详细参见AkkaRpcEnv实现类
 */
private[spark] abstract class RpcEnv(conf: SparkConf) {

  //默认超时时间
  private[spark] val defaultLookupTimeout = RpcUtils.lookupRpcTimeout(conf)

  /**
   * Return RpcEndpointRef of the registered [[RpcEndpoint]]. Will be used to implement
   * [[RpcEndpoint.self]]. Return `null` if the corresponding [[RpcEndpointRef]] does not exist.
   */
  private[rpc] def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef

  /**
   * Return the address that [[RpcEnv]] is listening to.
   * 创建默认的PRC服务器地址
   */
  def address: RpcAddress

  /**
   * Register a [[RpcEndpoint]] with a name and return its [[RpcEndpointRef]]. [[RpcEnv]] does not
   * guarantee thread-safety.
   * 返回已经建立好的连接
   */
  def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef

  /**
   * Retrieve the [[RpcEndpointRef]] represented by `uri` asynchronously.
   */
  def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef]

  /**
   * Retrieve the [[RpcEndpointRef]] represented by `uri`. This is a blocking action.
   */
  def setupEndpointRefByURI(uri: String): RpcEndpointRef = {
    defaultLookupTimeout.awaitResult(asyncSetupEndpointRefByURI(uri))
  }

  /**
   * Retrieve the [[RpcEndpointRef]] represented by `systemName`, `address` and `endpointName`
   * asynchronously.
   */
  def asyncSetupEndpointRef(
      systemName: String, address: RpcAddress, endpointName: String): Future[RpcEndpointRef] = {
    asyncSetupEndpointRefByURI(uriOf(systemName, address, endpointName))
  }

  /**
   * Retrieve the [[RpcEndpointRef]] represented by `systemName`, `address` and `endpointName`.
   * This is a blocking action.
   * 与address建立一个连接通道,即调用者要与address去连接
   */
  def setupEndpointRef(
      systemName: String, address: RpcAddress, endpointName: String): RpcEndpointRef = {
    setupEndpointRefByURI(uriOf(systemName, address, endpointName))
  }

  /**
   * Stop [[RpcEndpoint]] specified by `endpoint`.
   */
  def stop(endpoint: RpcEndpointRef): Unit

  /**
   * Shutdown this [[RpcEnv]] asynchronously. If need to make sure [[RpcEnv]] exits successfully,
   * call [[awaitTermination()]] straight after [[shutdown()]].
   */
  def shutdown(): Unit

  /**
   * Wait until [[RpcEnv]] exits.
   *
   * TODO do we need a timeout parameter?
   */
  def awaitTermination(): Unit

  /**
   * Create a URI used to create a [[RpcEndpointRef]]. Use this one to create the URI instead of
   * creating it manually because different [[RpcEnv]] may have different formats.
   */
  def uriOf(systemName: String, address: RpcAddress, endpointName: String): String

  /**
   * [[RpcEndpointRef]] cannot be deserialized without [[RpcEnv]]. So when deserializing any object
   * that contains [[RpcEndpointRef]]s, the deserialization codes should be wrapped by this method.
   */
  def deserialize[T](deserializationAction: () => T): T
}

//表示一个RPC服务器配置
private[spark] case class RpcEnvConfig(
    conf: SparkConf,
    name: String,//actorSystem的Name
    host: String,//RPC服务器所在host
    port: Int,//RPC服务器所在port
    securityManager: SecurityManager)


/**
 * Represents a host and port.
 * 表示一个PRC服务器的地址
 */
private[spark] case class RpcAddress(host: String, port: Int) {
  // TODO do we need to add the type of RpcEnv in the address?

  val hostPort: String = host + ":" + port

  override val toString: String = hostPort

  //spark形式的url
  def toSparkURL: String = "spark://" + hostPort
}


//表示一个PRC服务器的地址
private[spark] object RpcAddress {

  /**
   * Return the [[RpcAddress]] represented by `uri`.
   */
  def fromURI(uri: URI): RpcAddress = {
    RpcAddress(uri.getHost, uri.getPort)
  }

  /**
   * Return the [[RpcAddress]] represented by `uri`.
   */
  def fromURIString(uri: String): RpcAddress = {
    fromURI(new java.net.URI(uri))
  }

  /**
   * 参数是spark的url格式,例如spark://host:port
   * 从这里面获取host和port
   */
  def fromSparkURL(sparkUrl: String): RpcAddress = {
    val (host, port) = Utils.extractHostPortFromSparkUrl(sparkUrl)
    RpcAddress(host, port)
  }
}


/**
 * An exception thrown if RpcTimeout modifies a [[TimeoutException]].
 */
private[rpc] class RpcTimeoutException(message: String, cause: TimeoutException)
  extends TimeoutException(message) { initCause(cause) }


/**
 * Associates a timeout with a description so that a when a TimeoutException occurs, additional
 * context about the timeout can be amended to the exception message.
 * @param duration timeout duration in seconds 超额时间,单位second
 * @param timeoutProp the configuration property that controls this timeout 在配置文件中的key
 */
private[spark] class RpcTimeout(val duration: FiniteDuration, val timeoutProp: String)
  extends Serializable {

  /** Amends the standard message of TimeoutException to include the description
    * 如果超时的时候,打印异常信息
    * 该异常信息内提示了超时的key是timeoutProp
    **/
  private def createRpcTimeoutException(te: TimeoutException): RpcTimeoutException = {
    new RpcTimeoutException(te.getMessage() + ". This timeout is controlled by " + timeoutProp, te)
  }

  /**
   * PartialFunction to match a TimeoutException and add the timeout description to the message
   * 定义偏函数,只能匹配TimeoutException和RpcTimeoutException异常
   * @note This can be used in the recover callback of a Future to add to a TimeoutException
   * Example:
   *    val timeout = new RpcTimeout(5 millis, "short timeout")
   *    Future(throw new TimeoutException).recover(timeout.addMessageIfTimeout)
   *    T就是异常对象
   */
  def addMessageIfTimeout[T]: PartialFunction[Throwable, T] = {
    // The exception has already been converted to a RpcTimeoutException so just raise it
    case rte: RpcTimeoutException => throw rte
    // Any other TimeoutException get converted to a RpcTimeoutException with modified message
    case te: TimeoutException => throw createRpcTimeoutException(te)
  }

  /**
   * Wait for the completed result and return it. If the result is not available within this
   * timeout, throw a [[RpcTimeoutException]] to indicate which configuration controls the timeout.
   * @param  awaitable  the `Awaitable` to be awaited
   * @throws RpcTimeoutException if after waiting for the specified time `awaitable`
   *         is still not ready
   *  等候完成这个结果,并且返回该结果,这个结果必须在超时范围内完成,否则会抛异常
   */
  def awaitResult[T](awaitable: Awaitable[T]): T = {
    try {
      Await.result(awaitable, duration)
    } catch addMessageIfTimeout
  }
}


private[spark] object RpcTimeout {

  /**
   * Lookup the timeout property in the configuration and create
   * a RpcTimeout with the property key in the description.
   * @param conf configuration properties containing the timeout
   * @param timeoutProp property key for the timeout in seconds
   * @throws NoSuchElementException if property is not set
   * 从timeoutProp作为key,查询对应的值
   */
  def apply(conf: SparkConf, timeoutProp: String): RpcTimeout = {
    val timeout = { conf.getTimeAsSeconds(timeoutProp) seconds }
    new RpcTimeout(timeout, timeoutProp)
  }

  /**
   * Lookup the timeout property in the configuration and create
   * a RpcTimeout with the property key in the description.
   * Uses the given default value if property is not set
   * @param conf configuration properties containing the timeout
   * @param timeoutProp property key for the timeout in seconds
   * @param defaultValue default timeout value in seconds if property not found
   * 从timeoutProp作为key,查询对应的值,如果查询不到执行默认值
   */
  def apply(conf: SparkConf, timeoutProp: String, defaultValue: String): RpcTimeout = {
    val timeout = { conf.getTimeAsSeconds(timeoutProp, defaultValue) seconds }
    new RpcTimeout(timeout, timeoutProp)
  }

  /**
   * Lookup prioritized list of timeout properties in the configuration
   * and create a RpcTimeout with the first set property key in the
   * description.
   * Uses the given default value if property is not set
   * @param conf configuration properties containing the timeout
   * @param timeoutPropList prioritized list of property keys for the timeout in seconds
   * @param defaultValue default timeout value in seconds if no properties found
   * 从key集合中查找对应的值
   */
  def apply(conf: SparkConf, timeoutPropList: Seq[String], defaultValue: String): RpcTimeout = {
    require(timeoutPropList.nonEmpty)

    // Find the first set property or use the default value with the first property
    val itr = timeoutPropList.iterator
    
    //返回值元组,存在的(key,key对应的值)
    var foundProp: Option[(String, String)] = None
    while (itr.hasNext && foundProp.isEmpty){
      val propKey = itr.next()
      conf.getOption(propKey).foreach { prop => foundProp = Some(propKey, prop) } //prop => foundProp = Some(propKey, prop) 表示循环的value值是prop,然后进入下一阶段,为foundProp赋值为Some(propKey, prop)
    }
    
    val finalProp = foundProp.getOrElse(timeoutPropList.head, defaultValue)//默认的key和value,默认的key是第一个元素
    val timeout = { Utils.timeStringAsSeconds(finalProp._2) seconds }//转换成second
    new RpcTimeout(timeout, finalProp._1)
  }
}
