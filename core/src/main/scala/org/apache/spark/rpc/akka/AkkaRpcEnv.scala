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

package org.apache.spark.rpc.akka

import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.Future
import scala.language.postfixOps
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import akka.actor.{ActorSystem, ExtendedActorSystem, Actor, ActorRef, Props, Address}
import akka.event.Logging.Error
import akka.pattern.{ask => akkaAsk}
import akka.remote.{AssociationEvent, AssociatedEvent, DisassociatedEvent, AssociationErrorEvent}
import akka.serialization.JavaSerializer

import org.apache.spark.{SparkException, Logging, SparkConf}
import org.apache.spark.rpc._
import org.apache.spark.util.{ActorLogReceive, AkkaUtils, ThreadUtils}

/**
 * A RpcEnv implementation based on Akka.
 * 基于Akka方式实现PRC环境工场
 * TODO Once we remove all usages of Akka in other place, we can move this file to a new project and
 * remove Akka from the dependencies.
 *
 * @param actorSystem 服务器真实绑定的host以及提供的服务
 * @param conf
 * @param boundPort 服务器真实绑定的port
 * 代表一个服务器
 */
private[spark] class AkkaRpcEnv private[akka] (
    val actorSystem: ActorSystem, conf: SparkConf, boundPort: Int)
  extends RpcEnv(conf) with Logging {

  //创建默认的PRC服务器地址
  private val defaultAddress: RpcAddress = {
    val address = actorSystem.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress //akka://UniversityMessageSystem,即打印actorSystem的名字
    // In some test case, ActorSystem doesn't bind to any address.
    // So just use some default value since they are only some unit tests
    RpcAddress(address.host.getOrElse("localhost"), address.port.getOrElse(boundPort))
  }

  //创建默认的PRC服务器地址
  override val address: RpcAddress = defaultAddress

  /**
   * A lookup table to search a [[RpcEndpointRef]] for a [[RpcEndpoint]]. We need it to make
   * [[RpcEndpoint.self]] work.
   * 连接该服务器的客户端映射关系,每一个客户端对应RpcEndpoint和RpcEndpointRef两个属性
   */
  private val endpointToRef = new ConcurrentHashMap[RpcEndpoint, RpcEndpointRef]()

  /**
   * Need this map to remove `RpcEndpoint` from `endpointToRef` via a `RpcEndpointRef`
   * 连接该服务器的客户端映射关系,每一个客户端对应RpcEndpoint和RpcEndpointRef两个属性
   */
  private val refToEndpoint = new ConcurrentHashMap[RpcEndpointRef, RpcEndpoint]()

  //添加一个客户端连接该服务器
  private def registerEndpoint(endpoint: RpcEndpoint, endpointRef: RpcEndpointRef): Unit = {
    endpointToRef.put(endpoint, endpointRef)
    refToEndpoint.put(endpointRef, endpoint)
  }

  //解除一个客户端与该服务器的连接
  private def unregisterEndpoint(endpointRef: RpcEndpointRef): Unit = {
    val endpoint = refToEndpoint.remove(endpointRef)
    if (endpoint != null) {
      endpointToRef.remove(endpoint)
    }
  }

  /**
   * Retrieve the [[RpcEndpointRef]] of `endpoint`.
   * 查找客户端
   */
  override def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef = endpointToRef.get(endpoint)

  //向endpoint发送请求,创建endpoint的引用
  override def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef = {
    @volatile var endpointRef: AkkaRpcEndpointRef = null
    // Use lazy because the Actor needs to use `endpointRef`.
    // So `actorRef` should be created after assigning `endpointRef`.
    //对客户端RpcEndpoint封装一个本地引用Actor,里面包含客户端RpcEndpoint对象
    lazy val actorRef = actorSystem.actorOf(Props(new Actor with ActorLogReceive with Logging {

      assert(endpointRef != null)//客户端对象不为null

      override def preStart(): Unit = {
        // Listen for remote client network events
        context.system.eventStream.subscribe(self, classOf[AssociationEvent])
        safelyCall(endpoint) {
          endpoint.onStart()
        }
      }

      override def receiveWithLogging: Receive = {
        case AssociatedEvent(_, remoteAddress, _) =>
          safelyCall(endpoint) {
            endpoint.onConnected(akkaAddressToRpcAddress(remoteAddress))
          }

        case DisassociatedEvent(_, remoteAddress, _) =>
          safelyCall(endpoint) {
            endpoint.onDisconnected(akkaAddressToRpcAddress(remoteAddress))
          }

        case AssociationErrorEvent(cause, localAddress, remoteAddress, inbound, _) =>
          safelyCall(endpoint) {
            endpoint.onNetworkError(cause, akkaAddressToRpcAddress(remoteAddress))
          }

        case e: AssociationEvent =>
          // TODO ignore?

        case m: AkkaMessage =>
          logDebug(s"Received RPC message: $m")
          safelyCall(endpoint) {
            processMessage(endpoint, m, sender)
          }

        case AkkaFailure(e) =>
          safelyCall(endpoint) {
            throw e
          }

        case message: Any => {
          logWarning(s"Unknown message: $message")
        }

      }

      override def postStop(): Unit = {
        unregisterEndpoint(endpoint.self)
        safelyCall(endpoint) {
          endpoint.onStop()
        }
      }

      }), name = name)
    endpointRef = new AkkaRpcEndpointRef(defaultAddress, actorRef, conf, initInConstructor = false)
    registerEndpoint(endpoint, endpointRef)
    // Now actorRef can be created safely
    endpointRef.init()
    endpointRef
  }

  //发送一个信息
  private def processMessage(endpoint: RpcEndpoint, m: AkkaMessage, _sender: ActorRef): Unit = {
    val message = m.message
    val needReply = m.needReply//true表示发送者需要一个回复

    //因为receiveAndReply和receive方法返回值都是PartialFunction[Any, Unit]
    //receiveAndReply参数是一个RpcCallContext,是客户端要回复给发送端的信息,因此里面封装的都是_sender
    val pf: PartialFunction[Any, Unit] =
      if (needReply) {
        endpoint.receiveAndReply(new RpcCallContext {//里面的信息都是客户端要调用的
          override def sendFailure(e: Throwable): Unit = {
            _sender ! AkkaFailure(e)//客户端会调用_sender,让发送者收到信息
          }

          override def reply(response: Any): Unit = {
            _sender ! AkkaMessage(response, false)
          }

          // Some RpcEndpoints need to know the sender's address
          override val sender: RpcEndpointRef =
            new AkkaRpcEndpointRef(defaultAddress, _sender, conf)
        })
      } else {
        endpoint.receive
      }


    try {
      pf.applyOrElse[Any, Unit](message, { message =>
        throw new SparkException(s"Unmatched message $message from ${_sender}")
      })
    } catch {
      case NonFatal(e) =>
        _sender ! AkkaFailure(e)
        if (!needReply) {
          // If the sender does not require a reply, it may not handle the exception. So we rethrow
          // "e" to make sure it will be processed.
          throw e
        }
    }
  }

  /**
   * Run `action` safely to avoid to crash the thread. If any non-fatal exception happens, it will
   * call `endpoint.onError`. If `endpoint.onError` throws any non-fatal exception, just log it.
   */
  private def safelyCall(endpoint: RpcEndpoint)(action: => Unit): Unit = {
    try {
      action
    } catch {
      case NonFatal(e) => {
        try {
          endpoint.onError(e)
        } catch {
          case NonFatal(e) => logError(s"Ignore error: ${e.getMessage}", e)
        }
      }
    }
  }

  private def akkaAddressToRpcAddress(address: Address): RpcAddress = {
    RpcAddress(address.host.getOrElse(defaultAddress.host),
      address.port.getOrElse(defaultAddress.port))
  }

  override def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef] = {
    import actorSystem.dispatcher
    actorSystem.actorSelection(uri).resolveOne(defaultLookupTimeout.duration).
      map(new AkkaRpcEndpointRef(defaultAddress, _, conf)).
      // this is just in case there is a timeout from creating the future in resolveOne, we want the
      // exception to indicate the conf that determines the timeout
      recover(defaultLookupTimeout.addMessageIfTimeout)
  }

  override def uriOf(systemName: String, address: RpcAddress, endpointName: String): String = {
    AkkaUtils.address(
      AkkaUtils.protocol(actorSystem), systemName, address.host, address.port, endpointName)
  }

  //关闭服务器服务
  override def shutdown(): Unit = {
    actorSystem.shutdown()
  }

  override def stop(endpoint: RpcEndpointRef): Unit = {
    require(endpoint.isInstanceOf[AkkaRpcEndpointRef])
    actorSystem.stop(endpoint.asInstanceOf[AkkaRpcEndpointRef].actorRef)
  }

  override def awaitTermination(): Unit = {
    actorSystem.awaitTermination()
  }

  override def toString: String = s"${getClass.getSimpleName}($actorSystem)"

  override def deserialize[T](deserializationAction: () => T): T = {
    JavaSerializer.currentSystem.withValue(actorSystem.asInstanceOf[ExtendedActorSystem]) {
      deserializationAction()
    }
  }
}

//RPC环境工场,采用Akka框架实现
private[spark] class AkkaRpcEnvFactory extends RpcEnvFactory {

  def create(config: RpcEnvConfig): RpcEnv = {
    //创建Actor对象以及真实的host
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(
      config.name, config.host, config.port, config.conf, config.securityManager)
    actorSystem.actorOf(Props(classOf[ErrorMonitor]), "ErrorMonitor")
    new AkkaRpcEnv(actorSystem, config.conf, boundPort)
  }
}

/**
 * Monitor errors reported by Akka and log them.
 * 订阅Error错误信息的Actor
 */
private[akka] class ErrorMonitor extends Actor with ActorLogReceive with Logging {

  override def preStart(): Unit = { //订阅Error错误信息
    context.system.eventStream.subscribe(self, classOf[Error])
  }

  //对收到的信息进行log日志处理
  override def receiveWithLogging: Actor.Receive = {
    case Error(cause: Throwable, _, _, message: String) => logError(message, cause)
  }
}

//代表一个客户端实现类
private[akka] class AkkaRpcEndpointRef(
    @transient defaultAddress: RpcAddress,//服务器地址
    @transient _actorRef: => ActorRef,//引用的客户端Actor
    @transient conf: SparkConf,
    @transient initInConstructor: Boolean = true)
  extends RpcEndpointRef(conf) with Logging {

  lazy val actorRef = _actorRef //引用的actort对象

  //获取引用的Actor对应的ip和port
  override lazy val address: RpcAddress = {
    val akkaAddress = actorRef.path.address
    RpcAddress(akkaAddress.host.getOrElse(defaultAddress.host),
      akkaAddress.port.getOrElse(defaultAddress.port))
  }

  //引用的Actor的name
  override lazy val name: String = actorRef.path.name

  private[akka] def init(): Unit = {
    // Initialize the lazy vals
    actorRef
    address
    name
  }

  if (initInConstructor) {
    init()
  }

  //向引用的actor发送信息,信息被包装成AkkaMessage对象
  //tell是发完就走,纯粹的异步发送
  override def send(message: Any): Unit = {
    actorRef ! AkkaMessage(message, false)
  }

  //ask是发完等Future
  override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
    actorRef.ask(AkkaMessage(message, true))(timeout.duration).flatMap {
      // The function will run in the calling thread, so it should be short and never block.
      case msg @ AkkaMessage(message, reply) =>
        if (reply) {
          logError(s"Receive $msg but the sender cannot reply")
          Future.failed(new SparkException(s"Receive $msg but the sender cannot reply"))
        } else {
          Future.successful(message)
        }
      case AkkaFailure(e) =>
        Future.failed(e)
    }(ThreadUtils.sameThread).mapTo[T].
    recover(timeout.addMessageIfTimeout)(ThreadUtils.sameThread)
  }

  override def toString: String = s"${getClass.getSimpleName}($actorRef)"

  final override def equals(that: Any): Boolean = that match {
    case other: AkkaRpcEndpointRef => actorRef == other.actorRef
    case _ => false
  }

  final override def hashCode(): Int = if (actorRef == null) 0 else actorRef.hashCode()
}

/**
 * A wrapper to `message` so that the receiver knows if the sender expects a reply.
 * 对一个信息进行包装,如果发送者需要一个回复的时候,接受者要知道
 * @param message
 * @param needReply if the sender expects a reply message
 */
private[akka] case class AkkaMessage(message: Any, needReply: Boolean)

/**
 * A reply with the failure error from the receiver to the sender
 * 从接受者到发送者一个错误的回复
 */
private[akka] case class AkkaFailure(e: Throwable)
