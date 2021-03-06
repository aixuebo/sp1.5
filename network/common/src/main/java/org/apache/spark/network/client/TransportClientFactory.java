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

package org.apache.spark.network.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.TransportContext;
import org.apache.spark.network.server.TransportChannelHandler;
import org.apache.spark.network.util.IOMode;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;

/**
 * Factory for creating {@link TransportClient}s by using createClient.
 *
 * The factory maintains a connection pool to other hosts and should return the same
 * TransportClient for the same remote host. It also shares a single worker thread pool for
 * all TransportClients.
 *
 * TransportClients will be reused whenever possible. Prior to completing the creation of a new
 * TransportClient, all given {@link TransportClientBootstrap}s will be run.
 */
public class TransportClientFactory implements Closeable {

  /** A simple data structure to track the pool of clients between two peer nodes. 
   * 一个简单的线程池,每一个ip:port对应一个该线程池
   * 线程池的size数量由conf.numConnectionsPerPeer()决定
   *
   * 即该节点发送任何的服务器,都对应一个该对象,每一个服务器对应一个该对象
   * 该对象包含连接到该服务器的若干个客户端
   **/
  private static class ClientPool {
    TransportClient[] clients;//若干个客户端,任何一个客户端都可以连接到该服务器
    Object[] locks;//每一个客户端持有一个锁,用于共享操作

    public ClientPool(int size) {
      clients = new TransportClient[size];
      locks = new Object[size];
      for (int i = 0; i < size; i++) {
        locks[i] = new Object();
      }
    }
  }

  private final Logger logger = LoggerFactory.getLogger(TransportClientFactory.class);

  private final TransportContext context;
  private final TransportConf conf;
  private final List<TransportClientBootstrap> clientBootstraps;//拦截器集合,当客户端创建成功后,依次处理这些拦截器
  //每一个ip:port对应一个线程池,key是ip:port value是线程池ClientPool
  private final ConcurrentHashMap<SocketAddress, ClientPool> connectionPool;

  /** Random number generator for picking connections between peers.
   * 随机数,随机从 ClientPool连接池中获取一个连接,随机数从numConnectionsPerPeer中随机选择一个
   **/
  private final Random rand;
  private final int numConnectionsPerPeer;//每一个ip:port对应一个该线程池,线程池的size数量由conf.numConnectionsPerPeer()决定

  private final Class<? extends Channel> socketChannelClass;
  private EventLoopGroup workerGroup;
  private PooledByteBufAllocator pooledAllocator;

  public TransportClientFactory(
      TransportContext context,
      List<TransportClientBootstrap> clientBootstraps) {
    this.context = Preconditions.checkNotNull(context);
    this.conf = context.getConf();
    this.clientBootstraps = Lists.newArrayList(Preconditions.checkNotNull(clientBootstraps));
    
    this.connectionPool = new ConcurrentHashMap<SocketAddress, ClientPool>();//每一个ip:port对应一个该线程池
    this.numConnectionsPerPeer = conf.numConnectionsPerPeer();//每一个ip:port对应一个该线程池,线程池的size数量由conf.numConnectionsPerPeer()决定
    this.rand = new Random();

    IOMode ioMode = IOMode.valueOf(conf.ioMode());
    this.socketChannelClass = NettyUtils.getClientChannelClass(ioMode);
    // TODO: Make thread pool name configurable.
    this.workerGroup = NettyUtils.createEventLoop(ioMode, conf.clientThreads(), "shuffle-client");
    this.pooledAllocator = NettyUtils.createPooledByteBufAllocator(
      conf.preferDirectBufs(), false /* allowCache */, conf.clientThreads());
  }

  /**
   * Create a {@link TransportClient} connecting to the given remote host / port.
   *
   * We maintains an array of clients (size determined by spark.shuffle.io.numConnectionsPerPeer)
   * and randomly picks one to use. If no client was previously created in the randomly selected
   * spot, this function creates a new client and places it there.
   *
   * Prior to the creation of a new TransportClient, we will execute all
   * {@link TransportClientBootstrap}s that are registered with this factory.
   *
   * This blocks until a connection is successfully established and fully bootstrapped.
   *
   * Concurrency: This method is safe to call from multiple threads.
   * 创建一个与ip:port的连接,可以从缓存中获取该连接
   */
  public TransportClient createClient(String remoteHost, int remotePort) throws IOException {
    // Get connection from the connection pool first.
    // If it is not found or not active, create a new one.
    final InetSocketAddress address = new InetSocketAddress(remoteHost, remotePort);

    // Create the ClientPool if we don't have it yet.获取连接该ip:port的连接池
    ClientPool clientPool = connectionPool.get(address);
    if (clientPool == null) {
      connectionPool.putIfAbsent(address, new ClientPool(numConnectionsPerPeer));
      clientPool = connectionPool.get(address);
    }

    //从连接池中随机选择一个下标
    int clientIndex = rand.nextInt(numConnectionsPerPeer);
    TransportClient cachedClient = clientPool.clients[clientIndex];

    //返回该缓存的客户端连接
    if (cachedClient != null && cachedClient.isActive()) {
      logger.trace("Returning cached connection to {}: {}", address, cachedClient);
      return cachedClient;
    }

    // If we reach here, we don't have an existing connection open. Let's create a new one.
    //如果代码到达这里,说明没有存在的可以的一个连接,我们需要创建一个
    // Multiple threads might race here to create new connections. Keep only one of them active.
    //可能多线程在这里竞争,因此我们需要同步锁
    synchronized (clientPool.locks[clientIndex]) {
      cachedClient = clientPool.clients[clientIndex];

      if (cachedClient != null) {
        if (cachedClient.isActive()) {
          logger.trace("Returning cached connection to {}: {}", address, cachedClient);
          return cachedClient;
        } else {
          logger.info("Found inactive connection to {}, creating a new one.", address);
        }
      }
      //真正创建一个连接,并且将其存储到内存映射中
      clientPool.clients[clientIndex] = createClient(address);
      return clientPool.clients[clientIndex];
    }
  }

  /** Create a completely new {@link TransportClient} to the remote address.
   * 真正创建一个客户端连接,连接到ip:port服务器中 
   **/
  private TransportClient createClient(InetSocketAddress address) throws IOException {
    logger.debug("Creating new connection to " + address);

    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(workerGroup)
      .channel(socketChannelClass)
      // Disable Nagle's Algorithm since we don't want packets to wait
      .option(ChannelOption.TCP_NODELAY, true)
      .option(ChannelOption.SO_KEEPALIVE, true)
      .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, conf.connectionTimeoutMs())
      .option(ChannelOption.ALLOCATOR, pooledAllocator);

    final AtomicReference<TransportClient> clientRef = new AtomicReference<TransportClient>();
    final AtomicReference<Channel> channelRef = new AtomicReference<Channel>();

    //当连接到服务器的通道成功后,则调用该方法
    bootstrap.handler(new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) {//连接到服务器的通道
        TransportChannelHandler clientHandler = context.initializePipeline(ch);
        clientRef.set(clientHandler.getClient());//设置连接到服务器的客户端
        channelRef.set(ch);//设置连接到服务器的通道
      }
    });

    // Connect to the remote server
    long preConnect = System.nanoTime();
    ChannelFuture cf = bootstrap.connect(address);
    if (!cf.awaitUninterruptibly(conf.connectionTimeoutMs())) {
      throw new IOException(
        String.format("Connecting to %s timed out (%s ms)", address, conf.connectionTimeoutMs()));
    } else if (cf.cause() != null) {
      throw new IOException(String.format("Failed to connect to %s", address), cf.cause());
    }

    TransportClient client = clientRef.get();//获取客户端对象
    Channel channel = channelRef.get();//获取连接服务器后的通道对象
    assert client != null : "Channel future completed successfully with null client";

    //依次执行每一个拦截器
    // Execute any client bootstraps synchronously before marking the Client as successful.
    long preBootstrap = System.nanoTime();
    logger.debug("Connection to {} successful, running bootstraps...", address);
    try {
      for (TransportClientBootstrap clientBootstrap : clientBootstraps) {
        clientBootstrap.doBootstrap(client, channel);
      }
    } catch (Exception e) { // catch non-RuntimeExceptions too as bootstrap may be written in Scala
      long bootstrapTimeMs = (System.nanoTime() - preBootstrap) / 1000000;
      logger.error("Exception while bootstrapping client after " + bootstrapTimeMs + " ms", e);
      client.close();
      throw Throwables.propagate(e);
    }
    long postBootstrap = System.nanoTime();

    logger.debug("Successfully created connection to {} after {} ms ({} ms spent in bootstraps)",
      address, (postBootstrap - preConnect) / 1000000, (postBootstrap - preBootstrap) / 1000000);

    return client;
  }

  /** Close all connections in the connection pool, and shutdown the worker thread pool. 
   * 关闭所有的缓存的客户端连接 
   **/
  @Override
  public void close() {
    // Go through all clients and close them if they are active.
    for (ClientPool clientPool : connectionPool.values()) {
      for (int i = 0; i < clientPool.clients.length; i++) {
        TransportClient client = clientPool.clients[i];
        if (client != null) {
          clientPool.clients[i] = null;
          JavaUtils.closeQuietly(client);
        }
      }
    }
    connectionPool.clear();

    if (workerGroup != null) {
      workerGroup.shutdownGracefully();
      workerGroup = null;
    }
  }
}
