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

package org.apache.spark.scheduler

import java.io.{FileInputStream, InputStream}
import java.util.{NoSuchElementException, Properties}

import scala.xml.XML

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.util.Utils

/**
 * An interface to build Schedulable tree
 * buildPools: build the tree nodes(pools)
 * addTaskSetManager: build the leaf nodes(TaskSetManagers)
 */
private[spark] trait SchedulableBuilder {
  def rootPool: Pool //获取Root的Pool

  def buildPools() //创建Pool

  def addTaskSetManager(manager: Schedulable, properties: Properties) //将Schedulable添加到某个非叶子队列节点(Pool)上
}

private[spark] class FIFOSchedulableBuilder(val rootPool: Pool)
  extends SchedulableBuilder with Logging {

  override def buildPools() {
    // nothing
  }

  override def addTaskSetManager(manager: Schedulable, properties: Properties) {
    rootPool.addSchedulable(manager)
  }
}

/**
 * xml格式
 * <root>
 *  <pool @name="">
 *    <schedulingMode>FIFO</schedulingMode>
 *    <minShare>5</minShare>  即该队列上最少也要有这些个task在运行
 *    <weight>6</weight> 权重值,可以理解成他是百分比,但是不需要分配成100%
 *  </pool>
 *  <pool @name="">
 *    <schedulingMode>FIFO</schedulingMode>
 *    <minShare>5</minShare>
 *    <weight>6</weight>
 *  </pool>
 * </root>
 */
private[spark] class FairSchedulableBuilder(val rootPool: Pool, conf: SparkConf)
  extends SchedulableBuilder with Logging {

  val schedulerAllocFile = conf.getOption("spark.scheduler.allocation.file") //调度配置文件
  val DEFAULT_SCHEDULER_FILE = "fairscheduler.xml" //默认的调度配置文件
    
  val FAIR_SCHEDULER_PROPERTIES = "spark.scheduler.pool"//调度的name对应的key,即该任务分配到哪个队列上
  val DEFAULT_POOL_NAME = "default"//必须要有name为default的Pool

  //xml节点的内容
  val POOLS_PROPERTY = "pool" //配置文件的pool节点
  val MINIMUM_SHARES_PROPERTY = "minShare"//pool节点需要的minShare
  val SCHEDULING_MODE_PROPERTY = "schedulingMode"//pool节点需要的模式
  val WEIGHT_PROPERTY = "weight"//pool节点需要的weight
  val POOL_NAME_PROPERTY = "@name"//pool节点的名称
    
  //默认值
  val DEFAULT_SCHEDULING_MODE = SchedulingMode.FIFO
  val DEFAULT_MINIMUM_SHARE = 0
  val DEFAULT_WEIGHT = 1

  //创建Pool集合,包括子Pool
  override def buildPools() {
    
    //读取调度配置文件,返回流
    var is: Option[InputStream] = None
    try {
      is = Option {
        schedulerAllocFile.map { f =>
          new FileInputStream(f) //读取配置的自定义配置文件
        }.getOrElse {
          Utils.getSparkClassLoader.getResourceAsStream(DEFAULT_SCHEDULER_FILE) //读取默认配置文件
        }
      }

      is.foreach { i => buildFairSchedulerPool(i) }//加载该配置文件对应的队列信息
    } finally {
      is.foreach(_.close())
    }

    // finally create "default" pool
    buildDefaultPool() //必须要有name为default的Pool
  }

  //必须要有name为default的Pool,如果xml文件中不存在,则该函数创建一个默认的Pool
  private def buildDefaultPool() {//加载默认的default队列
    if (rootPool.getSchedulableByName(DEFAULT_POOL_NAME) == null) {
      //通过默认值创建default队列
      val pool = new Pool(DEFAULT_POOL_NAME, DEFAULT_SCHEDULING_MODE,
        DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT)
      rootPool.addSchedulable(pool)
      logInfo("Created default pool %s, schedulingMode: %s, minShare: %d, weight: %d".format(
        DEFAULT_POOL_NAME, DEFAULT_SCHEDULING_MODE, DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT))
    }
  }

  //参数是配置文件流,对该流进行解析
  private def buildFairSchedulerPool(is: InputStream) {//读取xml配置文件,创建若干子队列
    val xml = XML.load(is) //加载xml
    for (poolNode <- (xml \\ POOLS_PROPERTY)) {//循环所有的pool标签,即根目录下有多少个一级目录

      val poolName = (poolNode \ POOL_NAME_PROPERTY).text //获取@name属性
      var schedulingMode = DEFAULT_SCHEDULING_MODE
      var minShare = DEFAULT_MINIMUM_SHARE
      var weight = DEFAULT_WEIGHT

      val xmlSchedulingMode = (poolNode \ SCHEDULING_MODE_PROPERTY).text //获取schedulingMode的值
      if (xmlSchedulingMode != "") {
        try {
          schedulingMode = SchedulingMode.withName(xmlSchedulingMode)
        } catch {
          case e: NoSuchElementException =>
            logWarning("Error xml schedulingMode, using default schedulingMode")
        }
      }

      val xmlMinShare = (poolNode \ MINIMUM_SHARES_PROPERTY).text //获取minShare的值
      if (xmlMinShare != "") {
        minShare = xmlMinShare.toInt
      }

      val xmlWeight = (poolNode \ WEIGHT_PROPERTY).text //获取weight的值
      if (xmlWeight != "") {
        weight = xmlWeight.toInt
      }

      //根据配置文件,生成Pool对象
      val pool = new Pool(poolName, schedulingMode, minShare, weight)
      rootPool.addSchedulable(pool)
      logInfo("Created pool %s, schedulingMode: %s, minShare: %d, weight: %d".format(
        poolName, schedulingMode, minShare, weight))
    }
  }

  //在默认的某个名称的Pool上添加manager
  override def addTaskSetManager(manager: Schedulable, properties: Properties) {
    
    //获取默认的default名称的Pool
    var poolName = DEFAULT_POOL_NAME
    var parentPool = rootPool.getSchedulableByName(poolName)
    
    if (properties != null) {
      poolName = properties.getProperty(FAIR_SCHEDULER_PROPERTIES, DEFAULT_POOL_NAME) //获取该任务分配到哪个队列上
      parentPool = rootPool.getSchedulableByName(poolName) //找到队列
      if (parentPool == null) {//创建一个队列池,其实我觉得更应该抛异常,因为属于客户端都连分配的池子都写错了,这样乱改会影响池子的结构的
        // we will create a new pool that user has configured in app
        // instead of being defined in xml file
        parentPool = new Pool(poolName, DEFAULT_SCHEDULING_MODE,
          DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT)
        rootPool.addSchedulable(parentPool)
        logInfo("Created pool %s, schedulingMode: %s, minShare: %d, weight: %d".format(
          poolName, DEFAULT_SCHEDULING_MODE, DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT))
      }
    }
    
    parentPool.addSchedulable(manager) //为该队列分配一个任务调度
    logInfo("Added task set " + manager.name + " tasks to pool " + poolName)
  }
}
