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

package org.apache.spark.deploy.history

import java.util.zip.ZipOutputStream

import org.apache.spark.SparkException
import org.apache.spark.ui.SparkUI

//用于表示一个app的尝试任务
private[spark] case class ApplicationAttemptInfo(
    attemptId: Option[String],//尝试任务的ID
    startTime: Long,//尝试任务的开始时间以及结束时间
    endTime: Long,
    lastUpdated: Long,
    sparkUser: String,
    completed: Boolean = false) //是否完成的该尝试任务

//表示一个app任务
private[spark] case class ApplicationHistoryInfo(
    id: String,
    name: String,
    attempts: List[ApplicationAttemptInfo])

//提供的接口服务
private[history] abstract class ApplicationHistoryProvider {

  /**
   * Returns a list of applications available for the history server to show.
   *
   * @return List of all know applications.
   * 返回所有的app历史集合
   */
  def getListing(): Iterable[ApplicationHistoryInfo]

  /**
   * Returns the Spark UI for a specific application.
   * 返回历史上某一个app的尝试任务的连接
   * @param appId The application ID.
   * @param attemptId The application attempt ID (or None if there is no attempt ID).
   * @return The application's UI, or None if application is not found.
   */
  def getAppUI(appId: String, attemptId: Option[String]): Option[SparkUI]

  /**
   * Called when the server is shutting down.
   * 关闭历史服务
   */
  def stop(): Unit = { }

  /**
   * Returns configuration data to be shown in the History Server home page.
   *
   * @return A map with the configuration data. Data is show in the order returned by the map.
   */
  def getConfig(): Map[String, String] = Map()

  /**
   * Writes out the event logs to the output stream provided. The logs will be compressed into a
   * single zip file and written out.
   * @throws SparkException if the logs for the app id cannot be found.
   * 将事件日志写入到输出流中,该输出流是被压缩的zip流
   */
  @throws(classOf[SparkException])
  def writeEventLogs(appId: String, attemptId: Option[String], zipStream: ZipOutputStream): Unit

}
