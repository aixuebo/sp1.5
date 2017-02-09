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

package org.apache.spark.sql.execution.datasources.jdbc

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, DataSourceRegister}

class DefaultSource extends RelationProvider with DataSourceRegister {

  override def shortName(): String = "jdbc"

  /** Returns a new base relation with the given parameters. */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val url = parameters.getOrElse("url", sys.error("Option 'url' not specified"))
    val driver = parameters.getOrElse("driver", null)
    val table = parameters.getOrElse("dbtable", sys.error("Option 'dbtable' not specified"))

    val partitionColumn = parameters.getOrElse("partitionColumn", null) //拆分partition的字段
    val lowerBound = parameters.getOrElse("lowerBound", null) //从什么地方开始拆分
    val upperBound = parameters.getOrElse("upperBound", null) //到什么地方拆分为止
    val numPartitions = parameters.getOrElse("numPartitions", null) //拆分成多少个partition

    if (driver != null) DriverRegistry.register(driver) //注册一个driver

    if (partitionColumn != null
      && (lowerBound == null || upperBound == null || numPartitions == null)) {
      sys.error("Partitioning incompletely specified")
    }

    val partitionInfo = if (partitionColumn == null) {
      null
    } else {
      JDBCPartitioningInfo(
        partitionColumn,
        lowerBound.toLong,
        upperBound.toLong,
        numPartitions.toInt)
    }
    val parts = JDBCRelation.columnPartition(partitionInfo) //分区
    val properties = new Properties() // Additional properties that we will pass to getConnection
    parameters.foreach(kv => properties.setProperty(kv._1, kv._2)) //创建Properties对象

    JDBCRelation(url, table, parts, properties)(sqlContext)
  }
}
