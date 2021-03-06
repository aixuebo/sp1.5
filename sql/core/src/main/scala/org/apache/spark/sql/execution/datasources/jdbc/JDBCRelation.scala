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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}

/**
 * Instructions on how to partition the table among workers.
 * 该对象表示如何对JDBC的数据进行拆分成partition
 */
private[sql] case class JDBCPartitioningInfo(
    column: String,//要拆分的列,根据该列进行拆分数据
    lowerBound: Long,//最大值和最小值
    upperBound: Long,
    numPartitions: Int)//拆分成多少个partition

//静态方法
private[sql] object JDBCRelation {
  /**
   * Given a partitioning schematic (a column of integral type, a number of
   * partitions, and upper and lower bounds on the column's value), generate
   * WHERE clauses for each partition so that each row in the table appears
   * exactly once.  The parameters minValue and maxValue are advisory in that
   * incorrect values may cause the partitioning to be poor, but no data
   * will fail to be represented.
   * 真正对列进行拆分,返回拆分后的partition集合
   */
  def columnPartition(partitioning: JDBCPartitioningInfo): Array[Partition] = {
    if (partitioning == null) return Array[Partition](JDBCPartition(null, 0))

    val numPartitions = partitioning.numPartitions
    val column = partitioning.column
    if (numPartitions == 1) return Array[Partition](JDBCPartition(null, 0)) //不需要where条件

    // Overflow and silliness can happen if you subtract then divide.
    // Here we get a little roundoff, but that's (hopefully) OK.
    val stride: Long = (partitioning.upperBound / numPartitions
                      - partitioning.lowerBound / numPartitions) //计算每一个区间需要多少条数据  其实应该等于(max-min)/n,不明白他为什么这么啰嗦的算,估计跟取整数有关系
    var i: Int = 0
    var currentValue: Long = partitioning.lowerBound //从最小位置开始
    var ans = new ArrayBuffer[Partition]() //分区对象集合
    while (i < numPartitions) {
      val lowerBound = if (i != 0) s"$column >= $currentValue" else null //添加where条件
      currentValue += stride
      val upperBound = if (i != numPartitions - 1) s"$column < $currentValue" else null

      //where条件
      val whereClause =
        if (upperBound == null) {
          lowerBound
        } else if (lowerBound == null) {
          upperBound
        } else {
          s"$lowerBound AND $upperBound"
        }
      ans += JDBCPartition(whereClause, i)
      i = i + 1
    }
    ans.toArray
  }
}

private[sql] case class JDBCRelation(
    url: String,
    table: String,
    parts: Array[Partition],//分区信息
    properties: Properties = new Properties())(@transient val sqlContext: SQLContext)
  extends BaseRelation
  with PrunedFilteredScan
  with InsertableRelation {

  override val needConversion: Boolean = false

  //通过url连接driver,通过table进行查询该表的表结构
  override val schema: StructType = JDBCRDD.resolveTable(url, table, properties) //返回一个表的数据结构

    /**
     * requiredColumns 即select的字段
     * filters 表示过滤条件
     * 返回一个RDD
     */
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {//返回一个rdd
    val driver: String = DriverRegistry.getDriverClassName(url) //获取driver的具体class
    // Rely on a type erasure hack to pass RDD[InternalRow] back as RDD[Row]
    JDBCRDD.scanTable(
      sqlContext.sparkContext,
      schema,
      driver,
      url,
      properties,
      table,
      requiredColumns,
      filters,
      parts).asInstanceOf[RDD[Row]]
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    data.write
      .mode(if (overwrite) SaveMode.Overwrite else SaveMode.Append)
      .jdbc(url, table, properties)
  }
}
