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

package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{InternalRow, CatalystTypeConverters}
import org.apache.spark.sql.catalyst.expressions.Attribute


/**
 * Physical plan node for scanning data from a local collection.
 * 物理计划----从本地集合中扫描数据
 */
private[sql] case class LocalTableScan(
    output: Seq[Attribute],
    rows: Seq[InternalRow]) extends LeafNode {

  private lazy val rdd = sqlContext.sparkContext.parallelize(rows)

  protected override def doExecute(): RDD[InternalRow] = rdd

  //获取全部结果数据---因为是本地数据集合.因此不需要分布式去获取数据
  override def executeCollect(): Array[Row] = {
    val converter = CatalystTypeConverters.createToScalaConverter(schema) //根据输出定义的数据格式,转换成scala的格式
    rows.map(converter(_).asInstanceOf[Row]).toArray //因为知道了scala的格式,因此一行数据可以被解析成row对应的一行数据,因此组成了一组数据
  }

  //获取limit个数据
  override def executeTake(limit: Int): Array[Row] = {
    val converter = CatalystTypeConverters.createToScalaConverter(schema)
    rows.map(converter(_).asInstanceOf[Row]).take(limit).toArray
  }
}
