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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow, analysis}
import org.apache.spark.sql.types.{StructField, StructType}

//表示本地的一个数据集合
object LocalRelation {

  //根据输出的属性集合,创建空的表,即没有数据的表---只有schema
  def apply(output: Attribute*): LocalRelation = new LocalRelation(output)

  //根据输出的属性集合,创建空的表,即没有数据的表
  def apply(output1: StructField, output: StructField*): LocalRelation = {
    new LocalRelation(StructType(output1 +: output).toAttributes)
  }

  //根据输出的属性集合,创建对应的schema,将数据转换成内部的InternalRow
  def fromExternalRows(output: Seq[Attribute], data: Seq[Row]): LocalRelation = {
    val schema = StructType.fromAttributes(output) //根据输出的属性集合,创建对应的schema
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
    LocalRelation(output, data.map(converter(_).asInstanceOf[InternalRow]))
  }

  def fromProduct(output: Seq[Attribute], data: Seq[Product]): LocalRelation = {
    val schema = StructType.fromAttributes(output)
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
    LocalRelation(output, data.map(converter(_).asInstanceOf[InternalRow]))
  }
}

//本地的一个表,第一个参数是表的输出schema,第二个属性是表内数据集合
case class LocalRelation(output: Seq[Attribute], data: Seq[InternalRow] = Nil)
  extends LeafNode with analysis.MultiInstanceRelation {

  /**
   * Returns an identical copy of this relation with new exprIds for all attributes.  Different
   * attributes are required when a relation is going to be included multiple times in the same
   * query.
   * 根据out的属性集合,创建一个本地表
   */
  override final def newInstance(): this.type = {
    LocalRelation(output.map(_.newInstance()), data).asInstanceOf[this.type]
  }

  override protected def stringArgs = Iterator(output)

  //是否相同,主要看输出的数据类型是否相同 && 数据内容是否相同
  override def sameResult(plan: LogicalPlan): Boolean = plan match {
    case LocalRelation(otherOutput, otherData) =>
      otherOutput.map(_.dataType) == output.map(_.dataType) && otherData == data
    case _ => false
  }

  //输出所占字节大小.即输出字段类型的字节大小之和 * 数据行数
  override lazy val statistics =
    Statistics(sizeInBytes = output.map(_.dataType.defaultSize).sum * data.length)
}
