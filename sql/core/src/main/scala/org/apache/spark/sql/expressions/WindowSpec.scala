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

package org.apache.spark.sql.expressions

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.{Column, catalyst}
import org.apache.spark.sql.catalyst.expressions._


/**
 * :: Experimental ::
 * A window specification that defines the partitioning, ordering, and frame boundaries.
 *
 * Use the static methods in [[Window]] to create a [[WindowSpec]].
 *
 * @since 1.4.0
 网上demo
 import sys
from pyspark.sql.window import Window
import pyspark.sql.functions as func
windowSpec = \
  Window
    .partitionBy(df['category']) \
    .orderBy(df['revenue'].desc()) \
    .rangeBetween(-sys.maxsize, sys.maxsize)
dataFrame = sqlContext.table("productRevenue")
revenue_difference = \
  (func.max(dataFrame['revenue']).over(windowSpec) - dataFrame['revenue'])
dataFrame.select(
  dataFrame['product'],
  dataFrame['category'],
  dataFrame['revenue'],
  revenue_difference.alias("revenue_difference"))

 计算一个分组下最大的销售额与每一个销售额的差
 */
@Experimental
class WindowSpec private[sql](//定义一个窗口函数
    partitionSpec: Seq[Expression],//按照什么表达式去分类
    orderSpec: Seq[SortOrder],//分类中排序规则
    frame: catalyst.expressions.WindowFrame) {//该分类的组

  /**
   * Defines the partitioning columns in a [[WindowSpec]].
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def partitionBy(colName: String, colNames: String*): WindowSpec = {
    partitionBy((colName +: colNames).map(Column(_)): _*) //将若干个列进行组装成集合
  }

  /**
   * Defines the partitioning columns in a [[WindowSpec]].
   * @since 1.4.0
   * 按照若干个列进行分组
   */
  @scala.annotation.varargs
  def partitionBy(cols: Column*): WindowSpec = {
    new WindowSpec(cols.map(_.expr), orderSpec, frame)
  }

  /**
   * Defines the ordering columns in a [[WindowSpec]].
   * @since 1.4.0
   * 按照若干个列排序
   */
  @scala.annotation.varargs
  def orderBy(colName: String, colNames: String*): WindowSpec = {
    orderBy((colName +: colNames).map(Column(_)): _*)
  }

  /**
   * Defines the ordering columns in a [[WindowSpec]].
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def orderBy(cols: Column*): WindowSpec = {
    val sortOrder: Seq[SortOrder] = cols.map { col =>
      col.expr match {
        case expr: SortOrder => //说明本身就是order by xxx desc 这种全结构的语法
          expr
        case expr: Expression =>
          SortOrder(expr, Ascending) //默认升序排序
      }
    }
    new WindowSpec(partitionSpec, sortOrder, frame)
  }

  /**
   * Defines the frame boundaries, from `start` (inclusive) to `end` (inclusive).
   *
   * Both `start` and `end` are relative positions from the current row. For example, "0" means
   * "current row", while "-1" means the row before the current row, and "5" means the fifth row
   * after the current row.
   *
   * @param start boundary start, inclusive.
   *              The frame is unbounded if this is the minimum long value.
   * @param end boundary end, inclusive.
   *            The frame is unbounded if this is the maximum long value.
   * @since 1.4.0
   */
  def rowsBetween(start: Long, end: Long): WindowSpec = {
    between(RowFrame, start, end)
  }

  /**
   * Defines the frame boundaries, from `start` (inclusive) to `end` (inclusive).
   *
   * Both `start` and `end` are relative from the current row. For example, "0" means "current row",
   * while "-1" means one off before the current row, and "5" means the five off after the
   * current row.
   *
   * @param start boundary start, inclusive.
   *              The frame is unbounded if this is the minimum long value.
   * @param end boundary end, inclusive.
   *            The frame is unbounded if this is the maximum long value.
   * @since 1.4.0
   */
  def rangeBetween(start: Long, end: Long): WindowSpec = {
    between(RangeFrame, start, end)
  }

  private def between(typ: FrameType, start: Long, end: Long): WindowSpec = {
    val boundaryStart = start match {
      case 0 => CurrentRow
      case Long.MinValue => UnboundedPreceding
      case x if x < 0 => ValuePreceding(-start.toInt) //向前查start个
      case x if x > 0 => ValueFollowing(start.toInt) //向后若干个
    }

    val boundaryEnd = end match {
      case 0 => CurrentRow
      case Long.MaxValue => UnboundedFollowing
      case x if x < 0 => ValuePreceding(-end.toInt)
      case x if x > 0 => ValueFollowing(end.toInt)
    }

    new WindowSpec(
      partitionSpec,
      orderSpec,
      SpecifiedWindowFrame(typ, boundaryStart, boundaryEnd))
  }

  /**
   * Converts this [[WindowSpec]] into a [[Column]] with an aggregate expression.
   * 对应hive的语法是SUM(pv) OVER(PARTITION BY cookieid ORDER BY createtime ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) 表示当前行+往前3行的值进行sum,表示对窗口函数周期内的数据进行sum
   * 首先参数列就是一个聚合函数列,即sum(pv),只是该sum后面接的是一个聚合函数窗口
   */
  private[sql] def withAggregate(aggregate: Column): Column = {
    val windowExpr = aggregate.expr match {
      case Average(child) => WindowExpression(
        UnresolvedWindowFunction("avg", child :: Nil),
        WindowSpecDefinition(partitionSpec, orderSpec, frame))
      case Sum(child) => WindowExpression(
        UnresolvedWindowFunction("sum", child :: Nil),
        WindowSpecDefinition(partitionSpec, orderSpec, frame))
      case Count(child) => WindowExpression(
        UnresolvedWindowFunction("count", child :: Nil),
        WindowSpecDefinition(partitionSpec, orderSpec, frame))
      case First(child) => WindowExpression(
        // TODO this is a hack for Hive UDAF first_value
        UnresolvedWindowFunction("first_value", child :: Nil),
        WindowSpecDefinition(partitionSpec, orderSpec, frame))
      case Last(child) => WindowExpression(
        // TODO this is a hack for Hive UDAF last_value
        UnresolvedWindowFunction("last_value", child :: Nil),
        WindowSpecDefinition(partitionSpec, orderSpec, frame))
      case Min(child) => WindowExpression(
        UnresolvedWindowFunction("min", child :: Nil),
        WindowSpecDefinition(partitionSpec, orderSpec, frame))
      case Max(child) => WindowExpression(
        UnresolvedWindowFunction("max", child :: Nil),
        WindowSpecDefinition(partitionSpec, orderSpec, frame))
      case wf: WindowFunction => WindowExpression(
        wf,
        WindowSpecDefinition(partitionSpec, orderSpec, frame))
      case x =>
        throw new UnsupportedOperationException(s"$x is not supported in window operation.")
    }
    new Column(windowExpr)
  }

}
