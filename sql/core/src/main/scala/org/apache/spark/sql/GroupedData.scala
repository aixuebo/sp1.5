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

package org.apache.spark.sql

import scala.collection.JavaConversions._
import scala.language.implicitConversions

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute, Star}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Rollup, Cube, Aggregate}
import org.apache.spark.sql.types.NumericType

/**
 * Companion object for GroupedData
 * GroupedData的伴随对象
 */
private[sql] object GroupedData {
  def apply(
      df: DataFrame,//group by 应用在哪个df上
      groupingExprs: Seq[Expression],//group by表达式集合
      groupType: GroupType) //group by的方式
    : GroupedData = {
    new GroupedData(df, groupingExprs, groupType: GroupType)
  }

  /**
   * The Grouping Type
   */
  private[sql] trait GroupType

  /**
   * To indicate it's the GroupBy
   * 标准group by操作
   */
  private[sql] object GroupByType extends GroupType

  /**
   * To indicate it's the CUBE
   * 窗口函数
   */
  private[sql] object CubeType extends GroupType

  /**
   * To indicate it's the ROLLUP
   * 窗口函数
   */
  private[sql] object RollupType extends GroupType
}

/**
 * :: Experimental ::
 * A set of methods for aggregations on a [[DataFrame]], created by [[DataFrame.groupBy]].
 *
 * @since 1.3.0
 */
@Experimental
class GroupedData protected[sql](
    df: DataFrame,
    groupingExprs: Seq[Expression],
    private val groupType: GroupedData.GroupType) {

  //参数是select中要聚合的表达式集合
  private[this] def toDF(aggExprs: Seq[Expression]): DataFrame = {
    //最终聚合属性是group by的表达式与新的agg表达式之和
    val aggregates = if (df.sqlContext.conf.dataFrameRetainGroupColumns) { //是否保留group的属性
      groupingExprs ++ aggExprs //即group by的内容 + select的内容之和作为表达式集合
    } else {
      aggExprs
    }

    val aliasedAgg = aggregates.map {
      // Wrap UnresolvedAttribute with UnresolvedAlias, as when we resolve UnresolvedAttribute, we
      // will remove intermediate Alias for ExtractValue chain, and we need to alias it again to
      // make it a NamedExpression.
      case u: UnresolvedAttribute => UnresolvedAlias(u)
      case expr: NamedExpression => expr //有别名的表达式
      case expr: Expression => Alias(expr, expr.prettyString)() //为表达式起一个别名
    }
    groupType match {
      case GroupedData.GroupByType =>
        DataFrame(
          df.sqlContext, Aggregate(groupingExprs, aliasedAgg, df.logicalPlan)) //对group by进行包装,逻辑计划重新包装了一层
      case GroupedData.RollupType =>
        DataFrame(
          df.sqlContext, Rollup(groupingExprs, df.logicalPlan, aliasedAgg))
      case GroupedData.CubeType =>
        DataFrame(
          df.sqlContext, Cube(groupingExprs, df.logicalPlan, aliasedAgg))
    }
  }

  //对整数列进行函数包装,转换成新的表达式.
  //比如对age,date两个字段都获取max,因此select就变成max(age),max(date)了
  private[this] def aggregateNumericColumns(colNames: String*)(f: Expression => Expression)
    : DataFrame = {

    //将名字转换成属性集合
    val columnExprs = if (colNames.isEmpty) {//如果没传参数
      // No columns specified. Use all numeric columns.
      df.numericColumns //查找所有的数组类型的属性对应的表达式
    } else {
      // Make sure all specified columns are numeric.
      colNames.map { colName =>
        val namedExpr = df.resolve(colName) //将列名字转换成列属性对象
        if (!namedExpr.dataType.isInstanceOf[NumericType]) {
          throw new AnalysisException(
            s""""$colName" is not a numeric column. """ +
            "Aggregation function can only be applied on a numeric column.")
        }
        namedExpr
      }
    }
    toDF(columnExprs.map(f))//每一个属性都进行f函数处理,转换成新的属性
  }

  //将表达式字符串转换成表达式--返回值是一个表达式参数传入,返回一个新的表达式
  private[this] def strToExpr(expr: String): (Expression => Expression) = {
    expr.toLowerCase match {
      case "avg" | "average" | "mean" => Average
      case "max" => Max
      case "min" => Min
      case "sum" => Sum
      case "count" | "size" =>
        // Turn count(*) into count(1)
        (inputExpr: Expression) => inputExpr match {
          case s: Star => Count(Literal(1)) //如果表达式是*,则将其转换成1
          case _ => Count(inputExpr) //说明count里面追加的是一个表达式,比如distinct表达式
        }
    }
  }

  /**
   * (Scala-specific) Compute aggregates by specifying a map from column name to
   * aggregate methods. The resulting [[DataFrame]] will also contain the grouping columns.
   *
   * The available aggregate methods are `avg`, `max`, `min`, `sum`, `count`.
   * {{{
   *   // Selects the age of the oldest employee and the aggregate expense for each department
   *   df.groupBy("department").agg(
   *     "age" -> "max",
   *     "expense" -> "sum"
   *   )
   * }}}
   *
   * @since 1.3.0
   * 参数表示最少有一个表达式,第二个参数表示可以有若干个表达式
   */
  def agg(aggExpr: (String, String), aggExprs: (String, String)*): DataFrame = {
    agg((aggExpr +: aggExprs).toMap)
  }

  /**
   * (Scala-specific) Compute aggregates by specifying a map from column name to
   * aggregate methods. The resulting [[DataFrame]] will also contain the grouping columns.
   *
   * The available aggregate methods are `avg`, `max`, `min`, `sum`, `count`.
   * {{{
   *   // Selects the age of the oldest employee and the aggregate expense for each department
   *   df.groupBy("department").agg(Map(
   *     "age" -> "max",
   *     "expense" -> "sum"
   *   ))
   *   说明针对department进行分组,每一个组内对age进行max函数处理,因此是一个元组(aga,max)
   * }}}
   *
   * @since 1.3.0
   */
  def agg(exprs: Map[String, String]): DataFrame = {
    /**
     * 说明:
     * 1.每一个字段以及对应的表达式,比如age,max
     * 2.strToExpr(expr)将其表达式转换成表达式对象,比如Max
     * 3.表达式对象需要参数,参数就是df(colName).expr,即df(colName)返回具体的列对象Column,然后调用该列对象的expr返回该列的具体内容,该列一定是一个表达式,最简单的表达式也就是一个name名字的表达式,或者复杂的计算结果表达式
     * 4.对这些表达式集合进行group by处理
     */
    toDF(exprs.map { case (colName, expr) => //属性名字 和 聚合的表达式
      strToExpr(expr)(df(colName).expr) //strToExpr(expr)将表达式转换成 (Expression => Expression)形式聚合函数,然后将属性本身作为参数,应用到函数中,返回解析后的函数
    }.toSeq)
  }

  /**
   * (Java-specific) Compute aggregates by specifying a map from column name to
   * aggregate methods. The resulting [[DataFrame]] will also contain the grouping columns.
   *
   * The available aggregate methods are `avg`, `max`, `min`, `sum`, `count`.
   * {{{
   *   // Selects the age of the oldest employee and the aggregate expense for each department
   *   import com.google.common.collect.ImmutableMap;
   *   df.groupBy("department").agg(ImmutableMap.of("age", "max", "expense", "sum"));
   * }}}
   *
   * @since 1.3.0
   * 如果聚合函数想按照max(age),sum(expense),则可以写成java的map形式,即key是age,value是max这样
   * 或者ImmutableMap.of("age", "max", "expense", "sum")
   */
  def agg(exprs: java.util.Map[String, String]): DataFrame = {
    agg(exprs.toMap)
  }

  /**
   * Compute aggregates by specifying a series of aggregate columns. Note that this function by
   * default retains the grouping columns in its output. To not retain grouping columns, set
   * `spark.sql.retainGroupColumns` to false.
   *
   * The available aggregate methods are defined in [[org.apache.spark.sql.functions]].
   *
   * {{{
   *   // Selects the age of the oldest employee and the aggregate expense for each department
   *
   *   // Scala:
   *   import org.apache.spark.sql.functions._
   *   df.groupBy("department").agg(max("age"), sum("expense"))
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   df.groupBy("department").agg(max("age"), sum("expense"));
   * }}}
   *
   * Note that before Spark 1.4, the default behavior is to NOT retain grouping columns. To change
   * to that behavior, set config variable `spark.sql.retainGroupColumns` to `false`.
   * {{{
   *   // Scala, 1.3.x:
   *   df.groupBy("department").agg($"department", max("age"), sum("expense"))
   *
   *   // Java, 1.3.x:
   *   df.groupBy("department").agg(col("department"), max("age"), sum("expense"));
   * }}}
   *
   * @since 1.3.0
   * 添加至少一个表达式,第二个参数表示可以增加多个表达式
   * 参数不再是
   * 比如Column 表示一个列,该列上可以追加表达式,例如 max("age")
   * df.groupBy("department").agg($"department", max("age"), sum("expense"))
   * 表示对department进行分组了,因此select就可以有department列,以及其他两个列的聚合函数
   */
  @scala.annotation.varargs
  def agg(expr: Column, exprs: Column*): DataFrame = {
    toDF((expr +: exprs).map(_.expr))
  }

  /**
   * Count the number of rows for each group.
   * The resulting [[DataFrame]] will also contain the grouping columns.
   *
   * @since 1.3.0
   * 增加count聚合
   * 即对整个group by的结果只是追加了一个select count(1)的表达式,表达式的name就是count
   */
  def count(): DataFrame = toDF(Seq(Alias(Count(Literal(1)), "count")()))

  /**
   * Compute the average value for each numeric columns for each group. This is an alias for `avg`.
   * The resulting [[DataFrame]] will also contain the grouping columns.
   * When specified columns are given, only compute the average values for them.
   *
   * @since 1.3.0
   * 对若干个列都进行同一种函数
   */
  @scala.annotation.varargs
  def mean(colNames: String*): DataFrame = {
    aggregateNumericColumns(colNames : _*)(Average)
  }

  /**
   * Compute the max value for each numeric columns for each group.
   * The resulting [[DataFrame]] will also contain the grouping columns.
   * When specified columns are given, only compute the max values for them.
   *
   * @since 1.3.0
   * 计算参数集合中每一个属性的最大值
   * 对若干个列都进行同一种函数
   */
  @scala.annotation.varargs
  def max(colNames: String*): DataFrame = {
    aggregateNumericColumns(colNames : _*)(Max)
  }

  /**
   * Compute the mean value for each numeric columns for each group.
   * The resulting [[DataFrame]] will also contain the grouping columns.
   * When specified columns are given, only compute the mean values for them.
   *
   * @since 1.3.0
   * 对若干个列都进行同一种函数
   */
  @scala.annotation.varargs
  def avg(colNames: String*): DataFrame = {
    aggregateNumericColumns(colNames : _*)(Average)
  }

  /**
   * Compute the min value for each numeric column for each group.
   * The resulting [[DataFrame]] will also contain the grouping columns.
   * When specified columns are given, only compute the min values for them.
   *
   * @since 1.3.0
   * 对若干个列都进行同一种函数
   */
  @scala.annotation.varargs
  def min(colNames: String*): DataFrame = {
    aggregateNumericColumns(colNames : _*)(Min)
  }

  /**
   * Compute the sum for each numeric columns for each group.
   * The resulting [[DataFrame]] will also contain the grouping columns.
   * When specified columns are given, only compute the sum for them.
   *
   * @since 1.3.0
   * 对若干个列都进行同一种函数
   */
  @scala.annotation.varargs
  def sum(colNames: String*): DataFrame = {
    aggregateNumericColumns(colNames : _*)(Sum)
  }
}
