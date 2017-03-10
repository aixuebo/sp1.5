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

import java.{lang => jl}

import scala.collection.JavaConversions._

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


/**
 * :: Experimental ::
 * Functionality for working with missing data in [[DataFrame]]s.
 *
 * @since 1.3.1
 * 该类持有一个DF,因此是对DF的进一步过滤 填充操作
 * 主要对数据的null和Nan如何处理
 */
@Experimental
final class DataFrameNaFunctions private[sql](df: DataFrame) {

  /**
   * Returns a new [[DataFrame]] that drops rows containing any null or NaN values.
   *
   * @since 1.3.1
   * 只要有一列是null,则抛弃该列
   */
  def drop(): DataFrame = drop("any", df.columns)

  /**
   * Returns a new [[DataFrame]] that drops rows containing null or NaN values.
   *
   * If `how` is "any", then drop rows containing any null or NaN values.
   * If `how` is "all", then drop rows only if every column is null or NaN for that row.
   *
   * @since 1.3.1
   * 对所有列进行考察
   */
  def drop(how: String): DataFrame = drop(how, df.columns)

  /**
   * Returns a new [[DataFrame]] that drops rows containing any null or NaN values
   * in the specified columns.
   *
   * @since 1.3.1
   * 重要有一个列是null,则就抛弃
   */
  def drop(cols: Array[String]): DataFrame = drop(cols.toSeq)

  /**
   * (Scala-specific) Returns a new [[DataFrame]] that drops rows containing any null or NaN values
   * in the specified columns.
   *
   * @since 1.3.1
   * 重要有一个列是null,则就抛弃
   */
  def drop(cols: Seq[String]): DataFrame = drop(cols.size, cols)

  /**
   * Returns a new [[DataFrame]] that drops rows containing null or NaN values
   * in the specified columns.
   *
   * If `how` is "any", then drop rows containing any null or NaN values in the specified columns.
   * If `how` is "all", then drop rows only if every specified column is null or NaN for that row.
   *
   * @since 1.3.1
   */
  def drop(how: String, cols: Array[String]): DataFrame = drop(how, cols.toSeq)

  /**
   * (Scala-specific) Returns a new [[DataFrame]] that drops rows containing null or NaN values
   * in the specified columns.
   *
   * If `how` is "any", then drop rows containing any null or NaN values in the specified columns.
   * If `how` is "all", then drop rows only if every specified column is null or NaN for that row.
   *
   * @since 1.3.1
   */
  def drop(how: String, cols: Seq[String]): DataFrame = {
    how.toLowerCase match {
      case "any" => drop(cols.size, cols) //任意一列是null,则都抛弃,因此至少要有所有列,即cols.size
      case "all" => drop(1, cols) //所有的属性都是null,才将这行抛弃掉,1就表示只要有一个非null的属性,我都选择要,因此只有全部都是null的时候才会丢弃
      case _ => throw new IllegalArgumentException(s"how ($how) must be 'any' or 'all'")
    }
  }

  /**
   * Returns a new [[DataFrame]] that drops rows containing
   * less than `minNonNulls` non-null and non-NaN values.
   *
   * @since 1.3.1
   * 没设置列集合,则说明所有列都考虑
   */
  def drop(minNonNulls: Int): DataFrame = drop(minNonNulls, df.columns)

  /**
   * Returns a new [[DataFrame]] that drops rows containing
   * less than `minNonNulls` non-null and non-NaN values in the specified columns.
   *
   * @since 1.3.1
   */
  def drop(minNonNulls: Int, cols: Array[String]): DataFrame = drop(minNonNulls, cols.toSeq)

  /**
   * (Scala-specific) Returns a new [[DataFrame]] that drops rows containing less than
   * `minNonNulls` non-null and non-NaN values in the specified columns.
   * 返回一个新的DataFrame,在原来的DataFrame上进行过滤,丢弃掉一些行
   * 在给定的属性集合中,只要满足多于minNonNulls数量的非null,非Nan,则就继续保留数据
   * @since 1.3.1
   */
  def drop(minNonNulls: Int, cols: Seq[String]): DataFrame = {
    // Filtering condition:
    // only keep the row if it has at least `minNonNulls` non-null and non-NaN values.
    //创建过滤的表达式,表达式至少有多少个非null,非Nan的数据,我们就要这行数据
    val predicate = AtLeastNNonNulls(minNonNulls, cols.map(name => df.resolve(name)))
    df.filter(Column(predicate)) //对原有数据进行过滤
  }

  /**
   * Returns a new [[DataFrame]] that replaces null or NaN values in numeric columns with `value`.
   * 只是会替换值为null或者Nan的数字列,替换成value,如果该数字列不是Nan或者null,则不会被替换
   * @since 1.3.1
   */
  def fill(value: Double): DataFrame = fill(value, df.columns)

  /**
   * Returns a new [[DataFrame]] that replaces null values in string columns with `value`.
   *
   * @since 1.3.1
   */
  def fill(value: String): DataFrame = fill(value, df.columns)

  /**
   * Returns a new [[DataFrame]] that replaces null or NaN values in specified numeric columns.
   * If a specified column is not a numeric column, it is ignored.
   *
   * @since 1.3.1
   */
  def fill(value: Double, cols: Array[String]): DataFrame = fill(value, cols.toSeq)

  /**
   * (Scala-specific) Returns a new [[DataFrame]] that replaces null or NaN values in specified
   * numeric columns. If a specified column is not a numeric column, it is ignored.
   *
   * @since 1.3.1
   * 返回新的 DataFrame,如果列不是数字的,则忽略该列,即该列不被更改,如果是数字列,则填充一个默认值value
   *
   * 注意:只是会替换值为null或者Nan的数字列,替换成value,如果该数字列不是Nan或者null,则不会被替换
   */
  def fill(value: Double, cols: Seq[String]): DataFrame = {
    val columnEquals = df.sqlContext.analyzer.resolver //如何分别两个列是相同的对象

    //新的select属性集合
    val projections = df.schema.fields.map { f => //循环所有的属性
      // Only fill if the column is part of the cols list.
      if (f.dataType.isInstanceOf[NumericType] && cols.exists(col => columnEquals(f.name, col))) { //如果属性是数字类型 && 该属性是要填充的属性
        fillCol[Double](f, value) //为该属性设置一个值,注意:只是会替换值为null或者Nan的数字列,替换成value,如果该数字列不是Nan或者null,则不会被替换
      } else {
        df.col(f.name) //说明不是需要填充的属性,则默认还是该属性
      }
    }
    df.select(projections : _*)
  }

  /**
   * Returns a new [[DataFrame]] that replaces null values in specified string columns.
   * If a specified column is not a string column, it is ignored.
   *
   * @since 1.3.1
   * 为String类型的属性填充默认值
   * 注意:只是会替换值为null或者Nan的数字列,替换成value,如果该String列不是Nan或者null,则不会被替换
   */
  def fill(value: String, cols: Array[String]): DataFrame = fill(value, cols.toSeq)

  /**
   * (Scala-specific) Returns a new [[DataFrame]] that replaces null values in
   * specified string columns. If a specified column is not a string column, it is ignored.
   *
   * @since 1.3.1
   * 注意:只是会替换值为null或者Nan的数字列,替换成value,如果该String列不是Nan或者null,则不会被替换
   */
  def fill(value: String, cols: Seq[String]): DataFrame = {
    val columnEquals = df.sqlContext.analyzer.resolver //如何分别两个列是相同的对象
    val projections = df.schema.fields.map { f => //循环每一个列
      // Only fill if the column is part of the cols list.
      if (f.dataType.isInstanceOf[StringType] && cols.exists(col => columnEquals(f.name, col))) { //如果属性是String类型 && 该属性是要填充的属性
        fillCol[String](f, value) //对该列填充新值,注意:只是会替换值为null或者Nan的数字列,替换成value,如果该String列不是Nan或者null,则不会被替换
      } else {
        df.col(f.name) //保持原有列
      }
    }
    df.select(projections : _*)
  }

  /**
   * Returns a new [[DataFrame]] that replaces null values.
   *
   * The key of the map is the column name, and the value of the map is the replacement value.
   * The value must be of the following type: `Integer`, `Long`, `Float`, `Double`, `String`.
   *
   * For example, the following replaces null values in column "A" with string "unknown", and
   * null values in column "B" with numeric value 1.0.
   * {{{
   *   import com.google.common.collect.ImmutableMap;
   *   df.na.fill(ImmutableMap.of("A", "unknown", "B", 1.0));
   * }}}
   *
   * @since 1.3.1
   * 传入一个Map,key是要更改的属性,value是值,如果属性在df中的值是Nan或者null,则设置成参数对应的value
   */
  def fill(valueMap: java.util.Map[String, Any]): DataFrame = fill0(valueMap.toSeq)

  /**
   * (Scala-specific) Returns a new [[DataFrame]] that replaces null values.
   *
   * The key of the map is the column name, and the value of the map is the replacement value.
   * The value must be of the following type: `Int`, `Long`, `Float`, `Double`, `String`.
   *
   * For example, the following replaces null values in column "A" with string "unknown", and
   * null values in column "B" with numeric value 1.0.
   * {{{
   *   df.na.fill(Map(
   *     "A" -> "unknown",
   *     "B" -> 1.0
   *   ))
   * }}}
   *
   * @since 1.3.1
   * 传入一个Map,key是要更改的属性,value是值,如果属性在df中的值是Nan或者null,则设置成参数对应的value
   */
  def fill(valueMap: Map[String, Any]): DataFrame = fill0(valueMap.toSeq)

  /**
   * Replaces values matching keys in `replacement` map with the corresponding values.
   * Key and value of `replacement` map must have the same type, and can only be doubles or strings.
   * If `col` is "*", then the replacement is applied on all string columns or numeric columns.
   *
   * {{{
   *   import com.google.common.collect.ImmutableMap;
   *
   *   // Replaces all occurrences of 1.0 with 2.0 in column "height".
   *   df.replace("height", ImmutableMap.of(1.0, 2.0));
   *
   *   // Replaces all occurrences of "UNKNOWN" with "unnamed" in column "name".
   *   df.replace("name", ImmutableMap.of("UNKNOWN", "unnamed"));
   *
   *   // Replaces all occurrences of "UNKNOWN" with "unnamed" in all string columns.
   *   df.replace("*", ImmutableMap.of("UNKNOWN", "unnamed"));
   * }}}
   *
   * @param col name of the column to apply the value replacement
   * @param replacement value replacement map, as explained above
   *
   * @since 1.3.1
   * 对一个列进行替换,该列的值与map中的key相同的时候,则替换成对应的value,否则保持原来数据
   */
  def replace[T](col: String, replacement: java.util.Map[T, T]): DataFrame = {
    replace[T](col, replacement.toMap : Map[T, T])
  }

  /**
   * Replaces values matching keys in `replacement` map with the corresponding values.
   * Key and value of `replacement` map must have the same type, and can only be doubles or strings.
   *
   * {{{
   *   import com.google.common.collect.ImmutableMap;
   *
   *   // Replaces all occurrences of 1.0 with 2.0 in column "height" and "weight".
   *   df.replace(new String[] {"height", "weight"}, ImmutableMap.of(1.0, 2.0));
   *
   *   // Replaces all occurrences of "UNKNOWN" with "unnamed" in column "firstname" and "lastname".
   *   df.replace(new String[] {"firstname", "lastname"}, ImmutableMap.of("UNKNOWN", "unnamed"));
   * }}}
   *
   * @param cols list of columns to apply the value replacement
   * @param replacement value replacement map, as explained above
   *
   * @since 1.3.1
   * 对一组列都进行替换,该列的值与map中的key相同的时候,则替换成对应的value,否则保持原来数据
   */
  def replace[T](cols: Array[String], replacement: java.util.Map[T, T]): DataFrame = {
    replace(cols.toSeq, replacement.toMap)
  }

  /**
   * (Scala-specific) Replaces values matching keys in `replacement` map.
   * Key and value of `replacement` map must have the same type, and can only be doubles or strings.
   * If `col` is "*", then the replacement is applied on all string columns or numeric columns.
   *
   * {{{
   *   // Replaces all occurrences of 1.0 with 2.0 in column "height".
   *   df.replace("height", Map(1.0 -> 2.0))
   *
   *   // Replaces all occurrences of "UNKNOWN" with "unnamed" in column "name".
   *   df.replace("name", Map("UNKNOWN" -> "unnamed")
   *
   *   // Replaces all occurrences of "UNKNOWN" with "unnamed" in all string columns.
   *   df.replace("*", Map("UNKNOWN" -> "unnamed")
   * }}}
   *
   * @param col name of the column to apply the value replacement
   * @param replacement value replacement map, as explained above
   *
   * @since 1.3.1
   * 对一个列进行替换,该列的值与map中的key相同的时候,则替换成对应的value,否则保持原来数据
   */
  def replace[T](col: String, replacement: Map[T, T]): DataFrame = {
    if (col == "*") {
      replace0(df.columns, replacement) //表示对全部列进行替换
    } else {
      replace0(Seq(col), replacement)
    }
  }

  /**
   * (Scala-specific) Replaces values matching keys in `replacement` map.
   * Key and value of `replacement` map must have the same type, and can only be doubles or strings.
   *
   * {{{
   *   // Replaces all occurrences of 1.0 with 2.0 in column "height" and "weight".
   *   df.replace("height" :: "weight" :: Nil, Map(1.0 -> 2.0));
   *
   *   // Replaces all occurrences of "UNKNOWN" with "unnamed" in column "firstname" and "lastname".
   *   df.replace("firstname" :: "lastname" :: Nil, Map("UNKNOWN" -> "unnamed");
   * }}}
   *
   * @param cols list of columns to apply the value replacement
   * @param replacement value replacement map, as explained above
   *
   * @since 1.3.1
   * 对一组列进行替换,该列的值与map中的key相同的时候,则替换成对应的value,否则保持原来数据
   */
  def replace[T](cols: Seq[String], replacement: Map[T, T]): DataFrame = replace0(cols, replacement)

  private def replace0[T](cols: Seq[String], replacement: Map[T, T]): DataFrame = {
    if (replacement.isEmpty || cols.isEmpty) {
      return df
    }

    // replacementMap is either Map[String, String] or Map[Double, Double]
    //replacement.head._2 表示获取第一条记录中的value,根据value的类型可以进行转换
    val replacementMap: Map[_, _] = replacement.head._2 match {
      case v: String => replacement //说明传递的就是String类型的
      case _ => replacement.map { case (k, v) => (convertToDouble(k), convertToDouble(v)) } //将原始的map转换成double类型的map
    }

    // targetColumnType is either DoubleType or StringType
    ///replacement.head._1 表示获取第一条记录中的key
    val targetColumnType = replacement.head._1 match {
      case _: jl.Double | _: jl.Float | _: jl.Integer | _: jl.Long => DoubleType
      case _: String => StringType
    }

    val columnEquals = df.sqlContext.analyzer.resolver //判断属性是否相同的对象
    val projections = df.schema.fields.map { f => //循环所有的属性
      val shouldReplace = cols.exists(colName => columnEquals(colName, f.name)) //判断属性是否要进行替换
      if (f.dataType.isInstanceOf[NumericType] && targetColumnType == DoubleType && shouldReplace) { //属性是数字的,代替的值是double的,应该替换.因此则替换
        replaceCol(f, replacementMap)
      } else if (f.dataType == targetColumnType && shouldReplace) { //类型相同也可以替换
        replaceCol(f, replacementMap)
      } else {
        df.col(f.name) //保留原始
      }
    }
    df.select(projections : _*)
  }

  /**
   * 传入一个元组集合,元组由(属性,value)组成
   * 1.如果df中的name不在传入的元组属性中.则保留df该列不变
   * 2.如果df的name在传入的属性中.则判断df的列值是否是null或者Nan,如果是则替换成传入的value,如果不是null,则保留原有列数据
   */
  private def fill0(values: Seq[(String, Any)]): DataFrame = {

    //数据校验
    // Error handling
    values.foreach { case (colName, replaceValue) => //循环所有的列和对应的值
      // Check column name exists
      df.resolve(colName) //校验列的名字

      // Check data type
      replaceValue match { //校验值的类型
        case _: jl.Double | _: jl.Float | _: jl.Integer | _: jl.Long | _: String =>
          // This is good
        case _ => throw new IllegalArgumentException(
          s"Unsupported value type ${replaceValue.getClass.getName} ($replaceValue).")
      }
    }

    val columnEquals = df.sqlContext.analyzer.resolver //判断两列是否相同的对象
    val projections = df.schema.fields.map { f => //循环所有的属性
      values.find { case (k, _) => columnEquals(k, f.name) } //循环所有的values,k表示value中的name,判断两个name是否相同
        .map { case (_, v) =>
            v match { //对null或者Nan的数据进行替换
              case v: jl.Float => fillCol[Double](f, v.toDouble)
              case v: jl.Double => fillCol[Double](f, v)
              case v: jl.Long => fillCol[Double](f, v.toDouble)
              case v: jl.Integer => fillCol[Double](f, v.toDouble)
              case v: String => fillCol[String](f, v)
            }
      }.getOrElse(df.col(f.name)) //找到与属性相同的name,则进行替换,没有找到,则保持原有属性
    }
    df.select(projections : _*)
  }

  /**
   * Returns a [[Column]] expression that replaces null value in `col` with `replacement`.
   * 只是将null值替换成新的值,非null值不会被替换
   * 为一个属性设置一个新的值,该值的类型是T,即是String或者数字等等,返回新的列
   */
  private def fillCol[T](col: StructField, replacement: T): Column = {
    col.dataType match {
      case DoubleType | FloatType =>
        //最后强制选择第一个非null的值
        coalesce(nanvl(df.col("`" + col.name + "`"), lit(null)),//非Nan函数
          lit(replacement).cast(col.dataType)).as(col.name) //将代替的内容转换成Double类型,然后设置别名为原始名字
      case _ =>
        coalesce(df.col("`" + col.name + "`"), lit(replacement).cast(col.dataType)).as(col.name) //将代替的内容转换成String类型,然后设置别名为原始名字
    }
  }

  /**
   * Returns a [[Column]] expression that replaces value matching key in `replacementMap` with
   * value in `replacementMap`, using [[CaseWhen]].
   *
   * TODO: This can be optimized to use broadcast join when replacementMap is large.
   * 对一个列进行替换,该列的值与map中的key相同的时候,则替换成对应的value,否则保持原来数据
   */
  private def replaceCol(col: StructField, replacementMap: Map[_, _]): Column = {
    val keyExpr = df.col(col.name).expr //找到列的表达式
    def buildExpr(v: Any) = Cast(Literal(v), keyExpr.dataType) //对v进行强转换成列对应的返回值

    val branches = replacementMap.flatMap { case (source, target) =>
      Seq(buildExpr(source), buildExpr(target)) //对原始map转换成元组,
    }.toSeq
    new Column(CaseKeyWhen(keyExpr, branches :+ keyExpr)).as(col.name) //转换成case 表达式 when 表达式 then 表达式 else 表达式 end 操作.即key表达式的结果,当key的值满足的时候,则替换成对应的值
  }

  //将数据转换成double
  private def convertToDouble(v: Any): Double = v match {
    case v: Float => v.toDouble
    case v: Double => v
    case v: Long => v.toDouble
    case v: Int => v.toDouble
    case v => throw new IllegalArgumentException(
      s"Unsupported value type ${v.getClass.getName} ($v).")
  }
}
