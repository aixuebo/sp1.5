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

import org.apache.spark.sql.catalyst.expressions.aggregate.{Complete, AggregateExpression2}
import org.apache.spark.sql.execution.aggregate.ScalaUDAF
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.types._
import org.apache.spark.annotation.Experimental

/**
 * :: Experimental ::
 * The base class for implementing user-defined aggregate functions (UDAF).
 * 如何定义一个UDAF函数
 */
@Experimental
abstract class UserDefinedAggregateFunction extends Serializable {

  /**
   * A [[StructType]] represents data types of input arguments of this aggregate function.
   * For example, if a [[UserDefinedAggregateFunction]] expects two input arguments
   * with type of [[DoubleType]] and [[LongType]], the returned [[StructType]] will look like
   *
   * ```
   *   new StructType()
   *    .add("doubleInput", DoubleType)
   *    .add("longInput", LongType)
   * ```
   *
   * The name of a field of this [[StructType]] is only used to identify the corresponding
   * input argument. Users can choose names to identify the input arguments.
   * 定义udaf的输入参数的schema
   */
  def inputSchema: StructType

  /**
   * A [[StructType]] represents data types of values in the aggregation buffer.
   * For example, if a [[UserDefinedAggregateFunction]]'s buffer has two values
   * (i.e. two intermediate values) with type of [[DoubleType]] and [[LongType]],
   * the returned [[StructType]] will look like
   *
   * ```
   *   new StructType()
   *    .add("doubleInput", DoubleType)
   *    .add("longInput", LongType)
   * ```
   *
   * The name of a field of this [[StructType]] is only used to identify the corresponding
   * buffer value. Users can choose names to identify the input arguments.
   * 定义计算过程中需要的中间变量,比如要求均值,那么中间就需要记录多少个数据了,以及目前总和是多少
   * 比如 new StructType().add("mycnt", LongType).add("mysum", DoubleType)
   */
  def bufferSchema: StructType

  /**
   * The [[DataType]] of the returned value of this [[UserDefinedAggregateFunction]].
   * UDAF聚合函数的返回值
   */
  def dataType: DataType

  /**
   * Returns true iff this function is deterministic, i.e. given the same input,
   * always return the same output.
   * 相同的输入总是返回相同的输出,则该方法返回true
   */
  def deterministic: Boolean

  /**
   * Initializes the given aggregation buffer, i.e. the zero value of the aggregation buffer.
   *
   * The contract should be that applying the merge function on two initial buffers should just
   * return the initial buffer itself, i.e.
   * `merge(initialBuffer, initialBuffer)` should equal `initialBuffer`.
   */
  def initialize(buffer: MutableAggregationBuffer): Unit

  /**
   * Updates the given aggregation buffer `buffer` with new input data from `input`.
   *
   * This is called once per input row.
   * 每一行调用一次该方法,去更新buffer的内容---属于map方法
   */
  def update(buffer: MutableAggregationBuffer, input: Row): Unit

  /**
   * Merges two aggregation buffers and stores the updated buffer values back to `buffer1`.
   *
   * This is called when we merge two partially aggregated data together.用于合并两个局部的集合的时候调用该方法
   * 属于reduce的方法
   *
   * buffer1是本地local产生的一行随时merge的数据。
   * buffer2其实也是MutableAggregationBuffer,代表一行数据,而该行数据是不断从不同节点产生的汇总数据
   */
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit

  /**
   * Calculates the final result of this [[UserDefinedAggregateFunction]] based on the given
   * aggregation buffer.
   * 计算最终的值
   */
  def evaluate(buffer: Row): Any

  /**
   * Creates a [[Column]] for this UDAF using given [[Column]]s as input arguments.
   * 创建一个UDAF列,该列的意义就是计算UDAF
   */
  @scala.annotation.varargs
  def apply(exprs: Column*): Column = {
    val aggregateExpression =
      AggregateExpression2(
        ScalaUDAF(exprs.map(_.expr), this),
        Complete,
        isDistinct = false)
    Column(aggregateExpression)
  }

  /**
   * Creates a [[Column]] for this UDAF using the distinct values of the given
   * [[Column]]s as input arguments.
   */
  @scala.annotation.varargs
  def distinct(exprs: Column*): Column = {
    val aggregateExpression =
      AggregateExpression2(
        ScalaUDAF(exprs.map(_.expr), this),
        Complete,
        isDistinct = true)
    Column(aggregateExpression)
  }
}

/**
 * :: Experimental ::
 * A [[Row]] representing an mutable aggregation buffer.
 *
 * This is not meant to be extended outside of Spark.
 * 代表一行数据
 */
@Experimental
abstract class MutableAggregationBuffer extends Row {

  /** Update the ith value of this buffer.
    * 更新第i个值
    **/
  def update(i: Int, value: Any): Unit
}
