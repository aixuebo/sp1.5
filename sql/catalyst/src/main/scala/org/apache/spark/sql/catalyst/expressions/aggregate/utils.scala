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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.types.{StructType, MapType, ArrayType}

/**
 * Utility functions used by the query planner to convert our plan to new aggregation code path.
 */
object Utils {
  // Right now, we do not support complex types in the grouping key schema.
  //在group by中现在我们不支持复杂类型,
  //返回true 说明都是简单的类型,因此可以group by,返回false,说明group by的表达式中有复杂类型,因此不支持
  private def supportsGroupingKeySchema(aggregate: Aggregate): Boolean = {
    //查看聚合任务中的group by表达式集合的类型,是否存在复杂类型,比如数组  map  struct,如果存在则返回true
    val hasComplexTypes = aggregate.groupingExpressions.map(_.dataType).exists {
      case array: ArrayType => true
      case map: MapType => true
      case struct: StructType => true
      case _ => false
    }

    !hasComplexTypes
  }

  private def doConvert(plan: LogicalPlan): Option[Aggregate] = plan match {
    case p: Aggregate if supportsGroupingKeySchema(p) => //该聚合逻辑计划中,group by的表达式都是对的
      val converted = p.transformExpressionsDown { //transformExpressionsDown方法接受PartialFunction偏函数,参数就是匿名的偏函数, 返回值依然是Aggregate对象
        case expressions.Average(child) =>
          aggregate.AggregateExpression2(
            aggregateFunction = aggregate.Average(child),
            mode = aggregate.Complete,
            isDistinct = false)

        case expressions.Count(child) =>
          aggregate.AggregateExpression2(
            aggregateFunction = aggregate.Count(child),
            mode = aggregate.Complete,
            isDistinct = false)

        // We do not support multiple COUNT DISTINCT columns for now.
        case expressions.CountDistinct(children) if children.length == 1 =>
          aggregate.AggregateExpression2(
            aggregateFunction = aggregate.Count(children.head),
            mode = aggregate.Complete,
            isDistinct = true)

        case expressions.First(child) =>
          aggregate.AggregateExpression2(
            aggregateFunction = aggregate.First(child),
            mode = aggregate.Complete,
            isDistinct = false)

        case expressions.Last(child) =>
          aggregate.AggregateExpression2(
            aggregateFunction = aggregate.Last(child),
            mode = aggregate.Complete,
            isDistinct = false)

        case expressions.Max(child) =>
          aggregate.AggregateExpression2(
            aggregateFunction = aggregate.Max(child),
            mode = aggregate.Complete,
            isDistinct = false)

        case expressions.Min(child) =>
          aggregate.AggregateExpression2(
            aggregateFunction = aggregate.Min(child),
            mode = aggregate.Complete,
            isDistinct = false)

        case expressions.Sum(child) =>
          aggregate.AggregateExpression2(
            aggregateFunction = aggregate.Sum(child),
            mode = aggregate.Complete,
            isDistinct = false)

        case expressions.SumDistinct(child) =>
          aggregate.AggregateExpression2(
            aggregateFunction = aggregate.Sum(child),
            mode = aggregate.Complete,
            isDistinct = true)
      }
      // Check if there is any expressions.AggregateExpression1 left.
      // If so, we cannot convert this plan.
      //循环所有的select中所有的表达式集合,查看是否有聚合表达式的属性,true表示存在聚合表达式
      val hasAggregateExpression1 = converted.aggregateExpressions.exists { expr => //每一个select中的表达式
        // For every expressions, check if it contains AggregateExpression1.
        expr.find {
          case agg: expressions.AggregateExpression1 => true //说明有聚合表达式
          case other => false
        }.isDefined
      }

      // Check if there are multiple distinct columns.
      //循环所有的select中所有的表达式集合,获取所有的AggregateExpression2表达式集合
      val aggregateExpressions = converted.aggregateExpressions.flatMap { expr =>
        expr.collect {
          case agg: AggregateExpression2 => agg
        }
      }.toSet.toSeq

      //找到包含distinct的聚合表达式集合
      val functionsWithDistinct = aggregateExpressions.filter(_.isDistinct)

      //是否有多个distinct表达式,true表示有多个,false表示有0或者1个
      val hasMultipleDistinctColumnSets =
        if (functionsWithDistinct.map(_.aggregateFunction.children).distinct.length > 1) {
          true
        } else {
          false
        }

      //说明没有聚合函数  && 只有0或者1个distinct函数,返回该Aggregate聚合逻辑计划
      if (!hasAggregateExpression1 && !hasMultipleDistinctColumnSets) Some(converted) else None

    case other => None
  }

  def checkInvalidAggregateFunction2(aggregate: Aggregate): Unit = {
    // If the plan cannot be converted, we will do a final round check to see if the original
    // logical.Aggregate contains both AggregateExpression1 and AggregateExpression2. If so,
    // we need to throw an exception.
    val aggregateFunction2s = aggregate.aggregateExpressions.flatMap { expr =>
      expr.collect {
        case agg: AggregateExpression2 => agg.aggregateFunction
      }
    }.distinct
    if (aggregateFunction2s.nonEmpty) {
      // For functions implemented based on the new interface, prepare a list of function names.
      val invalidFunctions = {
        if (aggregateFunction2s.length > 1) {
          s"${aggregateFunction2s.tail.map(_.nodeName).mkString(",")} " +
            s"and ${aggregateFunction2s.head.nodeName} are"
        } else {
          s"${aggregateFunction2s.head.nodeName} is"
        }
      }
      val errorMessage =
        s"${invalidFunctions} implemented based on the new Aggregate Function " +
          s"interface and it cannot be used with functions implemented based on " +
          s"the old Aggregate Function interface."
      throw new AnalysisException(errorMessage)
    }
  }

  def tryConvert(plan: LogicalPlan): Option[Aggregate] = plan match {
    case p: Aggregate =>
      val converted = doConvert(p)
      if (converted.isDefined) {
        converted
      } else {
        checkInvalidAggregateFunction2(p)
        None
      }
    case other => None
  }
}
