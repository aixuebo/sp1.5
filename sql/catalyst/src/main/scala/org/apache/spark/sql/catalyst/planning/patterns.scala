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

package org.apache.spark.sql.catalyst.planning

import scala.annotation.tailrec

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.trees.TreeNodeRef
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._

/**
 * A pattern that matches any number of filter operations on top of another relational operator.
 * Adjacent filter operators are collected and their conditions are broken up and returned as a
 * sequence of conjunctive predicates.
 * 匹配所有filter操作的表达式
 *
 * @return A tuple containing a sequence of conjunctive predicates that should be used to filter the
 *         output and a relational operator.
 */
object FilteredOperation extends PredicateHelper {
  type ReturnType = (Seq[Expression], LogicalPlan)

  //拆分该逻辑计划终的filter表达式集合
  def unapply(plan: LogicalPlan): Option[ReturnType] = Some(collectFilters(Nil, plan))

  //第一个参数是所有的filter条件集合
  //第二个参数就是计划本身
  //收集计划中filter条件
  @tailrec
  private def collectFilters(filters: Seq[Expression], plan: LogicalPlan): ReturnType = plan match {
    case Filter(condition, child) => //说明该计划是一个filter计划,则提取fileter条件
      collectFilters(filters ++ splitConjunctivePredicates(condition), child) //递归操作
    case other => (filters, other)
  }
}

/**
 * A pattern that matches any number of project or filter operations on top of another relational
 * operator.  All filter operators are collected and their conditions are broken up and returned
 * together with the top project operator.
 * [[org.apache.spark.sql.catalyst.expressions.Alias Aliases]] are in-lined/substituted if
 * necessary.
 */
object PhysicalOperation extends PredicateHelper {
  type ReturnType = (Seq[NamedExpression], Seq[Expression], LogicalPlan)

  def unapply(plan: LogicalPlan): Option[ReturnType] = {
    val (fields, filters, child, _) = collectProjectsAndFilters(plan)
    Some((fields.getOrElse(child.output), filters, child))
  }

  /**
   * Collects projects and filters, in-lining/substituting aliases if necessary.  Here are two
   * examples for alias in-lining/substitution.  Before:
   * {{{
   *   SELECT c1 FROM (SELECT key AS c1 FROM t1) t2 WHERE c1 > 10   原来c1来自于t2,即c1对应的别名对象为Alias(t2,c1)---改成Alias(t1,c1)
   *   SELECT c1 AS c2 FROM (SELECT key AS c1 FROM t1) t2 WHERE c1 > 10
   * }}}
   * After:
   * {{{
   *   SELECT key AS c1 FROM t1 WHERE key > 10
   *   SELECT key AS c2 FROM t1 WHERE key > 10
   * }}}
   */
  def collectProjectsAndFilters(plan: LogicalPlan):
      (Option[Seq[NamedExpression]], Seq[Expression], LogicalPlan, Map[Attribute, Expression]) =
    plan match {
      case Project(fields, child) => //说明是对一个表进行select表达式查询
        val (_, filters, other, aliases) = collectProjectsAndFilters(child) //递归操作
        val substitutedFields = fields.map(substitute(aliases)).asInstanceOf[Seq[NamedExpression]]
        (Some(substitutedFields), filters, other, collectAliases(substitutedFields))

      case Filter(condition, child) => //说明是where条件 或者having条件
        val (fields, filters, other, aliases) = collectProjectsAndFilters(child)
        val substitutedCondition = substitute(aliases)(condition)
        (fields, filters ++ splitConjunctivePredicates(substitutedCondition), other, aliases)

      case other =>
        (None, Nil, other, Map.empty)
    }

  def collectAliases(fields: Seq[Expression]): Map[Attribute, Expression] = fields.collect {
    case a @ Alias(child, _) => a.toAttribute -> child
  }.toMap

  def substitute(aliases: Map[Attribute, Expression])(expr: Expression): Expression = {
    expr.transform {
      case a @ Alias(ref: AttributeReference, name) =>
        //别名对应的child对象,重新对别名命名
        aliases.get(ref).map(Alias(_, name)(a.exprId, a.qualifiers)).getOrElse(a)

      case a: AttributeReference =>
        aliases.get(a).map(Alias(_, a.name)(a.exprId, a.qualifiers)).getOrElse(a)
    }
  }
}

/**
 * Matches a logical aggregation that can be performed on distributed data in two steps.  The first
 * operates on the data in each partition performing partial aggregation for each group.  The second
 * occurs after the shuffle and completes the aggregation.
 *
 * This pattern will only match if all aggregate expressions can be computed partially and will
 * return the rewritten aggregation expressions for both phases.
 *
 * The returned values for this match are as follows:
 *  - Grouping attributes for the final aggregation.
 *  - Aggregates for the final aggregation.
 *  - Grouping expressions for the partial aggregation.
 *  - Partial aggregate expressions.
 *  - Input to the aggregation.
 *  对group by的聚合sql进行分析
 */
object PartialAggregation {
  type ReturnType =
    (Seq[Attribute], Seq[NamedExpression], Seq[Expression], Seq[NamedExpression], LogicalPlan)

  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    case logical.Aggregate(groupingExpressions, aggregateExpressions, child) => //说明是group by的聚合sql
      // Collect all aggregate expressions.
      val allAggregates =
        aggregateExpressions.flatMap(_ collect { case a: AggregateExpression1 => a})
      // Collect all aggregate expressions that can be computed partially.
      val partialAggregates =
        aggregateExpressions.flatMap(_ collect { case p: PartialAggregate1 => p})

      // Only do partial aggregation if supported by all aggregate expressions.
      if (allAggregates.size == partialAggregates.size) {//说明所有的聚合函数都是偏函数
        // Create a map of expressions to their partial evaluations for all aggregate expressions.
        val partialEvaluations: Map[TreeNodeRef, SplitEvaluation] =
          partialAggregates.map(a => (new TreeNodeRef(a), a.asPartial)).toMap

        // We need to pass all grouping expressions though so the grouping can happen a second
        // time. However some of them might be unnamed so we alias them allowing them to be
        // referenced in the second aggregation.
        val namedGroupingExpressions: Seq[(Expression, NamedExpression)] =
          groupingExpressions.map {
            case n: NamedExpression => (n, n)
            case other => (other, Alias(other, "PartialGroup")())
          }

        // Replace aggregations with a new expression that computes the result from the already
        // computed partial evaluations and grouping values.
        val rewrittenAggregateExpressions = aggregateExpressions.map(_.transformDown {
          case e: Expression if partialEvaluations.contains(new TreeNodeRef(e)) =>
            partialEvaluations(new TreeNodeRef(e)).finalEvaluation

          case e: Expression =>
            namedGroupingExpressions.collectFirst {
              case (expr, ne) if expr semanticEquals e => ne.toAttribute
            }.getOrElse(e)
        }).asInstanceOf[Seq[NamedExpression]]

        val partialComputation = namedGroupingExpressions.map(_._2) ++
          partialEvaluations.values.flatMap(_.partialEvaluations)

        val namedGroupingAttributes = namedGroupingExpressions.map(_._2.toAttribute)

        Some(
          (namedGroupingAttributes,
           rewrittenAggregateExpressions,
           groupingExpressions,
           partialComputation,
           child))
      } else {
        None
      }
    case _ => None
  }
}


/**
 * A pattern that finds joins with equality conditions that can be evaluated using equi-join.
 * 抽取join中on的表达式,抽取属于左边表和右边表的表达式
 */
object ExtractEquiJoinKeys extends Logging with PredicateHelper {
  /** (joinType, leftKeys, rightKeys, condition, leftChild, rightChild) */
  type ReturnType =
    (JoinType, Seq[Expression], Seq[Expression], Option[Expression], LogicalPlan, LogicalPlan)

  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    case join @ Join(left, right, joinType, condition) => //说明此时是一个join操作
      logDebug(s"Considering join on: $condition")
      // Find equi-join predicates that can be evaluated before the join, and thus can be used
      // as join keys.
      //joinPredicates 表示属性=属性,这等号两边的属性分别来自于两个表的情况
      //otherPredicates 表示非来自于两个表的情况,即剩余的情况
      val (joinPredicates, otherPredicates) =
        condition.map(splitConjunctivePredicates).getOrElse(Nil).partition {//将条件按照and拆分.然后找到a.id=b.id这种带有等号的on表达式
          case EqualTo(l, r) if (canEvaluate(l, left) && canEvaluate(r, right)) || //true表示该表达式 用到的字段都是plan中输出的字段
            (canEvaluate(l, right) && canEvaluate(r, left)) => true
          case _ => false
        }

      //拆分biao1.name = biao2.name
      val joinKeys = joinPredicates.map {
        case EqualTo(l, r) if canEvaluate(l, left) && canEvaluate(r, right) => (l, r) //找到左边表需要的条件 以及右边表需要的条件
        case EqualTo(l, r) if canEvaluate(l, right) && canEvaluate(r, left) => (r, l)
      }
      val leftKeys = joinKeys.map(_._1) //左边表需要的字段条件集合
      val rightKeys = joinKeys.map(_._2) //右边表需要的字段条件集合

      if (joinKeys.nonEmpty) {
        logDebug(s"leftKeys:$leftKeys | rightKeys:$rightKeys")
        Some((joinType, leftKeys, rightKeys, otherPredicates.reduceOption(And), left, right))
      } else {
        None
      }
    case _ => None
  }
}

/**
 * A pattern that collects all adjacent unions and returns their children as a Seq.
 */
object Unions {
  def unapply(plan: LogicalPlan): Option[Seq[LogicalPlan]] = plan match {//将union的所有select的逻辑计划组装成一个集合返回
    case u: Union => Some(collectUnionChildren(u))
    case _ => None
  }

  //递归的方式获取所有的select对应的逻辑计划
  private def collectUnionChildren(plan: LogicalPlan): Seq[LogicalPlan] = plan match {
    case Union(l, r) => collectUnionChildren(l) ++ collectUnionChildren(r)
    case other => other :: Nil
  }
}
