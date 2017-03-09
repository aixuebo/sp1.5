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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types._

/**
 * Throws user facing errors when passed invalid queries that fail to analyze.
 */
trait CheckAnalysis {

  /**
   * Override to provide additional checks for correct analysis.
   * These rules will be evaluated after our built-in check rules.
   * 扩展的校验规则,一个校验规则集合,参数是去校验每一个逻辑计划.不需要返回值,出现问题直接抛异常即可
   */
  val extendedCheckRules: Seq[LogicalPlan => Unit] = Nil

  protected def failAnalysis(msg: String): Nothing = {
    throw new AnalysisException(msg)
  }

  //true表示表达式包含多个Generator表达式
  protected def containsMultipleGenerators(exprs: Seq[Expression]): Boolean = {
    exprs.flatMap(_.collect {//收集所有的Generator表达式
      case e: Generator => e
    }).length > 1
  }

  def checkAnalysis(plan: LogicalPlan): Unit = {
    // We transform up and order the rules so as to catch the first possible failure instead
    // of the result of cascading resolution failures.
    //执行转换和排序规则,出啊去第一个可能失败的结果
    plan.foreachUp {//从孙子开始执行函数
      case p if p.analyzed => // Skip already analyzed sub-plans 跳过已经分析过的节点

      case operator: LogicalPlan =>
        operator transformExpressionsUp { //执行,返回还是LogicalPlan
          case a: Attribute if !a.resolved =>
            val from = operator.inputSet.map(_.name).mkString(", ") //子类输入的所有属性集合
            a.failAnalysis(s"cannot resolve '${a.prettyString}' given input columns $from")//提示给定的输出字段中,不能算出来一个表达式prettyString

          case e: Expression if e.checkInputDataTypes().isFailure => //表达式校验输入类型失败
            e.checkInputDataTypes() match {
              case TypeCheckResult.TypeCheckFailure(message) =>
                e.failAnalysis(
                  s"cannot resolve '${e.prettyString}' due to data type mismatch: $message") //说明该表达式对应的输入类型转换失败
            }

          case c: Cast if !c.resolved =>
            failAnalysis(
              s"invalid cast from ${c.child.dataType.simpleString} to ${c.dataType.simpleString}") //类型转换失败

          case WindowExpression(UnresolvedWindowFunction(name, _), _) =>
            failAnalysis(
              s"Could not resolve window function '$name'. " +
              "Note that, using window functions currently requires a HiveContext")

          case w @ WindowExpression(windowFunction, windowSpec) if windowSpec.validate.nonEmpty =>
            // The window spec is not valid.
            val reason = windowSpec.validate.get
            failAnalysis(s"Window specification $windowSpec is not valid because $reason")
        }

        //继续LogicalPlan   主要校验where group by  having order by操作
        operator match {
          case f: Filter if f.condition.dataType != BooleanType => //where条件的表达式 必须返回值是boolean类型
            failAnalysis(
              s"filter expression '${f.condition.prettyString}' " +
                s"of type ${f.condition.dataType.simpleString} is not a boolean.")

          case j @ Join(_, _, _, Some(condition)) if condition.dataType != BooleanType => //join类型的条件表达式必须返回值是boolean类型
            failAnalysis(
              s"join condition '${condition.prettyString}' " +
                s"of type ${condition.dataType.simpleString} is not a boolean.")

          case j @ Join(_, _, _, Some(condition)) =>
            def checkValidJoinConditionExprs(expr: Expression): Unit = expr match {
              case p: Predicate =>
                p.asInstanceOf[Expression].children.foreach(checkValidJoinConditionExprs)
              case e if e.dataType.isInstanceOf[BinaryType] =>
                failAnalysis(s"binary type expression ${e.prettyString} cannot be used " +
                  "in join conditions")
              case e if e.dataType.isInstanceOf[MapType] =>
                failAnalysis(s"map type expression ${e.prettyString} cannot be used " +
                  "in join conditions")
              case _ => // OK
            }

            checkValidJoinConditionExprs(condition)

          case Aggregate(groupingExprs, aggregateExprs, child) => //group表达式  和 select表达式
            def checkValidAggregateExpression(expr: Expression): Unit = expr match {//校验每一个select属性
              case _: AggregateExpression => // OK 必须是聚合表达式
              case e: Attribute if !groupingExprs.exists(_.semanticEquals(e)) => //说明非聚合表达式,又没有在group by中存在,则抛异常
                failAnalysis(
                  s"expression '${e.prettyString}' is neither present in the group by, " +
                    s"nor is it an aggregate function. " +
                    "Add to group by or wrap in first() if you don't care which value you get.")
              case e if groupingExprs.exists(_.semanticEquals(e)) => // OK
              case e if e.references.isEmpty => // OK 不依赖任何属性,则无所谓
              case e => e.children.foreach(checkValidAggregateExpression) //继续查看子类用到的表达式
            }

            def checkValidGroupingExprs(expr: Expression): Unit = expr.dataType match {
              case BinaryType => //group by的表达式返回值不能是二进制字节数组
                failAnalysis(s"binary type expression ${expr.prettyString} cannot be used " +
                  "in grouping expression")
              case m: MapType => //group by的表达式返回值不能是Map
                failAnalysis(s"map type expression ${expr.prettyString} cannot be used " +
                  "in grouping expression")
              case _ => // OK
            }

            aggregateExprs.foreach(checkValidAggregateExpression)
            groupingExprs.foreach(checkValidGroupingExprs)

          case Sort(orders, _, _) =>
            orders.foreach { order =>
              if (!RowOrdering.isOrderable(order.dataType)) {//表达式返回值必须是支持排序的
                failAnalysis(
                  s"sorting is not supported for columns of type ${order.dataType.simpleString}")
              }
            }

          case s @ SetOperation(left, right) if left.output.length != right.output.length => //输出字段必须相同
            failAnalysis(
              s"${s.nodeName} can only be performed on tables with the same number of columns, " +
               s"but the left table has ${left.output.length} columns and the right has " +
               s"${right.output.length}")

          case _ => // Fallbacks to the following checks
        }

        //校验select
        operator match {
          case o if o.children.nonEmpty && o.missingInput.nonEmpty => //说明缺失一部分属性,因此没办法运行sql
            val missingAttributes = o.missingInput.mkString(",")
            val input = o.inputSet.mkString(",")

            failAnalysis(
              s"resolved attribute(s) $missingAttributes missing from $input " +
                s"in operator ${operator.simpleString}")

          case p @ Project(exprs, _) if containsMultipleGenerators(exprs) => //表达式不能包含多个Generator表达式
            failAnalysis(
              s"""Only a single table generating function is allowed in a SELECT clause, found:
                 | ${exprs.map(_.prettyString).mkString(",")}""".stripMargin)

          // Special handling for cases when self-join introduce duplicate expression ids.
          case j @ Join(left, right, _, _) if left.outputSet.intersect(right.outputSet).nonEmpty =>
            val conflictingAttributes = left.outputSet.intersect(right.outputSet) //左边和右边属性名字冲突
            failAnalysis(
              s"""
                 |Failure when resolving conflicting references in Join:
                 |$plan
                 |Conflicting attributes: ${conflictingAttributes.mkString(",")}
                 |""".stripMargin)

          case o if !o.resolved =>
            failAnalysis(
              s"unresolved operator ${operator.simpleString}")

          case o if o.expressions.exists(!_.deterministic) && //不确定的表达式,比如随机数.只允许出现在select和where条件中
            !o.isInstanceOf[Project] && !o.isInstanceOf[Filter] =>
            failAnalysis(
              s"""nondeterministic expressions are only allowed in Project or Filter, found:
                 | ${o.expressions.map(_.prettyString).mkString(",")}
                 |in operator ${operator.simpleString}
             """.stripMargin)

          case _ => // Analysis successful!
        }
    }

    //扩展的校验规则
    extendedCheckRules.foreach(_(plan)) //循环每一个校验规则函数,将该逻辑计划传入到规则中去校验

    plan.foreach(_.setAnalyzed()) //设置该计划已经分析完成
  }
}
