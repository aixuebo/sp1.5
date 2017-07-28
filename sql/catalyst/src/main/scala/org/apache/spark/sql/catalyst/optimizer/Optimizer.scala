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

package org.apache.spark.sql.catalyst.optimizer

import scala.collection.immutable.HashSet
import org.apache.spark.sql.catalyst.analysis.{CleanupAliases, EliminateSubQueries}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.RightOuter
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types._

abstract class Optimizer extends RuleExecutor[LogicalPlan]

object DefaultOptimizer extends Optimizer {

  //预先设置的规则的批次集合
  val batches =
    // SubQueries are only needed for analysis and can be removed before execution.
    Batch("Remove SubQueries", FixedPoint(100),//最大循环次数是100
      EliminateSubQueries) :: //该批次下有一个规则EliminateSubQueries
    Batch("Aggregate", FixedPoint(100),
      ReplaceDistinctWithAggregate,
      RemoveLiteralFromGroupExpressions) ::
    Batch("Operator Optimizations", FixedPoint(100),
      // Operator push down
      SetOperationPushDown,
      SamplePushDown,
      PushPredicateThroughJoin,
      PushPredicateThroughProject,
      PushPredicateThroughGenerate,
      ColumnPruning,
      // Operator combine
      ProjectCollapsing,
      CombineFilters,
      CombineLimits,
      // Constant folding
      NullPropagation,
      OptimizeIn,
      ConstantFolding,
      LikeSimplification,
      BooleanSimplification,
      RemovePositive,
      SimplifyFilters,
      SimplifyCasts,
      SimplifyCaseConversionExpressions) ::
    Batch("Decimal Optimizations", FixedPoint(100),
      DecimalAggregates) ::
    Batch("LocalRelation", FixedPoint(100),
      ConvertToLocalRelation) :: Nil
}

/**
 * Pushes operations down into a Sample.
 * 针对抽样数据进行助词下推,where条件写入到抽样数据集合中,或者见抽样数据集合的不需要的字段删除掉
 */
object SamplePushDown extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // Push down filter into sample
    case Filter(condition, s @ Sample(lb, up, replace, seed, child)) =>
      Sample(lb, up, replace, seed,
        Filter(condition, child)) //让where条件追加到child里面，然后在进行抽样
    // Push down projection into sample
    case Project(projectList, s @ Sample(lb, up, replace, seed, child)) =>
      Sample(lb, up, replace, seed,
        Project(projectList, child))//只让需要的字段被select出来即可
  }
}

/**
 * Pushes operations to either side of a Union, Intersect or Except.
 * 对Union, Intersect or Except.三种集合的操作,进行助词下推
 */
object SetOperationPushDown extends Rule[LogicalPlan] {

  /**
   * Maps Attributes from the left side to the corresponding Attribute on the right side.
   */
  private def buildRewrites(bn: BinaryNode): AttributeMap[Attribute] = {
    assert(bn.isInstanceOf[Union] || bn.isInstanceOf[Intersect] || bn.isInstanceOf[Except]) //只是支持这三种情况
    assert(bn.left.output.size == bn.right.output.size) //输出的字段数量必须相同,想想也是union肯定要求select的字段是相同的

    AttributeMap(bn.left.output.zip(bn.right.output))
  }

  /**
   * Rewrites an expression so that it can be pushed to the right side of a
   * Union, Intersect or Except operator. This method relies on the fact that the output attributes
   * of a union/intersect/except are always equal to the left child's output.
   */
  private def pushToRight[A <: Expression](e: A, rewrites: AttributeMap[Attribute]) = {
    val result = e transform {
      case a: Attribute => rewrites(a)
    }

    // We must promise the compiler that we did not discard the names in the case of project
    // expressions.  This is safe since the only transformation is from Attribute => Attribute.
    result.asInstanceOf[A]
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // Push down filter into union
    case Filter(condition, u @ Union(left, right)) => //union后,进行where过滤掉,此时进行助词下推操作
      val rewrites = buildRewrites(u)
      Union(
        Filter(condition, left),
        Filter(pushToRight(condition, rewrites), right))

    // Push down projection into union
    case Project(projectList, u @ Union(left, right)) => //对union的结果,在进行select的情况,直接将select的内容推送到union即可
      val rewrites = buildRewrites(u)
      Union(
        Project(projectList, left),
        Project(projectList.map(pushToRight(_, rewrites)), right))

    // Push down filter into intersect
    case Filter(condition, i @ Intersect(left, right)) => //交集
      val rewrites = buildRewrites(i)
      Intersect(
        Filter(condition, left),
        Filter(pushToRight(condition, rewrites), right))

    // Push down projection into intersect
    case Project(projectList, i @ Intersect(left, right)) =>
      val rewrites = buildRewrites(i)
      Intersect(
        Project(projectList, left),
        Project(projectList.map(pushToRight(_, rewrites)), right))

    // Push down filter into except
    case Filter(condition, e @ Except(left, right)) => //差集
      val rewrites = buildRewrites(e)
      Except(
        Filter(condition, left),
        Filter(pushToRight(condition, rewrites), right))

    // Push down projection into except
    case Project(projectList, e @ Except(left, right)) =>
      val rewrites = buildRewrites(e)
      Except(
        Project(projectList, left),
        Project(projectList.map(pushToRight(_, rewrites)), right))
  }
}

/**
 * Attempts to eliminate the reading of unneeded columns from the query plan using the following
 * transformations:
 * 尝试消除不需要读取的列
 *
 *  - Inserting Projections beneath the following operators:
 *   - Aggregate
 *   - Generate
 *   - Project <- Join
 *   - LeftSemiJoin
 */
object ColumnPruning extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case a @ Aggregate(_, _, e @ Expand(_, groupByExprs, _, child)) //说明一行数据作为输入,多行数据作为输出的表达式
      if (child.outputSet -- AttributeSet(groupByExprs) -- a.references).nonEmpty => //说明有多余的字段,
      a.copy(child = e.copy(child = prunedChild(child, AttributeSet(groupByExprs) ++ a.references))) //只保留有效的属性集合

    // Eliminate attributes that are not needed to calculate the specified aggregates.
    case a @ Aggregate(_, _, child) if (child.outputSet -- a.references).nonEmpty =>
      a.copy(child = Project(a.references.toSeq, child))

    // Eliminate attributes that are not needed to calculate the Generate.
    case g: Generate if !g.join && (g.child.outputSet -- g.references).nonEmpty =>
      g.copy(child = Project(g.references.toSeq, g.child))

    case p @ Project(_, g: Generate) if g.join && p.references.subsetOf(g.generatedSet) =>
      p.copy(child = g.copy(join = false))

    case p @ Project(projectList, g: Generate) if g.join =>
      val neededChildOutput = p.references -- g.generatorOutput ++ g.references
      if (neededChildOutput == g.child.outputSet) {
        p
      } else {
        Project(projectList, g.copy(child = Project(neededChildOutput.toSeq, g.child)))
      }

    case p @ Project(projectList, a @ Aggregate(groupingExpressions, aggregateExpressions, child))
        if (a.outputSet -- p.references).nonEmpty =>
      Project(
        projectList,
        Aggregate(
          groupingExpressions,
          aggregateExpressions.filter(e => p.references.contains(e)),
          child))

    // Eliminate unneeded attributes from either side of a Join.
    case Project(projectList, Join(left, right, joinType, condition)) =>
      // Collect the list of all references required either above or to evaluate the condition.
      val allReferences: AttributeSet =
        AttributeSet(
          projectList.flatMap(_.references.iterator)) ++
          condition.map(_.references).getOrElse(AttributeSet(Seq.empty))

      /** Applies a projection only when the child is producing unnecessary attributes */
      def pruneJoinChild(c: LogicalPlan): LogicalPlan = prunedChild(c, allReferences)

      Project(projectList, Join(pruneJoinChild(left), pruneJoinChild(right), joinType, condition))

    // Eliminate unneeded attributes from right side of a LeftSemiJoin.
    case Join(left, right, LeftSemi, condition) =>
      // Collect the list of all references required to evaluate the condition.
      val allReferences: AttributeSet =
        condition.map(_.references).getOrElse(AttributeSet(Seq.empty))

      Join(left, prunedChild(right, allReferences), LeftSemi, condition)

    // Push down project through limit, so that we may have chance to push it further.
    case Project(projectList, Limit(exp, child)) =>
      Limit(exp, Project(projectList, child))

    // Push down project if possible when the child is sort
    case p @ Project(projectList, s @ Sort(_, _, grandChild))
      if s.references.subsetOf(p.outputSet) =>
      s.copy(child = Project(projectList, grandChild))

    // Eliminate no-op Projects
    case Project(projectList, child) if child.output == projectList => child //消除自己引用自己,因为列都一样,所以就不需要嵌套这一层
  }

  /** Applies a projection only when the child is producing unnecessary attributes
    * 只保留有效的属性集合
    **/
  private def prunedChild(c: LogicalPlan, allReferences: AttributeSet) =
    if ((c.outputSet -- allReferences.filter(c.outputSet.contains)).nonEmpty) {
      Project(allReferences.filter(c.outputSet.contains).toSeq, c)
    } else {
      c
    }
}

/**
 * Combines two adjacent [[Project]] operators into one and perform alias substitution,
 * merging the expressions into one single expression.
 * 合并两个select合并成1个
 */
object ProjectCollapsing extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    //原始含义:内部对child表有若干个表达式projectList2,然后再最终的结果集上又进行projectList1表达式
    case p @ Project(projectList1, Project(projectList2, child)) =>
      // Create a map of Aliases to their values from the child projection.
      // e.g., 'SELECT ... FROM (SELECT a + b AS c, d ...)' produces Map(c -> Alias(a + b, c)).
      val aliasMap = AttributeMap(projectList2.collect {
        case a: Alias => (a.toAttribute, a) //如果属性有别名,则转换成(属性,别名)这样的元组形式
      })

      // We only collapse these two Projects if their overlapped expressions are all
      // deterministic.
      val hasNondeterministic = projectList1.exists(_.collect {
        case a: Attribute if aliasMap.contains(a) => aliasMap(a).child
      }.exists(!_.deterministic))

      if (hasNondeterministic) {
        p //保持原样,即不进行合并操作
      } else {
        // Substitute any attributes that are produced by the child projection, so that we safely
        // eliminate it.
        // e.g., 'SELECT c + 1 FROM (SELECT a + b AS C ...' produces 'SELECT a + b + 1 ...' 我们将c+1,转换成a+b+1
        // TODO: Fix TransformBase to avoid the cast below.
        val substitutedProjection = projectList1.map(_.transform {
          case a: Attribute => aliasMap.getOrElse(a, a)
        }).asInstanceOf[Seq[NamedExpression]]
        // collapse 2 projects may introduce unnecessary Aliases, trim them here.
        val cleanedProjection = substitutedProjection.map(p =>
          CleanupAliases.trimNonTopLevelAliases(p).asInstanceOf[NamedExpression]
        )
        Project(cleanedProjection, child)
      }
  }
}

/**
 * Simplifies LIKE expressions that do not need full regular expressions to evaluate the condition.
 * For example, when the expression is just checking to see if a string starts with a given
 * pattern.
 * 针对like关键字语法进行优化,不需要匹配全部的正则表达式，而是使用String的一些方法去代替正则表达式,会更高效
 * 比如 当表达式仅仅校验是否给定字符串开始的时候
 *
 */
object LikeSimplification extends Rule[LogicalPlan] {
  // if guards below protect from escapes on trailing %.
  // Cases like "something\%" are not optimized, but this does not affect correctness.
  //[^_%] 表示非_或者%内容开头即可
  private val startsWith = "([^_%]+)%".r //比如abc%
  private val endsWith = "%([^_%]+)".r //比如%abc
  private val contains = "%([^_%]+)%".r //比如%abc%
  private val equalTo = "([^_%]*)".r //比如abc

  //name like '%xxxx%'
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case Like(l, Literal(utf, StringType)) =>
      utf.toString match {
        case startsWith(pattern) if !pattern.endsWith("\\") => //将字符串转换成startsWith方法
          StartsWith(l, Literal(pattern))
        case endsWith(pattern) => //将字符串转换成endsWith方法
          EndsWith(l, Literal(pattern))
        case contains(pattern) if !pattern.endsWith("\\") => //将字符串转换成contains方法
          Contains(l, Literal(pattern))
        case equalTo(pattern) =>
          EqualTo(l, Literal(pattern)) //将字符串转换成=方法
        case _ =>
          Like(l, Literal.create(utf, StringType)) //正常like语法转换
      }
  }
}

/**
 * Replaces [[Expression Expressions]] that can be statically evaluated with
 * equivalent [[Literal]] values. This rule is more specific with
 * Null value propagation from bottom to top of the expression tree.
 * null的传播---从底到顶的传播
 * 如果值是null,该如何处理
 */
object NullPropagation extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsUp {
      case e @ Count(Literal(null, _)) => Cast(Literal(0L), e.dataType) //count(null)转换成0
      case e @ IsNull(c) if !c.nullable => Literal.create(false, BooleanType) // name is null,并且表达式c不是null,因此可以转换成false常量即可
      case e @ IsNotNull(c) if !c.nullable => Literal.create(true, BooleanType)
      case e @ GetArrayItem(Literal(null, _), _) => Literal.create(null, e.dataType)
      case e @ GetArrayItem(_, Literal(null, _)) => Literal.create(null, e.dataType)
      case e @ GetMapValue(Literal(null, _), _) => Literal.create(null, e.dataType) //创建一个null对象
      case e @ GetMapValue(_, Literal(null, _)) => Literal.create(null, e.dataType)
      case e @ GetStructField(Literal(null, _), _, _) => Literal.create(null, e.dataType)
      case e @ GetArrayStructFields(Literal(null, _), _, _, _, _) =>
        Literal.create(null, e.dataType)
      case e @ EqualNullSafe(Literal(null, _), r) => IsNull(r)
      case e @ EqualNullSafe(l, Literal(null, _)) => IsNull(l)
      case e @ Count(expr) if !expr.nullable => Count(Literal(1))

      // For Coalesce, remove null literals.
      case e @ Coalesce(children) =>
        val newChildren = children.filter {
          case Literal(null, _) => false
          case _ => true
        }
        if (newChildren.length == 0) {
          Literal.create(null, e.dataType)
        } else if (newChildren.length == 1) {
          newChildren.head
        } else {
          Coalesce(newChildren)
        }

      case e @ Substring(Literal(null, _), _, _) => Literal.create(null, e.dataType)
      case e @ Substring(_, Literal(null, _), _) => Literal.create(null, e.dataType)
      case e @ Substring(_, _, Literal(null, _)) => Literal.create(null, e.dataType)

      // MaxOf and MinOf can't do null propagation
      case e: MaxOf => e
      case e: MinOf => e

      // Put exceptional cases above if any
      case e @ BinaryArithmetic(Literal(null, _), _) => Literal.create(null, e.dataType)
      case e @ BinaryArithmetic(_, Literal(null, _)) => Literal.create(null, e.dataType)

      case e @ BinaryComparison(Literal(null, _), _) => Literal.create(null, e.dataType)
      case e @ BinaryComparison(_, Literal(null, _)) => Literal.create(null, e.dataType)

      case e: StringRegexExpression => e.children match {
        case Literal(null, _) :: right :: Nil => Literal.create(null, e.dataType)
        case left :: Literal(null, _) :: Nil => Literal.create(null, e.dataType)
        case _ => e
      }

      case e: StringPredicate => e.children match {
        case Literal(null, _) :: right :: Nil => Literal.create(null, e.dataType)
        case left :: Literal(null, _) :: Nil => Literal.create(null, e.dataType)
        case _ => e
      }
    }
  }
}

/**
 * Replaces [[Expression Expressions]] that can be statically evaluated with
 * equivalent [[Literal]] values.
 */
object ConstantFolding extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsDown {
      // Skip redundant folding of literals. This rule is technically not necessary. Placing this
      // here avoids running the next rule for Literal values, which would create a new Literal
      // object and running eval unnecessarily.
      case l: Literal => l

      // Fold expressions that are foldable.
      case e if e.foldable => Literal.create(e.eval(EmptyRow), e.dataType)
    }
  }
}

/**
 * Replaces [[In (value, seq[Literal])]] with optimized version[[InSet (value, HashSet[Literal])]]
 * which is much faster
 * 优化in 操作,比如id in (1,2,3) 将结果集优化成set代替,效果会更好
 */
object OptimizeIn extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsDown {
      case In(v, list) if !list.exists(!_.isInstanceOf[Literal]) && list.size > 10 =>
        val hSet = list.map(e => e.eval(EmptyRow)) //获取list中的值的内容集合.将集合内容转换成set对象
        InSet(v, HashSet() ++ hSet)
    }
  }
}

/**
 * Simplifies boolean expressions:
 * 1. Simplifies expressions whose answer can be determined without evaluating both sides.
 * 2. Eliminates / extracts common factors.
 * 3. Merge same expressions
 * 4. Removes `Not` operator.
 */
object BooleanSimplification extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsUp {
      case and @ And(left, right) => (left, right) match { //两个表达式 and 操作
        // true && r  =>  r
        case (Literal(true, BooleanType), r) => r //只看r即可
        // l && true  =>  l
        case (l, Literal(true, BooleanType)) => l //只看前面left表达式即可
        // false && r  =>  false
        case (Literal(false, BooleanType), _) => Literal(false) //一旦有一个false,则结果就是false
        // l && false  =>  false
        case (_, Literal(false, BooleanType)) => Literal(false) //一旦有一个false,则结果就是false
        // a && a  =>  a
        case (l, r) if l fastEquals r => l //说明表达式相同,因此选择一个即可
        // (a || b) && (a || c)  =>  a || (b && c) 提取公共条件
        case _ =>
          // 1. Split left and right to get the disjunctive predicates,
          //   i.e. lhs = (a, b), rhs = (a, c)
          // 2. Find the common predict between lhsSet and rhsSet, i.e. common = (a)
          // 3. Remove common predict from lhsSet and rhsSet, i.e. ldiff = (b), rdiff = (c)
          // 4. Apply the formula, get the optimized predicate: common || (ldiff && rdiff)
          val lhs = splitDisjunctivePredicates(left) //按照or拆分成集合
          val rhs = splitDisjunctivePredicates(right)
          val common = lhs.filter(e => rhs.exists(e.semanticEquals(_))) //寻找公共的条件
          if (common.isEmpty) {//说明没有公共的条件
            // No common factors, return the original predicate
            and //因为没有公共的,因此保持原样即可,不需要转换,而and就是原始的表达式的别名
          } else {//说明有公共的条件
            val ldiff = lhs.filterNot(e => common.exists(e.semanticEquals(_))) //过滤掉公共的,剩余的left条件
            val rdiff = rhs.filterNot(e => common.exists(e.semanticEquals(_))) //过滤掉公共的,剩余的right条件
            if (ldiff.isEmpty || rdiff.isEmpty) {
              // (a || b || c || ...) && (a || b) => (a || b)
              common.reduce(Or)
            } else {
              // (a || b || c || ...) && (a || b || d || ...) =>
              // ((c || ...) && (d || ...)) || a || b
              (common :+ And(ldiff.reduce(Or), rdiff.reduce(Or))).reduce(Or)
            }
          }
      }  // end of And(left, right)

      case or @ Or(left, right) => (left, right) match { //两个表达式 or 操作
        // true || r  =>  true
        case (Literal(true, BooleanType), _) => Literal(true)
        // r || true  =>  true
        case (_, Literal(true, BooleanType)) => Literal(true)
        // false || r  =>  r
        case (Literal(false, BooleanType), r) => r
        // l || false  =>  l
        case (l, Literal(false, BooleanType)) => l
        // a || a => a
        case (l, r) if l fastEquals r => l
        // (a && b) || (a && c)  =>  a && (b || c)
        case _ =>
           // 1. Split left and right to get the conjunctive predicates,
           //   i.e.  lhs = (a, b), rhs = (a, c)
           // 2. Find the common predict between lhsSet and rhsSet, i.e. common = (a)
           // 3. Remove common predict from lhsSet and rhsSet, i.e. ldiff = (b), rdiff = (c)
           // 4. Apply the formula, get the optimized predicate: common && (ldiff || rdiff)
          val lhs = splitConjunctivePredicates(left)
          val rhs = splitConjunctivePredicates(right)
          val common = lhs.filter(e => rhs.exists(e.semanticEquals(_)))
          if (common.isEmpty) {
            // No common factors, return the original predicate
            or
          } else {
            val ldiff = lhs.filterNot(e => common.exists(e.semanticEquals(_)))
            val rdiff = rhs.filterNot(e => common.exists(e.semanticEquals(_)))
            if (ldiff.isEmpty || rdiff.isEmpty) {
              // (a && b) || (a && b && c && ...) => a && b
              common.reduce(And)
            } else {
              // (a && b && c && ...) || (a && b && d && ...) =>
              // ((c && ...) || (d && ...)) && a && b
              (common :+ Or(ldiff.reduce(And), rdiff.reduce(And))).reduce(And)
            }
          }
      }  // end of Or(left, right)

      case not @ Not(exp) => exp match {
        // not(true)  =>  false
        case Literal(true, BooleanType) => Literal(false) //因为是取反,因此true就变成false
        // not(false)  =>  true
        case Literal(false, BooleanType) => Literal(true)
        // not(l > r)  =>  l <= r
        case GreaterThan(l, r) => LessThanOrEqual(l, r) //因为去反,因此>转换成<=
        // not(l >= r)  =>  l < r
        case GreaterThanOrEqual(l, r) => LessThan(l, r)
        // not(l < r)  =>  l >= r
        case LessThan(l, r) => GreaterThanOrEqual(l, r)
        // not(l <= r)  =>  l > r
        case LessThanOrEqual(l, r) => GreaterThan(l, r)
        // not(not(e))  =>  e
        case Not(e) => e
        case _ => not //保持原样
      }  // end of Not(exp)

      // if (true) a else b  =>  a
      // if (false) a else b  =>  b
      case e @ If(Literal(v, _), trueValue, falseValue) => if (v == true) trueValue else falseValue //把判断条件直接转换成true或者false对应的条件
    }
  }
}

/**
 * Combines two adjacent [[Filter]] operators into one, merging the
 * conditions into one conjunctive predicate.
 * 合并两个相临的filter过滤操作,合并成1个。
 */
object CombineFilters extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    //原始含义:内部是一个过滤器,使用nc条件过滤结果集grandChild,然后过滤后在使用fc过滤条件进一步过滤
    case ff @ Filter(fc, nf @ Filter(nc, grandChild)) => Filter(And(nc, fc), grandChild) //对grandChild结果集有两个过滤条件,一个是nc 一个是fc
  }
}

/**
 * Removes filters that can be evaluated trivially.  This is done either by eliding the filter for
 * cases where it will always evaluate to `true`, or substituting a dummy empty relation when the
 * filter will always evaluate to `false`.
 */
object SimplifyFilters extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // If the filter condition always evaluate to true, remove the filter.
    //如果filter的条件总是true常量,则表示不需要过滤,因此直接移除filter即可
    case Filter(Literal(true, BooleanType), child) => child
    // If the filter condition always evaluate to null or false,
    // replace the input with an empty relation.
      //如果过滤的条件表达式总是null,或者fasle,说明没有任何结果输出,因此数据内容设置为 Seq.empty即可
    case Filter(Literal(null, _), child) => LocalRelation(child.output, data = Seq.empty)
    case Filter(Literal(false, BooleanType), child) => LocalRelation(child.output, data = Seq.empty)
  }
}

/**
 * Pushes [[Filter]] operators through [[Project]] operators, in-lining any [[Alias Aliases]]
 * that were defined in the projection.
 *
 * This heuristic is valid assuming the expression evaluation cost is minimal.
 */
object PushPredicateThroughProject extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    //原始sql含义:select * from grandChild 子查询表,然后where 条件
    case filter @ Filter(condition, project @ Project(fields, grandChild)) =>
      // Create a map of Aliases to their values from the child projection.
      // e.g., 'SELECT a + b AS c, d ...' produces Map(c -> a + b).我们可以获取c等于a+b
      val aliasMap = AttributeMap(fields.collect {
        case a: Alias => (a.toAttribute, a.child)
      })

      // Split the condition into small conditions by `And`, so that we can push down part of this
      // condition without nondeterministic expressions.
      val andConditions = splitConjunctivePredicates(condition) //按照and去拆分条件

      val (deterministic, nondeterministic) = andConditions.partition(_.collect {
        case a: Attribute if aliasMap.contains(a) => aliasMap(a)
      }.forall(_.deterministic))

      // If there is no nondeterministic conditions, push down the whole condition.
      if (nondeterministic.isEmpty) {
        project.copy(child = Filter(replaceAlias(condition, aliasMap), grandChild))
      } else {
        // If they are all nondeterministic conditions, leave it un-changed.
        if (deterministic.isEmpty) {
          filter
        } else {
          // Push down the small conditions without nondeterministic expressions.
          val pushedCondition = deterministic.map(replaceAlias(_, aliasMap)).reduce(And)
          Filter(nondeterministic.reduce(And),
            project.copy(child = Filter(pushedCondition, grandChild)))
        }
      }
  }

  // Substitute any attributes that are produced by the child projection, so that we safely
  // eliminate it.
  private def replaceAlias(condition: Expression, sourceAliases: AttributeMap[Expression]) = {
    condition.transform {
      case a: Attribute => sourceAliases.getOrElse(a, a)
    }
  }
}

/**
 * Push [[Filter]] operators through [[Generate]] operators. Parts of the predicate that reference
 * attributes generated in [[Generate]] will remain above, and the rest should be pushed beneath.
 */
object PushPredicateThroughGenerate extends Rule[LogicalPlan] with PredicateHelper {

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter @ Filter(condition, g: Generate) =>
      // Predicates that reference attributes produced by the `Generate` operator cannot
      // be pushed below the operator.
      val (pushDown, stayUp) = splitConjunctivePredicates(condition).partition {
        conjunct => conjunct.references subsetOf g.child.outputSet
      }
      if (pushDown.nonEmpty) {
        val pushDownPredicate = pushDown.reduce(And)
        val withPushdown = Generate(g.generator, join = g.join, outer = g.outer,
          g.qualifier, g.generatorOutput, Filter(pushDownPredicate, g.child))
        stayUp.reduceOption(And).map(Filter(_, withPushdown)).getOrElse(withPushdown)
      } else {
        filter
      }
  }
}

/**
 * Pushes down [[Filter]] operators where the `condition` can be
 * evaluated using only the attributes of the left or right side of a join.  Other
 * [[Filter]] conditions are moved into the `condition` of the [[Join]].
 *
 * And also Pushes down the join filter, where the `condition` can be evaluated using only the
 * attributes of the left or right side of sub query when applicable.
 *
 * Check https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior for more details
 * 针对join表后的数据进行where处理,该如何优化
 */
object PushPredicateThroughJoin extends Rule[LogicalPlan] with PredicateHelper {
  /**
   * Splits join condition expressions into three categories based on the attributes required
   * to evaluate them.
   * @return (canEvaluateInLeft, canEvaluateInRight, haveToEvaluateInBoth)
   * 将where条件拆分成三部分
   */
  private def split(condition: Seq[Expression], left: LogicalPlan, right: LogicalPlan) = {
    val (leftEvaluateCondition, rest) =
        condition.partition(_.references subsetOf left.outputSet) //将查询条件拆分成属于左边表的字段以及剩余字段
    val (rightEvaluateCondition, commonCondition) =
        rest.partition(_.references subsetOf right.outputSet) //将剩余字段拆分成属于右边表的字段 以及公共条件(即剩余条件)

    (leftEvaluateCondition, rightEvaluateCondition, commonCondition)
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // push the where condition down into join filter
    case f @ Filter(filterCondition, Join(left, right, joinType, joinCondition)) => //针对join表后的数据进行where处理
      val (leftFilterConditions, rightFilterConditions, commonFilterCondition) =
        split(splitConjunctivePredicates(filterCondition), left, right) //拆分where条件,分三部分:左边表的条件、右边表的条件、公共条件

      joinType match {
        case Inner =>
          // push down the single side `where` condition into respective sides
          val newLeft = leftFilterConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left) //不断将两个相邻的条件用and连接起来,然后最终在进行一个filter过滤,在Left表基础上进行过滤
          val newRight = rightFilterConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right) //与left同理
          val newJoinCond = (commonFilterCondition ++ joinCondition).reduceLeftOption(And) //公共的条件与原始join的条件进行合并,产生新的join条件

          Join(newLeft, newRight, Inner, newJoinCond) //组成新的join条件
        case RightOuter =>
          // push down the right side only `where` condition
          //select * from (select * from biao1 right join biao2 on biao1.id = biao2.id) a where a.id = xxx
          //为什么左边的条件不进行过滤呢,好奇怪
          val newLeft = left
          val newRight = rightFilterConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right) //属于right表的条件用and连接起来,对right原是表进行过滤
          val newJoinCond = joinCondition //join的条件
          val newJoin = Join(newLeft, newRight, RightOuter, newJoinCond) //新的join对象

          (leftFilterConditions ++ commonFilterCondition).
            reduceLeftOption(And).map(Filter(_, newJoin)).getOrElse(newJoin) //对join后的结果集,使用leftFilterConditions和commonFilterCondition公共条件进过滤
        case _ @ (LeftOuter | LeftSemi) => //与right过程相反
          // push down the left side only `where` condition
          val newLeft = leftFilterConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = right
          val newJoinCond = joinCondition
          val newJoin = Join(newLeft, newRight, joinType, newJoinCond)

          (rightFilterConditions ++ commonFilterCondition).
            reduceLeftOption(And).map(Filter(_, newJoin)).getOrElse(newJoin)
        case FullOuter => f // DO Nothing for Full Outer Join 不做任何优化
      }

    // push down the join filter into sub query scanning if applicable
    case f @ Join(left, right, joinType, joinCondition) => //就是一个简单的join表,对on后面的语法进行拆分
      val (leftJoinConditions, rightJoinConditions, commonJoinCondition) =
        split(joinCondition.map(splitConjunctivePredicates).getOrElse(Nil), left, right)

      joinType match {
        case _ @ (Inner | LeftSemi) =>
          // push down the single side only join filter for both sides sub queries
          val newLeft = leftJoinConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left) //on后面属于left表的条件追加到left表的where中
          val newRight = rightJoinConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = commonJoinCondition.reduceLeftOption(And) //其他公共条件,继续追加到on中

          Join(newLeft, newRight, joinType, newJoinCond) //组成新的join对象
        case RightOuter =>
          // push down the left side only join filter for left side sub query
          //select * from biao1 right join biao2 on biao1.id = biao2.id and b.id = xxx and a.id = xxx
          //因为on语法是在reduce端操作的,
          val newLeft = leftJoinConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left) //left表进行where补充,变成子查询
          val newRight = right
          val newJoinCond = (rightJoinConditions ++ commonJoinCondition).reduceLeftOption(And) //刨除left表的where后,剩下的追加到on后面

          Join(newLeft, newRight, RightOuter, newJoinCond)
        case LeftOuter =>
          // push down the right side only join filter for right sub query
          val newLeft = left
          val newRight = rightJoinConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = (leftJoinConditions ++ commonJoinCondition).reduceLeftOption(And)

          Join(newLeft, newRight, LeftOuter, newJoinCond)
        case FullOuter => f //full不变化数据结构
      }
  }
}

/**
 * Removes [[Cast Casts]] that are unnecessary because the input is already the correct type.
 * 移除cast语法,因为数据类型本来就是对的,因此没必要加入cast语法
 */
object SimplifyCasts extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case Cast(e, dataType) if e.dataType == dataType => e
  }
}

/**
 * Removes [[UnaryPositive]] identify function
 * 移除+号
 */
object RemovePositive extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case UnaryPositive(child) => child //说明是带有+号的一元正数,删除该+号即可
  }
}

/**
 * Combines two adjacent [[Limit]] operators into one, merging the
 * expressions into one single expression.
 * 合并两个limit操作,合并成一个
 */
object CombineLimits extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    //原始逻辑,内部针对grandChild数据进行limit ne,然后总结果在进一步limit le,因此就是针对grandChild结果集进行min(ne,le)操作
    case ll @ Limit(le, nl @ Limit(ne, grandChild)) =>
      Limit(If(LessThan(ne, le), ne, le), grandChild)
  }
}

/**
 * Removes the inner case conversion expressions that are unnecessary because
 * the inner conversion is overwritten by the outer one.
 * 移除内部的表达式,因为外部的表达式会覆盖掉内部的表达式,因此没必要执行内部的表达式
 */
object SimplifyCaseConversionExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsUp {
      case Upper(Upper(child)) => Upper(child) //对字母进行大写,最终结果就是大写
      case Upper(Lower(child)) => Upper(child) //对字母进行大写,最终结果就是大写
      case Lower(Upper(child)) => Lower(child)//同理最终结果就是小写
      case Lower(Lower(child)) => Lower(child)
    }
  }
}

/**
 * Speeds up aggregates on fixed-precision decimals by executing them on unscaled Long values.
 * 在固定精度的情况下加速聚合,技术上是通过执行在非扩展的long类型上实现的
 * This uses the same rules for increasing the precision and scale of the output as
 * [[org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion.DecimalPrecision]].
 * 对Decimal类型进行sum或者Average聚合的时候,为了快速聚合,进行的小转换
 */
object DecimalAggregates extends Rule[LogicalPlan] {
  import Decimal.MAX_LONG_DIGITS

  /** Maximum number of decimal digits representable precisely in a Double */
  private val MAX_DOUBLE_DIGITS = 15

  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions { //对所有的表达式进行转换
    case Sum(e @ DecimalType.Expression(prec, scale)) if prec + 10 <= MAX_LONG_DIGITS =>
      MakeDecimal(Sum(UnscaledValue(e)), prec + 10, scale)

    case Average(e @ DecimalType.Expression(prec, scale)) if prec + 4 <= MAX_DOUBLE_DIGITS =>
      Cast(
        Divide(Average(UnscaledValue(e)), Literal.create(math.pow(10.0, scale), DoubleType)),
        DecimalType(prec + 4, scale + 4))
  }
}

/**
 * Converts local operations (i.e. ones that don't require data exchange) on LocalRelation to
 * another LocalRelation.
 * 如果不要求数据转换的情况下,转换成本地操作
 * 前提是数据结果集已经加载到本地了,因此可以进行转换
 * This is relatively simple as it currently handles only a single case: Project.
 */
object ConvertToLocalRelation extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Project(projectList, LocalRelation(output, data)) => //projectList表示select的表达式,LocalRelation表示from 的表的内容,其中output是from的所有字段的schema,data是全部数据内容
      val projection = new InterpretedProjection(projectList, output)// 表达式 转换成from表中的字段
      LocalRelation(projectList.map(_.toAttribute), data.map(projection))//将原始的一行数据,现在只选择select部分的属性输出
  }
}

/**
 * Replaces logical [[Distinct]] operator with an [[Aggregate]] operator.
 * {{{
 *   SELECT DISTINCT f1, f2 FROM t  ==>  SELECT f1, f2 FROM t GROUP BY f1, f2
 * }}}
 *
 * 将DISTINCT的sql进行转换,即SELECT DISTINCT f1, f2 FROM t  ==>  SELECT f1, f2 FROM t GROUP BY f1, f2
 */
object ReplaceDistinctWithAggregate extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Distinct(child) => Aggregate(child.output, child.output, child)
  }
}

/**
 * Removes literals from group expressions in [[Aggregate]], as they have no effect to the result
 * but only makes the grouping key bigger.
 * 移除group by中表达式foldable=false的表达式,因为他们对结果没有影响
 *
 * 比如group by id,name,6,因此6这样的常量就可以被省略了
 */
object RemoveLiteralFromGroupExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case a @ Aggregate(grouping, _, _) =>
      val newGrouping = grouping.filter(!_.foldable)
      a.copy(groupingExpressions = newGrouping)
  }
}
