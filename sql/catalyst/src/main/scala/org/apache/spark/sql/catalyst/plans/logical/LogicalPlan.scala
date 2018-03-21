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

import org.apache.spark.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, TreeNode}


//解析一个sql,返回该sql的逻辑执行计划
abstract class LogicalPlan extends QueryPlan[LogicalPlan] with Logging {

  private var _analyzed: Boolean = false //true表示该计划已经被分析过了

  /**
   * Marks this plan as already analyzed.  This should only be called by CheckAnalysis.
   */
  private[catalyst] def setAnalyzed(): Unit = { _analyzed = true }

  /**
   * Returns true if this node and its children have already been gone through analysis and
   * verification.  Note that this is only an optimization used to avoid analyzing trees that
   * have already been analyzed, and can be reset by transformations.
   */
  def analyzed: Boolean = _analyzed

  /**
   * Returns a copy of this node where `rule` has been recursively applied first to all of its
   * children and then itself (post-order). When `rule` does not apply to a given node, it is left
   * unchanged.  This function is similar to `transformUp`, but skips sub-trees that have already
   * been marked as analyzed.
   * 会递归调用所有子节点,让他们参与规则处理
   *
   * @param rule the function use to transform this nodes children 他会对他的子节点进行变换
   * 参数是一个偏函数,即从一个LogicalPlan转换成一个LogicalPlan的过程
   *
   * resolveOperators方法接收一个偏函数,偏函数必须只能是处理某种类型的LogicalPlan,返回也是LogicalPlan
   */
  def resolveOperators(rule: PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan = {
    if (!analyzed) {//没有分析过,才去分析
      val afterRuleOnChildren = transformChildren(rule, (t, r) => t.resolveOperators(r))
      if (this fastEquals afterRuleOnChildren) {//比较结果是否相同
        CurrentOrigin.withOrigin(origin) {
          rule.applyOrElse(this, identity[LogicalPlan])
        }
      } else {
        CurrentOrigin.withOrigin(origin) {
          rule.applyOrElse(afterRuleOnChildren, identity[LogicalPlan]) //如果afterRuleOnChildren满足rule偏函数规则,则处理,不满足则返回规则本身
        }
      }
    } else {//分析过了直接返回
      this
    }
  }

  /**
   * Recursively transforms the expressions of a tree, skipping nodes that have already
   * been analyzed.
   */
  def resolveExpressions(r: PartialFunction[Expression, Expression]): LogicalPlan = {
    this resolveOperators  {
      case p => p.transformExpressions(r) //表示匿名的偏函数实现,由于resolveOperators接收 PartialFunction[LogicalPlan, LogicalPlan],因此p就是LogicalPlan类型,返回值也是LogicalPlan类型,这样就代表参数r是每一个LogicalPlan都要执行transformExpressions时候传入的参数
    }
  }

  /**
   * Computes [[Statistics]] for this plan. The default implementation assumes the output
   * cardinality is the product of of all child plan's cardinality, i.e. applies in the case
   * of cartesian joins.
   *
   * [[LeafNode]]s must override this.
   */
  def statistics: Statistics = {
    if (children.size == 0) {
      throw new UnsupportedOperationException(s"LeafNode $nodeName must implement statistics.")
    }
    Statistics(sizeInBytes = children.map(_.statistics.sizeInBytes).product) //递归获取所有子对象的输出字节大小
  }

  /**
   * Returns true if this expression and all its children have been resolved to a specific schema
   * and false if it still contains any unresolved placeholders. Implementations of LogicalPlan
   * can override this (e.g.
   * [[org.apache.spark.sql.catalyst.analysis.UnresolvedRelation UnresolvedRelation]]
   * should return `false`).
   * 校验本身以及所有的子对象
   */
  lazy val resolved: Boolean = expressions.forall(_.resolved) && childrenResolved //childrenResolved表示继续递归,让子节点的子节点再次执行

  override protected def statePrefix = if (!resolved) "'" else super.statePrefix

  /**
   * Returns true if all its children of this query plan have been resolved.
   * true表示所有的子计划已经resolved
   * 属于一个校验的过程
   */
  def childrenResolved: Boolean = children.forall(_.resolved)

  /**
   * Returns true when the given logical plan will return the same results as this logical plan.
   *
   * Since its likely undecidable to generally determine if two given plans will produce the same
   * results, it is okay for this function to return false, even if the results are actually
   * the same.  Such behavior will not affect correctness, only the application of performance
   * enhancements like caching.  However, it is not acceptable to return true if the results could
   * possibly be different.
   *
   * By default this function performs a modified version of equality that is tolerant of cosmetic
   * differences like attribute naming and or expression id differences.  Logical operators that
   * can do better should override this function.
   */
  def sameResult(plan: LogicalPlan): Boolean = {
    val cleanLeft = EliminateSubQueries(this)
    val cleanRight = EliminateSubQueries(plan)

    cleanLeft.getClass == cleanRight.getClass &&
      cleanLeft.children.size == cleanRight.children.size && {
      logDebug(
        s"[${cleanRight.cleanArgs.mkString(", ")}] == [${cleanLeft.cleanArgs.mkString(", ")}]")
      cleanRight.cleanArgs == cleanLeft.cleanArgs
    } &&
    (cleanLeft.children, cleanRight.children).zipped.forall(_ sameResult _)
  }

  /** Args that have cleaned such that differences in expression id should not affect equality */
  protected lazy val cleanArgs: Seq[Any] = {
    val input = children.flatMap(_.output)
    productIterator.map {
      // Children are checked using sameResult above.
      case tn: TreeNode[_] if containsChild(tn) => null
      case e: Expression => BindReferences.bindReference(e, input, allowFailures = true)
      case s: Option[_] => s.map {
        case e: Expression => BindReferences.bindReference(e, input, allowFailures = true)
        case other => other
      }
      case s: Seq[_] => s.map {
        case e: Expression => BindReferences.bindReference(e, input, allowFailures = true)
        case other => other
      }
      case other => other
    }.toSeq
  }

  /**
   * Optionally resolves the given strings to a [[NamedExpression]] using the input from all child
   * nodes of this LogicalPlan. The attribute is expressed as
   * as string in the following form: `[scope].AttributeName.[nested].[fields]...`.
   */
  def resolveChildren(
      nameParts: Seq[String],
      resolver: Resolver): Option[NamedExpression] =
    resolve(nameParts, children.flatMap(_.output), resolver)

  /**
   * Optionally resolves the given strings to a [[NamedExpression]] based on the output of this
   * LogicalPlan. The attribute is expressed as string in the following form:
   * `[scope].AttributeName.[nested].[fields]...`.
   */
  def resolve(
      nameParts: Seq[String],
      resolver: Resolver): Option[NamedExpression] =
    resolve(nameParts, output, resolver)

  /**
   * Given an attribute name, split it to name parts by dot, but
   * don't split the name parts quoted by backticks, for example,
   * `ab.cd`.`efg` should be split into two parts "ab.cd" and "efg".
   * 给一个属性名字.使用.进行拆分,但是不能拆分在``里面的.
   */
  def resolveQuoted(
      name: String,//要查询的属性名字
      resolver: Resolver) //判断两个名字是否相同
    : Option[NamedExpression] = {
    resolve(UnresolvedAttribute.parseAttributeName(name) //将name拆分成集合
      , output, resolver)
  }

  /**
   * Resolve the given `name` string against the given attribute, returning either 0 or 1 match.
   *
   * This assumes `name` has multiple parts, where the 1st part is a qualifier
   * (i.e. table name, alias, or subquery alias).
   * See the comment above `candidates` variable in resolve() for semantics the returned data.
   * 匹配table.column
   */
  private def resolveAsTableColumn(
      nameParts: Seq[String],//要匹配的属性内容,比如table.column.fileldName
      resolver: Resolver,//如何判断两个字符串先通过
      attribute: Attribute) //该nameParts是否与该属性相似
    : Option[(Attribute, List[String])] = {
    assert(nameParts.length > 1)
    if (attribute.qualifiers.exists(resolver(_, nameParts.head))) {//说明table匹配上了
      // At least one qualifier matches. See if remaining parts match.
      val remainingParts = nameParts.tail //获取剩下的部分
      resolveAsColumn(remainingParts, resolver, attribute)//匹配column
    } else {
      None
    }
  }

  /**
   * Resolve the given `name` string against the given attribute, returning either 0 or 1 match.
   *
   * Different from resolveAsTableColumn, this assumes `name` does NOT start with a qualifier.
   * See the comment above `candidates` variable in resolve() for semantics the returned data.
   * 匹配column属性
   */
  private def resolveAsColumn(
      nameParts: Seq[String],//要匹配的属性内容,比如column.fileldName
      resolver: Resolver,
      attribute: Attribute): Option[(Attribute, List[String])] = {
    if (resolver(attribute.name, nameParts.head)) {//说明匹配成功
      Option((attribute.withName(nameParts.head), nameParts.tail.toList))
    } else {
      None
    }
  }

  /** Performs attribute resolution given a name and a sequence of possible attributes.
    * 给一个属性名字以及属性集合,来执行name属于哪个属性
    **/
  protected def resolve(
      nameParts: Seq[String],//要查找的属性名字,比如数据库.tableName
      input: Seq[Attribute],//输入的属性集合
      resolver: Resolver) //如何比较两个字符是相同的
      : Option[NamedExpression] = {

    // A sequence of possible candidate matches.
    // Each candidate is a tuple. The first element is a resolved attribute, followed by a list
    // of parts that are to be resolved.
    // For example, consider an example where "a" is the table name, "b" is the column name,
    // and "c" is the struct field name, i.e. "a.b.c". In this case, Attribute will be "a.b",
    // and the second element will be List("c").
    var candidates: Seq[(Attribute, List[String])] = {
      // If the name has 2 or more parts, try to resolve it as `table.column` first.
      if (nameParts.length > 1) {
        input.flatMap { option =>
          resolveAsTableColumn(nameParts, resolver, option)
        }
      } else {
        Seq.empty
      }
    }

    // If none of attributes match `table.column` pattern, we try to resolve it as a column.
    if (candidates.isEmpty) { //说明没有匹配table.column模式,因此我们要尝试使用column拮据
      candidates = input.flatMap { candidate =>
        resolveAsColumn(nameParts, resolver, candidate)
      }
    }

    def name = UnresolvedAttribute(nameParts).name

    candidates.distinct match {
      // One match, no nested fields, use it. //说明只有一个名字,不是嵌套的属性
      case Seq((a, Nil)) => Some(a)

      // One match, but we also need to extract the requested nested field.
      case Seq((a, nestedFields)) =>
        // The foldLeft adds ExtractValues for every remaining parts of the identifier,
        // and aliased it with the last part of the name.
        // For example, consider "a.b.c", where "a" is resolved to an existing attribute.
        // Then this will add ExtractValue("c", ExtractValue("b", a)), and alias the final
        // expression as "c".
        val fieldExprs = nestedFields.foldLeft(a: Expression)((expr, fieldName) =>
          ExtractValue(expr, Literal(fieldName), resolver))
        Some(Alias(fieldExprs, nestedFields.last)())

      // No matches.说明没有匹配成功
      case Seq() =>
        logTrace(s"Could not find $name in ${input.mkString(", ")}")
        None

      // More than one match.多余一个匹配,引用不明确
      case ambiguousReferences =>
        val referenceNames = ambiguousReferences.map(_._1).mkString(", ")
        throw new AnalysisException(
          s"Reference '$name' is ambiguous, could be: $referenceNames.")
    }
  }
}

/**
 * A logical plan node with no children.
 * 叶子节点不能有子节点
 */
abstract class LeafNode extends LogicalPlan {
  override def children: Seq[LogicalPlan] = Nil
}

/**
 * A logical plan node with single child.
 * 逻辑子节点只能有一个
 */
abstract class UnaryNode extends LogicalPlan {
  def child: LogicalPlan

  override def children: Seq[LogicalPlan] = child :: Nil
}

/**
 * A logical plan node with a left and right child.
 * 逻辑运算需要两个子节点
 */
abstract class BinaryNode extends LogicalPlan {
  def left: LogicalPlan
  def right: LogicalPlan

  override def children: Seq[LogicalPlan] = Seq(left, right)
}
