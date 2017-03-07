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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types.{NullType, BooleanType, DataType}

//条件表达式


//if表达式.第一个表达式返回boolean类型,从而决定后面哪个表达式去执行真正逻辑
case class If(predicate: Expression, trueValue: Expression, falseValue: Expression)
  extends Expression {

  override def children: Seq[Expression] = predicate :: trueValue :: falseValue :: Nil
  override def nullable: Boolean = trueValue.nullable || falseValue.nullable

  override def checkInputDataTypes(): TypeCheckResult = {
    if (predicate.dataType != BooleanType) { //校验第一个表达式一定是boolean类型的
      TypeCheckResult.TypeCheckFailure(
        s"type of predicate expression in If should be boolean, not ${predicate.dataType}")
    } else if (trueValue.dataType != falseValue.dataType) {//必须保证true和false时候执行的表达式返回值是一样的
      TypeCheckResult.TypeCheckFailure(s"differing types in '$prettyString' " +
        s"(${trueValue.dataType.simpleString} and ${falseValue.dataType.simpleString}).")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def dataType: DataType = trueValue.dataType //返回值就是true或者false表达式的返回值

  override def eval(input: InternalRow): Any = {
    if (true == predicate.eval(input)) {
      trueValue.eval(input) //true去执行
    } else {
      falseValue.eval(input) //执行false表达式
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val condEval = predicate.gen(ctx)
    val trueEval = trueValue.gen(ctx)
    val falseEval = falseValue.gen(ctx)

    s"""
      ${condEval.code}
      boolean ${ev.isNull} = false;
      ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
      if (!${condEval.isNull} && ${condEval.primitive}) {
        ${trueEval.code}
        ${ev.isNull} = ${trueEval.isNull};
        ${ev.primitive} = ${trueEval.primitive};
      } else {
        ${falseEval.code}
        ${ev.isNull} = ${falseEval.isNull};
        ${ev.primitive} = ${falseEval.primitive};
      }
    """
  }

  override def toString: String = s"if ($predicate) $trueValue else $falseValue"
}

//case when 表达式
trait CaseWhenLike extends Expression {

  // Note that `branches` are considered in consecutive pairs (cond, val), and the optional last
  // element is the value for the default catch-all case (if provided).
  // Hence, `branches` consists of at least two elements, and can have an odd or even length.
  def branches: Seq[Expression] //子表达式集合

  //每2个是一组,每次跳跃2个,因为when then两个表达式表示的是一组含义
  /**
  var aa = Seq(1,2,3,4,5,6)
  aa.sliding(2,2).foreach(println(_))
   输出
  List(1, 2)
List(3, 4)
List(5, 6)
   */
  @transient lazy val whenList =
    branches.sliding(2, 2).collect { case Seq(whenExpr, _) => whenExpr }.toSeq //获取when表达式集合
  @transient lazy val thenList =
    branches.sliding(2, 2).collect { case Seq(_, thenExpr) => thenExpr }.toSeq //获取then表达式集合
  val elseValue = if (branches.length % 2 == 0) None else Option(branches.last) //else 表达式

  // both then and else expressions should be considered.
  def valueTypes: Seq[DataType] = (thenList ++ elseValue).map(_.dataType) //返回值集合
  def valueTypesEqual: Boolean = valueTypes.distinct.size == 1 //是否返回值都是一样的类型

  //校验
  override def checkInputDataTypes(): TypeCheckResult = {
    if (valueTypesEqual) {//说明返回值类型一样
      checkTypesInternal() //子类具体实现
    } else {
      TypeCheckResult.TypeCheckFailure(//说明返回值类型不一样,抛异常
        "THEN and ELSE expressions should all be same type or coercible to a common type")
    }
  }

  protected def checkTypesInternal(): TypeCheckResult //子类实现

  override def dataType: DataType = thenList.head.dataType //返回值类型是一样的,因此任意一个都可以作为返回值

  override def nullable: Boolean = {
    // If no value is nullable and no elseValue is provided, the whole statement defaults to null.
    thenList.exists(_.nullable) || (elseValue.map(_.nullable).getOrElse(true))
  }
}

// scalastyle:off
/**
 * Case statements of the form "CASE WHEN a THEN b [WHEN c THEN d]* [ELSE e] END".
 * Refer to this link for the corresponding semantics:
 * https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF#LanguageManualUDF-ConditionalFunctions
 */
// scalastyle:on
//case when 表达式 then 表达式 else 表达式 end 操作
case class CaseWhen(branches: Seq[Expression]) extends CaseWhenLike {

  // Use private[this] Array to speed up evaluation.
  @transient private[this] lazy val branchesArr = branches.toArray //表达式对应的数组

  override def children: Seq[Expression] = branches //表达式集合

  //进行校验
  override protected def checkTypesInternal(): TypeCheckResult = {
    if (whenList.forall(_.dataType == BooleanType)) {//校验when表达式必须返回值是boolean
      TypeCheckResult.TypeCheckSuccess
    } else {//说明此时有非boolean类型的when
      val index = whenList.indexWhere(_.dataType != BooleanType)
      TypeCheckResult.TypeCheckFailure(
        s"WHEN expressions in CaseWhen should all be boolean type, " +
          s"but the ${index + 1}th when expression's type is ${whenList(index)}")
    }
  }

  /** Written in imperative fashion for performance considerations. */
  override def eval(input: InternalRow): Any = {
    val len = branchesArr.length //循环所有的表达式
    var i = 0
    // If all branches fail and an elseVal is not provided, the whole statement
    // defaults to null, according to Hive's semantics.
    while (i < len - 1) {
      if (branchesArr(i).eval(input) == true) { //循环所有的表达式,找到when是true的,即第一个true的,则值执行对应的then表达式
        return branchesArr(i + 1).eval(input)
      }
      i += 2
    }
    var res: Any = null
    if (i == len - 1) { //必须有else表达式,因此执行else表达式
      res = branchesArr(i).eval(input)
    }
    return res
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val len = branchesArr.length
    val got = ctx.freshName("got")

    val cases = (0 until len/2).map { i =>
      val cond = branchesArr(i * 2).gen(ctx)
      val res = branchesArr(i * 2 + 1).gen(ctx)
      s"""
        if (!$got) {
          ${cond.code}
          if (!${cond.isNull} && ${cond.primitive}) {
            $got = true;
            ${res.code}
            ${ev.isNull} = ${res.isNull};
            ${ev.primitive} = ${res.primitive};
          }
        }
      """
    }.mkString("\n")

    val other = if (len % 2 == 1) {
      val res = branchesArr(len - 1).gen(ctx)
      s"""
        if (!$got) {
          ${res.code}
          ${ev.isNull} = ${res.isNull};
          ${ev.primitive} = ${res.primitive};
        }
      """
    } else {
      ""
    }

    s"""
      boolean $got = false;
      boolean ${ev.isNull} = true;
      ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
      $cases
      $other
    """
  }

  override def toString: String = {
    "CASE" + branches.sliding(2, 2).map {
      case Seq(cond, value) => s" WHEN $cond THEN $value"
      case Seq(elseValue) => s" ELSE $elseValue"
    }.mkString
  }
}

// scalastyle:off
/**
 * Case statements of the form "CASE a WHEN b THEN c [WHEN d THEN e]* [ELSE f] END".
 * Refer to this link for the corresponding semantics:
 * https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF#LanguageManualUDF-ConditionalFunctions
 */
// scalastyle:on
//case 表达式 when 表达式 then 表达式 else 表达式 end 操作
case class CaseKeyWhen(key: Expression,//case后面的表达式
                       branches: Seq[Expression]) //when then else等表达式
      extends CaseWhenLike {

  // Use private[this] Array to speed up evaluation.
  @transient private[this] lazy val branchesArr = branches.toArray

  override def children: Seq[Expression] = key +: branches //总的表达式作为子节点

  override protected def checkTypesInternal(): TypeCheckResult = {
    if ((key +: whenList).map(_.dataType).distinct.size > 1) {//key和when表达式类型是一致的,因为when表达的就是key,所以必须要类型一致
      TypeCheckResult.TypeCheckFailure(
        "key and WHEN expressions should all be same type or coercible to a common type")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  //执行else
  private def evalElse(input: InternalRow): Any = {
    if (branchesArr.length % 2 == 0) {//说明没有else
      null
    } else {
      branchesArr(branchesArr.length - 1).eval(input)//说明有else,则执行else
    }
  }

  /** Written in imperative fashion for performance considerations. */
  override def eval(input: InternalRow): Any = {
    val evaluatedKey = key.eval(input) //获取key表达式对应的值
    // If key is null, we can just return the else part or null if there is no else.
    // If key is not null but doesn't match any when part, we need to return
    // the else part or null if there is no else, according to Hive's semantics.
    if (evaluatedKey != null) {
      val len = branchesArr.length
      var i = 0
      while (i < len - 1) {
        if (evaluatedKey ==  branchesArr(i).eval(input)) { //循环所有的when表达式,找到when的结果与key相同的
          return branchesArr(i + 1).eval(input) //则执行对应的then表达式
        }
        i += 2
      }
    }
    evalElse(input) //执行else表达式
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val keyEval = key.gen(ctx)
    val len = branchesArr.length
    val got = ctx.freshName("got")

    val cases = (0 until len/2).map { i =>
      val cond = branchesArr(i * 2).gen(ctx)
      val res = branchesArr(i * 2 + 1).gen(ctx)
      s"""
        if (!$got) {
          ${cond.code}
          if (!${cond.isNull} && ${ctx.genEqual(key.dataType, keyEval.primitive, cond.primitive)}) {
            $got = true;
            ${res.code}
            ${ev.isNull} = ${res.isNull};
            ${ev.primitive} = ${res.primitive};
          }
        }
      """
    }.mkString("\n")

    val other = if (len % 2 == 1) {
      val res = branchesArr(len - 1).gen(ctx)
      s"""
        if (!$got) {
          ${res.code}
          ${ev.isNull} = ${res.isNull};
          ${ev.primitive} = ${res.primitive};
        }
      """
    } else {
      ""
    }

    s"""
      boolean $got = false;
      boolean ${ev.isNull} = true;
      ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
      ${keyEval.code}
      if (!${keyEval.isNull}) {
        $cases
      }
      $other
    """
  }

  override def toString: String = {
    s"CASE $key" + branches.sliding(2, 2).map {
      case Seq(cond, value) => s" WHEN $cond THEN $value"
      case Seq(elseValue) => s" ELSE $elseValue"
    }.mkString
  }
}

/**
 * A function that returns the least value of all parameters, skipping null values.
 * It takes at least 2 parameters, and returns null iff all parameters are null.
 * 至少两个参数,如果所有参数都是null,则返回null,否则返回非null的最小的值
 */
case class Least(children: Seq[Expression]) extends Expression {

  override def nullable: Boolean = children.forall(_.nullable) //所有表达式都是null,才返回true
  override def foldable: Boolean = children.forall(_.foldable) //所有表达式都是静态的,才返回true

  private lazy val ordering = TypeUtils.getInterpretedOrdering(dataType) //返回值可以进行排序

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length <= 1) {//校验表达式参数必须大于1
      TypeCheckResult.TypeCheckFailure(s"LEAST requires at least 2 arguments")
    } else if (children.map(_.dataType).distinct.count(_ != NullType) > 1) {//表达式返回值都是相同的,或者是NullType
      TypeCheckResult.TypeCheckFailure(
        s"The expressions should all have the same type," +
          s" got LEAST (${children.map(_.dataType)}).")
    } else {
      TypeUtils.checkForOrderingExpr(dataType, "function " + prettyName) //校验返回值类型是支持排序的类型,否则抛异常
    }
  }

  override def dataType: DataType = children.head.dataType //第一个子节点对应的类型是返回值,因为所有子表达式都是相同类型的

  override def eval(input: InternalRow): Any = {
    children.foldLeft[Any](null)((r, c) => {
      val evalc = c.eval(input)
      if (evalc != null) { //说明当前表达式不是null
        if (r == null || ordering.lt(evalc, r)) evalc else r //如果是当前值小与r,因此说明当前值更小,因此返回当前值,否则说明当前值大,忽略当前值
      } else {//说明当前表达式是null,因此结果就是不要这个表达式的值,因此还是返回r
        r
      }
    })
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val evalChildren = children.map(_.gen(ctx))
    def updateEval(i: Int): String =
      s"""
        if (!${evalChildren(i).isNull} && (${ev.isNull} ||
          ${ctx.genComp(dataType, evalChildren(i).primitive, ev.primitive)} < 0)) {
          ${ev.isNull} = false;
          ${ev.primitive} = ${evalChildren(i).primitive};
        }
      """
    s"""
      ${evalChildren.map(_.code).mkString("\n")}
      boolean ${ev.isNull} = true;
      ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
      ${children.indices.map(updateEval).mkString("\n")}
    """
  }
}

/**
 * A function that returns the greatest value of all parameters, skipping null values.
 * It takes at least 2 parameters, and returns null iff all parameters are null.
 * 至少两个参数,如果所有参数都是null,则返回null,否则返回非null的最大的值
 */
case class Greatest(children: Seq[Expression]) extends Expression {

  override def nullable: Boolean = children.forall(_.nullable)
  override def foldable: Boolean = children.forall(_.foldable)

  private lazy val ordering = TypeUtils.getInterpretedOrdering(dataType)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length <= 1) {//校验表达式参数必须大于1
      TypeCheckResult.TypeCheckFailure(s"GREATEST requires at least 2 arguments")
    } else if (children.map(_.dataType).distinct.count(_ != NullType) > 1) {//表达式返回值都是相同的,或者是NullType
      TypeCheckResult.TypeCheckFailure(
        s"The expressions should all have the same type," +
          s" got GREATEST (${children.map(_.dataType)}).")
    } else {
      TypeUtils.checkForOrderingExpr(dataType, "function " + prettyName)  //校验返回值类型是支持排序的类型,否则抛异常
    }
  }

  override def dataType: DataType = children.head.dataType //返回值就是任意一个子表达式对应的返回值

  override def eval(input: InternalRow): Any = {
    children.foldLeft[Any](null)((r, c) => {//循环每一个表达式
      val evalc = c.eval(input) //返回该表达式对应的值
      if (evalc != null) {
        if (r == null || ordering.gt(evalc, r)) evalc else r //如果是当前值大与r,因此说明当前值更大,因此返回当前值,否则说明当前值小,忽略当前值
      } else {//说明当前表达式是null,因此结果就是不要这个表达式的值,因此还是返回r
        r
      }
    })
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val evalChildren = children.map(_.gen(ctx))
    def updateEval(i: Int): String =
      s"""
        if (!${evalChildren(i).isNull} && (${ev.isNull} ||
          ${ctx.genComp(dataType, evalChildren(i).primitive, ev.primitive)} > 0)) {
          ${ev.isNull} = false;
          ${ev.primitive} = ${evalChildren(i).primitive};
        }
      """
    s"""
      ${evalChildren.map(_.code).mkString("\n")}
      boolean ${ev.isNull} = true;
      ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
      ${children.indices.map(updateEval).mkString("\n")}
    """
  }
}
