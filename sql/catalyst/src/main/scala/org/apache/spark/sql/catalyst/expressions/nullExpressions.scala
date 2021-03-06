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
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenContext, GeneratedExpressionCode}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._


/**
 * An expression that is evaluated to the first non-null input.
 * 返回第一个非null的表达式
 * {{{
 *   coalesce(1, 2) => 1
 *   coalesce(null, 1, 2) => 1
 *   coalesce(null, null, 2) => 2
 *   coalesce(null, null, null) => null
 * }}}
 */
case class Coalesce(children: Seq[Expression]) extends Expression {

  /** Coalesce is nullable if all of its children are nullable, or if it has no children. */
  override def nullable: Boolean = children.forall(_.nullable) //所有子表达式都是null,则该值才是null,才返回true

  // Coalesce is foldable if all children are foldable.
  override def foldable: Boolean = children.forall(_.foldable) //所有表达式都是静态的,该表达式才是静态的

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children == Nil) {
      TypeCheckResult.TypeCheckFailure("input to function coalesce cannot be empty")
    } else {
      TypeUtils.checkForSameTypeInputExpr(children.map(_.dataType), "function coalesce") //校验参数type类型必须是一样的
    }
  }

  override def dataType: DataType = children.head.dataType //返回值类型

  override def eval(input: InternalRow): Any = {
    var result: Any = null
    val childIterator = children.iterator //对每一个子表达式进行运算,返回第一个不是null的数据
    while (childIterator.hasNext && result == null) {
      result = childIterator.next().eval(input)
    }
    result
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    s"""
      boolean ${ev.isNull} = true;
      ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
    """ +
    children.map { e =>
      val eval = e.gen(ctx)
      s"""
        if (${ev.isNull}) {
          ${eval.code}
          if (!${eval.isNull}) {
            ${ev.isNull} = false;
            ${ev.primitive} = ${eval.primitive};
          }
        }
      """
    }.mkString("\n")
  }
}


/**
 * Evaluates to `true` iff it's NaN.
 * 对double或者float类型进行判断是否是NaN类型
 * true表示是NaN
 */
case class IsNaN(child: Expression) extends UnaryExpression
  with Predicate //返回类型必须是boolean类型
  with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(DoubleType, FloatType)) //输入是小数类型

  override def nullable: Boolean = false //不能是null

  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      false
    } else {
      child.dataType match {
        case DoubleType => value.asInstanceOf[Double].isNaN //返回boolean类型
        case FloatType => value.asInstanceOf[Float].isNaN
      }
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval = child.gen(ctx)
    child.dataType match {
      case DoubleType | FloatType =>
        s"""
          ${eval.code}
          boolean ${ev.isNull} = false;
          ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
          ${ev.primitive} = !${eval.isNull} && Double.isNaN(${eval.primitive});
        """
    }
  }
}

/**
 * An Expression evaluates to `left` iff it's not NaN, or evaluates to `right` otherwise.
 * This Expression is useful for mapping NaN values to null.
 * 先计算左边表达式,再计算右边,返回第一个非Nan的表达式对应的值
 */
case class NaNvl(left: Expression, right: Expression)
    extends BinaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = left.dataType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(DoubleType, FloatType), TypeCollection(DoubleType, FloatType)) //两个表达式参数必须是小数类型

  //将表达式转换成小数
  override def eval(input: InternalRow): Any = {
    val value = left.eval(input)
    if (value == null) {
      null
    } else {
      left.dataType match {
        case DoubleType =>
          if (!value.asInstanceOf[Double].isNaN) value else right.eval(input)
        case FloatType =>
          if (!value.asInstanceOf[Float].isNaN) value else right.eval(input)
      }
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val leftGen = left.gen(ctx)
    val rightGen = right.gen(ctx)
    left.dataType match {
      case DoubleType | FloatType =>
        s"""
          ${leftGen.code}
          boolean ${ev.isNull} = false;
          ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
          if (${leftGen.isNull}) {
            ${ev.isNull} = true;
          } else {
            if (!Double.isNaN(${leftGen.primitive})) {
              ${ev.primitive} = ${leftGen.primitive};
            } else {
              ${rightGen.code}
              if (${rightGen.isNull}) {
                ${ev.isNull} = true;
              } else {
                ${ev.primitive} = ${rightGen.primitive};
              }
            }
          }
        """
    }
  }
}


/**
 * An expression that is evaluated to true if the input is null.
 * 表达式 IS null
 */
case class IsNull(child: Expression) extends UnaryExpression
  with Predicate { //返回类型必须是boolean类型
  override def nullable: Boolean = false

  //判断表达式的结果是否是null
  override def eval(input: InternalRow): Any = {
    child.eval(input) == null
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval = child.gen(ctx)
    ev.isNull = "false"
    ev.primitive = eval.isNull
    eval.code
  }
}


/**
 * An expression that is evaluated to true if the input is not null.
 * 表达式 IS not null
 */
case class IsNotNull(child: Expression) extends UnaryExpression
  with Predicate { //返回类型必须是boolean类型
  override def nullable: Boolean = false

  //确保表达式对应的值不是null,就返回true
  override def eval(input: InternalRow): Any = {
    child.eval(input) != null
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval = child.gen(ctx)
    ev.isNull = "false"
    ev.primitive = s"(!(${eval.isNull}))"
    eval.code
  }
}


/**
 * A predicate that is evaluated to be true if there are at least `n` non-null and non-NaN values.
 * 判断至少有n个非null的子表达式存在,则返回true
 * 注意:表达式当是double和float的时候,非null表示非NaN
 */
case class AtLeastNNonNulls(n: Int, children: Seq[Expression])
  extends Predicate { //返回类型必须是boolean类型

  override def nullable: Boolean = false
  override def foldable: Boolean = children.forall(_.foldable) //判断所有子表达式都是静态的,则结果是静态的

  override def toString: String = s"AtLeastNNulls(n, ${children.mkString(",")})"

  private[this] val childrenArray = children.toArray

  override def eval(input: InternalRow): Boolean = {
    var numNonNulls = 0 //非null的表达式数量
    var i = 0
    while (i < childrenArray.length && numNonNulls < n) {
      val evalC = childrenArray(i).eval(input) //返回每一个表达式对应的值
      if (evalC != null) {
        childrenArray(i).dataType match {//表达式当是double和float的时候,非null表示非NaN
          case DoubleType =>
            if (!evalC.asInstanceOf[Double].isNaN) numNonNulls += 1
          case FloatType =>
            if (!evalC.asInstanceOf[Float].isNaN) numNonNulls += 1
          case _ => numNonNulls += 1
        }
      }
      i += 1
    }
    numNonNulls >= n
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val nonnull = ctx.freshName("nonnull")
    val code = children.map { e =>
      val eval = e.gen(ctx)
      e.dataType match {
        case DoubleType | FloatType =>
          s"""
            if ($nonnull < $n) {
              ${eval.code}
              if (!${eval.isNull} && !Double.isNaN(${eval.primitive})) {
                $nonnull += 1;
              }
            }
          """
        case _ =>
          s"""
            if ($nonnull < $n) {
              ${eval.code}
              if (!${eval.isNull}) {
                $nonnull += 1;
              }
            }
          """
      }
    }.mkString("\n")
    s"""
      int $nonnull = 0;
      $code
      boolean ${ev.isNull} = false;
      boolean ${ev.primitive} = $nonnull >= $n;
     """
  }
}
