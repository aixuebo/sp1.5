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
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval
//该类表示算数运算的表达式,即加减乘除运算


//- 表达式  将表达式的值转换成负数
case class UnaryMinus(child: Expression) extends UnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection.NumericAndInterval) //输入参数是数字类型和间隔类型,他们用于加减法

  override def dataType: DataType = child.dataType //返回类型与参数的类型相同

  override def toString: String = s"-$child"

  private lazy val numeric = TypeUtils.getNumeric(dataType) //将数据类型转换成Numeric类型

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = dataType match {
    case dt: DecimalType => defineCodeGen(ctx, ev, c => s"$c.unary_$$minus()")
    case dt: NumericType => nullSafeCodeGen(ctx, ev, eval => {
      val originValue = ctx.freshName("origin")
      // codegen would fail to compile if we just write (-($c))
      // for example, we could not write --9223372036854775808L in code
      s"""
        ${ctx.javaType(dt)} $originValue = (${ctx.javaType(dt)})($eval);
        ${ev.primitive} = (${ctx.javaType(dt)})(-($originValue));
      """})
    case dt: CalendarIntervalType => defineCodeGen(ctx, ev, c => s"$c.negate()")
  }

  protected override def nullSafeEval(input: Any): Any = {
    if (dataType.isInstanceOf[CalendarIntervalType]) { //时间类型
      input.asInstanceOf[CalendarInterval].negate()
    } else {
      numeric.negate(input) //将表达式的值转换成负数
    }
  }
}

//一元正数(不是数一定是正的,主要是带的是加法),即加法
case class UnaryPositive(child: Expression) extends UnaryExpression with ExpectsInputTypes {
  override def prettyName: String = "positive"

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection.NumericAndInterval) //输入参数是数字类型和间隔类型,他们用于加减法

  override def dataType: DataType = child.dataType //返回类型与参数的类型相同

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String =
    defineCodeGen(ctx, ev, c => c)

  /**
具体一元方式是子类实现
即一元表达式的具体子类只需要关注自己这一层接收的参数如何处理就可以。不需要关注参数怎么来的。
参数是具体的计算后的值。
   */
  protected override def nullSafeEval(input: Any): Any = input //返回表达式对应的值
}

/**
 * A function that get the absolute value of the numeric value.
 * 返回绝对值
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the absolute value of the numeric value",
  extended = "> SELECT _FUNC_('-1');\n1")
case class Abs(child: Expression) extends UnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  override def dataType: DataType = child.dataType

  private lazy val numeric = TypeUtils.getNumeric(dataType) //如何将数据转换成NumericType类型

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = dataType match {
    case dt: DecimalType =>
      defineCodeGen(ctx, ev, c => s"$c.abs()")
    case dt: NumericType =>
      defineCodeGen(ctx, ev, c => s"(${ctx.javaType(dt)})(java.lang.Math.abs($c))")
  }

  protected override def nullSafeEval(input: Any): Any = numeric.abs(input)
}

abstract class BinaryArithmetic extends BinaryOperator { //二元表达式

  override def dataType: DataType = left.dataType //返回值就是其中的元素类型

  override lazy val resolved = childrenResolved && checkInputDataTypes().isSuccess //进行校验参数类型

  /** Name of the function for this expression on a [[Decimal]] type. */
  def decimalMethod: String =
    sys.error("BinaryArithmetics must override either decimalMethod or genCode")

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = dataType match {
    case dt: DecimalType =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1.$decimalMethod($eval2)")
    // byte and short are casted into int when add, minus, times or divide
    case ByteType | ShortType =>
      defineCodeGen(ctx, ev,
        (eval1, eval2) => s"(${ctx.javaType(dataType)})($eval1 $symbol $eval2)")
    case _ =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1 $symbol $eval2")
  }
}

//拆成2个表达式对象
private[sql] object BinaryArithmetic {
  def unapply(e: BinaryArithmetic): Option[(Expression, Expression)] = Some((e.left, e.right))
}

//纯粹的加法
case class Add(left: Expression, right: Expression) extends BinaryArithmetic {

  override def inputType: AbstractDataType = TypeCollection.NumericAndInterval

  override def symbol: String = "+"

  private lazy val numeric = TypeUtils.getNumeric(dataType) //将数据类型转换成Numeric类型

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    if (dataType.isInstanceOf[CalendarIntervalType]) { //时间类型
      input1.asInstanceOf[CalendarInterval].add(input2.asInstanceOf[CalendarInterval])
    } else {
      numeric.plus(input1, input2) //加法
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = dataType match {
    case dt: DecimalType =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1.$$plus($eval2)")
    case ByteType | ShortType =>
      defineCodeGen(ctx, ev,
        (eval1, eval2) => s"(${ctx.javaType(dataType)})($eval1 $symbol $eval2)")
    case CalendarIntervalType =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1.add($eval2)")
    case _ =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1 $symbol $eval2")
  }
}

//减法
case class Subtract(left: Expression, right: Expression) extends BinaryArithmetic {

  override def inputType: AbstractDataType = TypeCollection.NumericAndInterval

  override def symbol: String = "-"

  private lazy val numeric = TypeUtils.getNumeric(dataType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    if (dataType.isInstanceOf[CalendarIntervalType]) {
      input1.asInstanceOf[CalendarInterval].subtract(input2.asInstanceOf[CalendarInterval])
    } else {
      numeric.minus(input1, input2)
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = dataType match {
    case dt: DecimalType =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1.$$minus($eval2)")
    case ByteType | ShortType =>
      defineCodeGen(ctx, ev,
        (eval1, eval2) => s"(${ctx.javaType(dataType)})($eval1 $symbol $eval2)")
    case CalendarIntervalType =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1.subtract($eval2)")
    case _ =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1 $symbol $eval2")
  }
}

//表达式乘法
case class Multiply(left: Expression, right: Expression) extends BinaryArithmetic {

  override def inputType: AbstractDataType = NumericType

  override def symbol: String = "*"
  override def decimalMethod: String = "$times"

  private lazy val numeric = TypeUtils.getNumeric(dataType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = numeric.times(input1, input2)
}

//表达式除法
case class Divide(left: Expression, right: Expression) extends BinaryArithmetic {

  override def inputType: AbstractDataType = NumericType

  override def symbol: String = "/"
  override def decimalMethod: String = "$div"
  override def nullable: Boolean = true

  //(Any, Any) => Any 表示二元函数 即该方法返回一个除法函数   执行过程是判断返回值是int还是浮点数
  private lazy val div: (Any, Any) => Any = dataType match {
    case ft: FractionalType => ft.fractional.asInstanceOf[Fractional[Any]].div
    case it: IntegralType => it.integral.asInstanceOf[Integral[Any]].quot
  }

  override def eval(input: InternalRow): Any = {
    val input2 = right.eval(input)
    if (input2 == null || input2 == 0) {//解决分母不是0的问题,直接返回null
      null
    } else {
      val input1 = left.eval(input)
      if (input1 == null) {
        null
      } else {
        div(input1, input2)//执行除法
      }
    }
  }

  /**
   * Special case handling due to division by 0 => null.
   */
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)
    val isZero = if (dataType.isInstanceOf[DecimalType]) {
      s"${eval2.primitive}.isZero()"
    } else {
      s"${eval2.primitive} == 0"
    }
    val javaType = ctx.javaType(dataType)
    val divide = if (dataType.isInstanceOf[DecimalType]) {
      s"${eval1.primitive}.$decimalMethod(${eval2.primitive})"
    } else {
      s"($javaType)(${eval1.primitive} $symbol ${eval2.primitive})"
    }
    s"""
      ${eval2.code}
      boolean ${ev.isNull} = false;
      $javaType ${ev.primitive} = ${ctx.defaultValue(javaType)};
      if (${eval2.isNull} || $isZero) {
        ${ev.isNull} = true;
      } else {
        ${eval1.code}
        if (${eval1.isNull}) {
          ${ev.isNull} = true;
        } else {
          ${ev.primitive} = $divide;
        }
      }
    """
  }
}

//%运算   比如5%3 = 2,即余数为2
case class Remainder(left: Expression, right: Expression) extends BinaryArithmetic {

  override def inputType: AbstractDataType = NumericType

  override def symbol: String = "%"
  override def decimalMethod: String = "remainder"
  override def nullable: Boolean = true

  //运算函数
  private lazy val integral = dataType match { //根据返回值不同,创建不同的运算方式
    case i: IntegralType => i.integral.asInstanceOf[Integral[Any]]
    case i: FractionalType => i.asIntegral.asInstanceOf[Integral[Any]]
  }

  override def eval(input: InternalRow): Any = {
    val input2 = right.eval(input)
    if (input2 == null || input2 == 0) {
      null
    } else {
      val input1 = left.eval(input)
      if (input1 == null) {
        null
      } else {
        integral.rem(input1, input2)
      }
    }
  }

  /**
   * Special case handling for x % 0 ==> null.
   */
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)
    val isZero = if (dataType.isInstanceOf[DecimalType]) {
      s"${eval2.primitive}.isZero()"
    } else {
      s"${eval2.primitive} == 0"
    }
    val javaType = ctx.javaType(dataType)
    val remainder = if (dataType.isInstanceOf[DecimalType]) {
      s"${eval1.primitive}.$decimalMethod(${eval2.primitive})"
    } else {
      s"($javaType)(${eval1.primitive} $symbol ${eval2.primitive})"
    }
    s"""
      ${eval2.code}
      boolean ${ev.isNull} = false;
      $javaType ${ev.primitive} = ${ctx.defaultValue(javaType)};
      if (${eval2.isNull} || $isZero) {
        ${ev.isNull} = true;
      } else {
        ${eval1.code}
        if (${eval1.isNull}) {
          ${ev.isNull} = true;
        } else {
          ${ev.primitive} = $remainder;
        }
      }
    """
  }
}

//返回两个数的较大的一个.如果是null,则返回另外一个,如果都是null,则返回null
case class MaxOf(left: Expression, right: Expression) extends BinaryArithmetic {
  // TODO: Remove MaxOf and MinOf, and replace its usage with Greatest and Least.

  override def inputType: AbstractDataType = TypeCollection.Ordered //数据数据是可以排序的类型

  override def nullable: Boolean = left.nullable && right.nullable

  private lazy val ordering = TypeUtils.getInterpretedOrdering(dataType) //返回值是可以排序的类型

  override def eval(input: InternalRow): Any = {
    val input1 = left.eval(input)
    val input2 = right.eval(input)
    if (input1 == null) {
      input2
    } else if (input2 == null) {
      input1
    } else {
      if (ordering.compare(input1, input2) < 0) {
        input2
      } else {
        input1
      }
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)
    val compCode = ctx.genComp(dataType, eval1.primitive, eval2.primitive)

    eval1.code + eval2.code + s"""
      boolean ${ev.isNull} = false;
      ${ctx.javaType(left.dataType)} ${ev.primitive} =
        ${ctx.defaultValue(left.dataType)};

      if (${eval1.isNull}) {
        ${ev.isNull} = ${eval2.isNull};
        ${ev.primitive} = ${eval2.primitive};
      } else if (${eval2.isNull}) {
        ${ev.isNull} = ${eval1.isNull};
        ${ev.primitive} = ${eval1.primitive};
      } else {
        if ($compCode > 0) {
          ${ev.primitive} = ${eval1.primitive};
        } else {
          ${ev.primitive} = ${eval2.primitive};
        }
      }
    """
  }

  override def symbol: String = "max"
}

//返回两个数的较小的一个.如果是null,则返回另外一个,如果都是null,则返回null
case class MinOf(left: Expression, right: Expression) extends BinaryArithmetic {
  // TODO: Remove MaxOf and MinOf, and replace its usage with Greatest and Least.

  override def inputType: AbstractDataType = TypeCollection.Ordered

  override def nullable: Boolean = left.nullable && right.nullable

  private lazy val ordering = TypeUtils.getInterpretedOrdering(dataType)

  override def eval(input: InternalRow): Any = {
    val input1 = left.eval(input)
    val input2 = right.eval(input)
    if (input1 == null) {
      input2
    } else if (input2 == null) {
      input1
    } else {
      if (ordering.compare(input1, input2) < 0) {
        input1
      } else {
        input2
      }
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)
    val compCode = ctx.genComp(dataType, eval1.primitive, eval2.primitive)

    eval1.code + eval2.code + s"""
      boolean ${ev.isNull} = false;
      ${ctx.javaType(left.dataType)} ${ev.primitive} =
        ${ctx.defaultValue(left.dataType)};

      if (${eval1.isNull}) {
        ${ev.isNull} = ${eval2.isNull};
        ${ev.primitive} = ${eval2.primitive};
      } else if (${eval2.isNull}) {
        ${ev.isNull} = ${eval1.isNull};
        ${ev.primitive} = ${eval1.primitive};
      } else {
        if ($compCode < 0) {
          ${ev.primitive} = ${eval1.primitive};
        } else {
          ${ev.primitive} = ${eval2.primitive};
        }
      }
    """
  }

  override def symbol: String = "min"
}

case class Pmod(left: Expression, right: Expression) extends BinaryArithmetic {

  override def toString: String = s"pmod($left, $right)"

  override def symbol: String = "pmod"

  //判断类型是数字类型或者null类型
  protected def checkTypesInternal(t: DataType) =
    TypeUtils.checkForNumericExpr(t, "pmod")

  override def inputType: AbstractDataType = NumericType //输入类型是数字类型

  protected override def nullSafeEval(left: Any, right: Any) =
    dataType match {//根据返回值类型不同,创建不同的pmod方法
      case IntegerType => pmod(left.asInstanceOf[Int], right.asInstanceOf[Int])
      case LongType => pmod(left.asInstanceOf[Long], right.asInstanceOf[Long])
      case ShortType => pmod(left.asInstanceOf[Short], right.asInstanceOf[Short])
      case ByteType => pmod(left.asInstanceOf[Byte], right.asInstanceOf[Byte])
      case FloatType => pmod(left.asInstanceOf[Float], right.asInstanceOf[Float])
      case DoubleType => pmod(left.asInstanceOf[Double], right.asInstanceOf[Double])
      case _: DecimalType => pmod(left.asInstanceOf[Decimal], right.asInstanceOf[Decimal])
    }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      dataType match {
        case dt: DecimalType =>
          val decimalAdd = "$plus"
          s"""
            ${ctx.javaType(dataType)} r = $eval1.remainder($eval2);
            if (r.compare(new org.apache.spark.sql.types.Decimal().set(0)) < 0) {
              ${ev.primitive} = (r.$decimalAdd($eval2)).remainder($eval2);
            } else {
              ${ev.primitive} = r;
            }
          """
        // byte and short are casted into int when add, minus, times or divide
        case ByteType | ShortType =>
          s"""
            ${ctx.javaType(dataType)} r = (${ctx.javaType(dataType)})($eval1 % $eval2);
            if (r < 0) {
              ${ev.primitive} = (${ctx.javaType(dataType)})((r + $eval2) % $eval2);
            } else {
              ${ev.primitive} = r;
            }
          """
        case _ =>
          s"""
            ${ctx.javaType(dataType)} r = $eval1 % $eval2;
            if (r < 0) {
              ${ev.primitive} = (r + $eval2) % $eval2;
            } else {
              ${ev.primitive} = r;
            }
          """
      }
    })
  }

  private def pmod(a: Int, n: Int): Int = {
    val r = a % n //计算余数,比如5 % 3 = 2,即余数是2
    if (r < 0) {(r + n) % n} else r
  }

  private def pmod(a: Long, n: Long): Long = {
    val r = a % n
    if (r < 0) {(r + n) % n} else r
  }

  private def pmod(a: Byte, n: Byte): Byte = {
    val r = a % n
    if (r < 0) {((r + n) % n).toByte} else r.toByte
  }

  private def pmod(a: Double, n: Double): Double = {
    val r = a % n
    if (r < 0) {(r + n) % n} else r
  }

  private def pmod(a: Short, n: Short): Short = {
    val r = a % n
    if (r < 0) {((r + n) % n).toShort} else r.toShort
  }

  private def pmod(a: Float, n: Float): Float = {
    val r = a % n
    if (r < 0) {(r + n) % n} else r
  }

  private def pmod(a: Decimal, n: Decimal): Decimal = {
    val r = a % n
    if (r.compare(Decimal.ZERO) < 0) {(r + n) % n} else r
  }
}
