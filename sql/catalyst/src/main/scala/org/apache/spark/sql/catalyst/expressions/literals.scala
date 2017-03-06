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

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types._

//文字对象由值和类型组成
object Literal {
  //参数v就是具体的值
  def apply(v: Any): Literal = v match {
    case i: Int => Literal(i, IntegerType) //如果v是int类型的,则创建IntegerType类型,值是v的Literal对象
    case l: Long => Literal(l, LongType)
    case d: Double => Literal(d, DoubleType)
    case f: Float => Literal(f, FloatType)
    case b: Byte => Literal(b, ByteType)
    case s: Short => Literal(s, ShortType)
    case s: String => Literal(UTF8String.fromString(s), StringType)
    case b: Boolean => Literal(b, BooleanType)
    case d: BigDecimal => Literal(Decimal(d), DecimalType(Math.max(d.precision, d.scale), d.scale))
    case d: java.math.BigDecimal =>
      Literal(Decimal(d), DecimalType(Math.max(d.precision, d.scale), d.scale()))
    case d: Decimal => Literal(d, DecimalType(Math.max(d.precision, d.scale), d.scale))
    case t: Timestamp => Literal(DateTimeUtils.fromJavaTimestamp(t), TimestampType)
    case d: Date => Literal(DateTimeUtils.fromJavaDate(d), DateType)
    case a: Array[Byte] => Literal(a, BinaryType)
    case i: CalendarInterval => Literal(i, CalendarIntervalType)
    case null => Literal(null, NullType)
    case _ =>
      throw new RuntimeException("Unsupported literal type " + v.getClass + " " + v)
  }

  def create(v: Any, dataType: DataType): Literal = {
    Literal(CatalystTypeConverters.convertToCatalyst(v), dataType)
  }
}

/**
 * An extractor that matches non-null literal values
 * 抽取非null的属性和属性值
 */
object NonNullLiteral {
  def unapply(literal: Literal): Option[(Any, DataType)] = {
    Option(literal.value).map(_ => (literal.value, literal.dataType))
  }
}

/**
 * Extractor for retrieving Int literals.
 */
object IntegerLiteral {
  //从Literal对象中抽取int值
  def unapply(a: Any): Option[Int] = a match {
    case Literal(a: Int, IntegerType) => Some(a)
    case _ => None
  }
}

/**
 * In order to do type checking, use Literal.create() instead of constructor
 * 由属性值和属性类型组成的对象
 * 表示该叶子节点就是一个具体的值
 */
case class Literal protected (value: Any, dataType: DataType)
  extends LeafExpression with CodegenFallback {

  override def foldable: Boolean = true //该值肯定是静态的,因此返回true
  override def nullable: Boolean = value == null //判断value是否是null

  override def toString: String = if (value != null) value.toString else "null"

  override def equals(other: Any): Boolean = other match {
    case o: Literal =>
      dataType.equals(o.dataType) &&
        (value == null && null == o.value || value != null && value.equals(o.value))
    case _ => false
  }

  override def eval(input: InternalRow): Any = value

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    // change the isNull and primitive to consts, to inline them
    if (value == null) {
      ev.isNull = "true"
      s"final ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};"
    } else {
      dataType match {
        case BooleanType =>
          ev.isNull = "false"
          ev.primitive = value.toString
          ""
        case FloatType =>
          val v = value.asInstanceOf[Float]
          if (v.isNaN || v.isInfinite) {
            super.genCode(ctx, ev)
          } else {
            ev.isNull = "false"
            ev.primitive = s"${value}f"
            ""
          }
        case DoubleType =>
          val v = value.asInstanceOf[Double]
          if (v.isNaN || v.isInfinite) {
            super.genCode(ctx, ev)
          } else {
            ev.isNull = "false"
            ev.primitive = s"${value}D"
            ""
          }
        case ByteType | ShortType =>
          ev.isNull = "false"
          ev.primitive = s"(${ctx.javaType(dataType)})$value"
          ""
        case IntegerType | DateType =>
          ev.isNull = "false"
          ev.primitive = value.toString
          ""
        case TimestampType | LongType =>
          ev.isNull = "false"
          ev.primitive = s"${value}L"
          ""
        // eval() version may be faster for non-primitive types
        case other =>
          super.genCode(ctx, ev)
      }
    }
  }
}

// TODO: Specialize
//可以变化的属性值
case class MutableLiteral(var value: Any, dataType: DataType, nullable: Boolean = true)
  extends LeafExpression with CodegenFallback {

  //表达式expression处理数据行内容InternalRow,返回一个value值
  def update(expression: Expression, input: InternalRow): Unit = {
    value = expression.eval(input)
  }

  //对一个数据行进行处理,返回一个value值
  override def eval(input: InternalRow): Any = value
}
