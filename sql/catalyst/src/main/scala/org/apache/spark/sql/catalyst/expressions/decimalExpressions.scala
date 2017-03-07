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
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenContext, GeneratedExpressionCode}
import org.apache.spark.sql.types._

//如何转换Decimal类型
/**
 * Return the unscaled Long value of a Decimal, assuming it fits in a Long.
 * Note: this expression is internal and created only by the optimizer,
 * we don't need to do type check for it.
 * 将Decimal类型的输入转换成long类型
 */
case class UnscaledValue(child: Expression) extends UnaryExpression {

  override def dataType: DataType = LongType
  override def toString: String = s"UnscaledValue($child)"

  protected override def nullSafeEval(input: Any): Any =
    input.asInstanceOf[Decimal].toUnscaledLong

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c => s"$c.toUnscaledLong()")
  }
}

/**
 * Create a Decimal from an unscaled Long value.
 * Note: this expression is internal and created only by the optimizer,
 * we don't need to do type check for it.
 * 返回一个Decimal类型数据
 * 输入表达式返回的是long类型
 *
 * precision 表示 字段长度
 * scale 表示 小数位数
 * 例如：-4.75, precision=3，scale=2，
 */
case class MakeDecimal(child: Expression, precision: Int, scale: Int) extends UnaryExpression {

  override def dataType: DataType = DecimalType(precision, scale)
  override def toString: String = s"MakeDecimal($child,$precision,$scale)"

  protected override def nullSafeEval(input: Any): Any =
    Decimal(input.asInstanceOf[Long], precision, scale)

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, eval => {
      s"""
        ${ev.primitive} = (new Decimal()).setOrNull($eval, $precision, $scale);
        ${ev.isNull} = ${ev.primitive} == null;
      """
    })
  }
}

/**
 * An expression used to wrap the children when promote the precision of DecimalType to avoid
 * promote multiple times.
 * 提高精度
 */
case class PromotePrecision(child: Expression) extends UnaryExpression {
  override def dataType: DataType = child.dataType //返回值还是参数的返回值.因为他的目标就是提高精度.不改变原类型
  override def eval(input: InternalRow): Any = child.eval(input)
  override def gen(ctx: CodeGenContext): GeneratedExpressionCode = child.gen(ctx)
  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = ""
  override def prettyName: String = "promote_precision"
}

/**
 * Rounds the decimal to given scale and check whether the decimal can fit in provided precision
 * or not, returns null if not.
 * 更改child表达式对应的精度和小数点位数
 */
case class CheckOverflow(child: Expression, dataType: DecimalType) extends UnaryExpression {

  override def nullable: Boolean = true

  override def nullSafeEval(input: Any): Any = {
    val d = input.asInstanceOf[Decimal].clone() //复制原有的小数
    if (d.changePrecision(dataType.precision, dataType.scale)) {//更改child表达式对应的精度和小数点位数
      d
    } else {
      null
    }
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, eval => {
      val tmp = ctx.freshName("tmp")
      s"""
         | Decimal $tmp = $eval.clone();
         | if ($tmp.changePrecision(${dataType.precision}, ${dataType.scale})) {
         |   ${ev.primitive} = $tmp;
         | } else {
         |   ${ev.isNull} = true;
         | }
       """.stripMargin
    })
  }

  override def toString: String = s"CheckOverflow($child, $dataType)"
}
