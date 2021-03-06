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
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateSafeProjection, GenerateUnsafeProjection}
import org.apache.spark.sql.types.{DataType, Decimal, StructType, _}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * A [[Projection]] that is calculated by calling the `eval` of each of the specified expressions.
 * @param expressions a sequence of expressions that determine the value of each column of the
 *                    output row.
 * 参数是一组表达式,返回值是每一个表达式对当前行的处理结果,因此又组成了一行新的数据,即用表达式结果组成的新记录
 *
 * 表达式是BoundReference
 */
class InterpretedProjection(expressions: Seq[Expression]) extends Projection {

  //第一个参数是select中的表达式,第二个参数是from表内的所有字段的schema
  def this(expressions: Seq[Expression], inputSchema: Seq[Attribute]) = this(expressions.map(BindReferences.bindReference(_, inputSchema)))


  //循环所有的表达式,对不确定性的表达式 进行初始化,其他的保持(因为其他的不需要初始化)
  expressions.foreach(_.foreach {
    case n: Nondeterministic => n.setInitialValues() //对不确定性的表达式 进行初始化
    case _ =>
  })

  // null check is required for when Kryo invokes the no-arg constructor.
  //返回表达式数组
  protected val exprArray = if (expressions != null) expressions.toArray else null

  //一行数据过来,如何处理
  def apply(input: InternalRow): InternalRow = {
    val outputArray = new Array[Any](exprArray.length) //每一个表达式的返回值
    var i = 0
    while (i < exprArray.length) {//循环每一个表达式,每一个表达式都对当前行数据进行处理
      outputArray(i) = exprArray(i).eval(input)
      i += 1
    }
    new GenericInternalRow(outputArray) //转换成一行具体的值
  }

  override def toString(): String = s"Row => [${exprArray.mkString(",")}]"
}

/**
 * A [[MutableProjection]] that is calculated by calling `eval` on each of the specified
 * expressions.
 * @param expressions a sequence of expressions that determine the value of each column of the
 *                    output row.
 */
case class InterpretedMutableProjection(expressions: Seq[Expression]) extends MutableProjection {

  def this(expressions: Seq[Expression], inputSchema: Seq[Attribute]) = this(expressions.map(BindReferences.bindReference(_, inputSchema)))

  //循环所有的表达式,对不确定性的表达式 进行初始化,其他的保持(因为其他的不需要初始化)
  expressions.foreach(_.foreach {
    case n: Nondeterministic => n.setInitialValues()
    case _ =>
  })

  private[this] val exprArray = expressions.toArray //表达式数组
  private[this] var mutableRow: MutableRow = new GenericMutableRow(exprArray.length) //可变的row行.该行的内容是每一个表达式的输出结果

  def currentValue: InternalRow = mutableRow

  override def target(row: MutableRow): MutableProjection = {
    mutableRow = row
    this
  }

  override def apply(input: InternalRow): InternalRow = {
    var i = 0
    while (i < exprArray.length) {
      mutableRow(i) = exprArray(i).eval(input) //对每一个表达式运算,输出结果
      i += 1
    }
    mutableRow
  }
}

/**
 * A projection that returns UnsafeRow.
 */
abstract class UnsafeProjection extends Projection {
  override def apply(row: InternalRow): UnsafeRow
}

object UnsafeProjection {

  /*
   * Returns whether UnsafeProjection can support given StructType, Array[DataType] or
   * Seq[Expression].
   */
  def canSupport(schema: StructType): Boolean = canSupport(schema.fields.map(_.dataType))
  def canSupport(exprs: Seq[Expression]): Boolean = canSupport(exprs.map(_.dataType).toArray)

  //字段类型集合
  private def canSupport(types: Array[DataType]): Boolean = {
    types.forall(GenerateUnsafeProjection.canSupport)
  }

  /**
   * Returns an UnsafeProjection for given StructType.
   */
  def create(schema: StructType): UnsafeProjection = create(schema.fields.map(_.dataType))

  /**
   * Returns an UnsafeProjection for given Array of DataTypes.
   */
  def create(fields: Array[DataType]): UnsafeProjection = {
    create(fields.zipWithIndex.map(x => new BoundReference(x._2, x._1, true)))
  }

  /**
   * Returns an UnsafeProjection for given sequence of Expressions (bounded).
   */
  def create(exprs: Seq[Expression]): UnsafeProjection = {
    GenerateUnsafeProjection.generate(exprs)
  }

  def create(expr: Expression): UnsafeProjection = create(Seq(expr))

  /**
   * Returns an UnsafeProjection for given sequence of Expressions, which will be bound to
   * `inputSchema`.
   */
  def create(exprs: Seq[Expression], inputSchema: Seq[Attribute]): UnsafeProjection = {
    create(exprs.map(BindReferences.bindReference(_, inputSchema)))
  }
}

/**
 * A projection that could turn UnsafeRow into GenericInternalRow
 */
object FromUnsafeProjection {

  /**
   * Returns an Projection for given StructType.
   */
  def apply(schema: StructType): Projection = {
    apply(schema.fields.map(_.dataType))
  }

  /**
   * Returns an UnsafeProjection for given Array of DataTypes.
   */
  def apply(fields: Seq[DataType]): Projection = {
    create(fields.zipWithIndex.map(x => {
      new BoundReference(x._2, x._1, true)
    }))
  }

  /**
   * Returns an Projection for given sequence of Expressions (bounded).
   */
  private def create(exprs: Seq[Expression]): Projection = {
    GenerateSafeProjection.generate(exprs)
  }
}
