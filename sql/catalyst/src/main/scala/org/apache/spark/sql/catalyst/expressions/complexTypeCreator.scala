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

import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._

//该类用于创建复杂的对象

/**
 * Returns an Array containing the evaluation of all children expressions.
 * 创建一个数组,所有子表达式返回的值是相同类型的,因此返回值组成了一个数组
 */
case class CreateArray(children: Seq[Expression]) extends Expression {

  override def foldable: Boolean = children.forall(_.foldable) //所有子表达式都是静态的时候返回true

  //校验参数type类型必须是一样的,因为是数组,所以必须是相同的类型
  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForSameTypeInputExpr(children.map(_.dataType), "function array")

  //返回值类型树数组,元素的类型就是第一个元素对应的类型
  override def dataType: DataType = {
    ArrayType(
      children.headOption.map(_.dataType).getOrElse(NullType),//第一个元素对应的类型
      containsNull = children.exists(_.nullable)) //是否允许有null
  }

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    new GenericArrayData(children.map(_.eval(input)).toArray) //每一个表达式返回一个值,然后组成数组
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val arrayClass = classOf[GenericArrayData].getName
    s"""
      final boolean ${ev.isNull} = false;
      final Object[] values = new Object[${children.size}];
    """ +
      children.zipWithIndex.map { case (e, i) =>
        val eval = e.gen(ctx)
        eval.code + s"""
          if (${eval.isNull}) {
            values[$i] = null;
          } else {
            values[$i] = ${eval.primitive};
          }
         """
      }.mkString("\n") +
      s"final ${ctx.javaType(dataType)} ${ev.primitive} = new $arrayClass(values);"
  }

  override def prettyName: String = "array"
}

/**
 * Returns a Row containing the evaluation of all children expressions.
 * 创建一个struct对象,每一个表达式返回的值组成了一个struct的一个属性
 */
case class CreateStruct(children: Seq[Expression]) extends Expression {

  override def foldable: Boolean = children.forall(_.foldable)

  //返回值一个struct对象,每一个表达式组成了一个属性
  override lazy val dataType: StructType = {
    val fields = children.zipWithIndex.map { case (child, idx) => //循环所有的表达式,以及序号
      child match {
        case ne: NamedExpression => //如是不确定性的表达式,要先进行初始化操作
          StructField(ne.name, ne.dataType, ne.nullable, ne.metadata)
        case _ =>
          StructField(s"col${idx + 1}", child.dataType, child.nullable, Metadata.empty) //属性的name就是col+序号,序号从1开始,以及数据类型就是该表达式的返回值类型,是否为null,以及元数据
      }
    }
    StructType(fields)
  }

  override def nullable: Boolean = false

  //对应的表达式值的集合就组成一个InternalRow行对象
  override def eval(input: InternalRow): Any = {
    InternalRow(children.map(_.eval(input)): _*)
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val rowClass = classOf[GenericMutableRow].getName
    s"""
      boolean ${ev.isNull} = false;
      final $rowClass ${ev.primitive} = new $rowClass(${children.size});
    """ +
      children.zipWithIndex.map { case (e, i) =>
        val eval = e.gen(ctx)
        eval.code + s"""
          if (${eval.isNull}) {
            ${ev.primitive}.update($i, null);
          } else {
            ${ev.primitive}.update($i, ${eval.primitive});
          }
         """
      }.mkString("\n")
  }

  override def prettyName: String = "struct"
}


/**
 * Creates a struct with the given field names and values
 * 使用name和value构建一个struct对象
 * @param children Seq(name1, val1, name2, val2, ...) 参数表达式一个key 一个value方式进行的
 *
 */
case class CreateNamedStruct(children: Seq[Expression]) extends Expression {

  //grouped(2)相当于sliding(2,2),即每隔2个做一个分组
  private lazy val (nameExprs, valExprs) =
    children.grouped(2).map { case Seq(name, value) => (name, value) }.toList.unzip //name,value的元组集合,然后进行unzip,即name的组合在一起,value的表达式组合在一起

  private lazy val names = nameExprs.map(_.eval(EmptyRow))

  //组成一个Struct对象
  override lazy val dataType: StructType = {

    val fields = names.zip(valExprs).map { case (name, valExpr) =>
      StructField(name.asInstanceOf[UTF8String].toString,
        valExpr.dataType, valExpr.nullable, Metadata.empty) //name就是第一个表达式,value类型和是否是null则从第二个表达式出
    }
    StructType(fields)
  }

  override def foldable: Boolean = valExprs.forall(_.foldable)

  override def nullable: Boolean = false

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.size % 2 != 0) { //校验必须子表达式是2的倍数
      TypeCheckResult.TypeCheckFailure(s"$prettyName expects an even number of arguments.")
    } else {
      val invalidNames = nameExprs.filterNot(e => e.foldable && e.dataType == StringType)//过滤name表达式不是静态的,也不是字符串的
      if (invalidNames.nonEmpty) {//如果还有其他类型的,是不允许的,因为name就是字符串和静态两种表达式可以当作name
        TypeCheckResult.TypeCheckFailure(
          s"Only foldable StringType expressions are allowed to appear at odd position , got :" +
            s" ${invalidNames.mkString(",")}")
      } else if (names.forall(_ != null)){//name都不允许是null,即不能用null表示名字
        TypeCheckResult.TypeCheckSuccess
      } else {
        TypeCheckResult.TypeCheckFailure("Field name should not be null")
      }
    }
  }

  //用值表达式解析,产生一行数据
  override def eval(input: InternalRow): Any = {
    InternalRow(valExprs.map(_.eval(input)): _*)
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val rowClass = classOf[GenericMutableRow].getName
    s"""
      boolean ${ev.isNull} = false;
      final $rowClass ${ev.primitive} = new $rowClass(${valExprs.size});
    """ +
      valExprs.zipWithIndex.map { case (e, i) =>
        val eval = e.gen(ctx)
        eval.code + s"""
          if (${eval.isNull}) {
            ${ev.primitive}.update($i, null);
          } else {
            ${ev.primitive}.update($i, ${eval.primitive});
          }
         """
      }.mkString("\n")
  }

  override def prettyName: String = "named_struct"
}

/**
 * Returns a Row containing the evaluation of all children expressions. This is a variant that
 * returns UnsafeRow directly. The unsafe projection operator replaces [[CreateStruct]] with
 * this expression automatically at runtime.
 * 创建一个不安全的Struct,所谓不安全的是因为只是校验了子类,没有校验本身数据参数是否正确
 */
case class CreateStructUnsafe(children: Seq[Expression]) extends Expression {

  override def foldable: Boolean = children.forall(_.foldable)

  override lazy val resolved: Boolean = childrenResolved //说明所有子类的校验参数已经校验完成

  //表达式集合组成了一个StructType对象,即每一个表达式都是一个属性
  override lazy val dataType: StructType = {
    val fields = children.zipWithIndex.map { case (child, idx) =>
      child match {
        case ne: NamedExpression =>
          StructField(ne.name, ne.dataType, ne.nullable, ne.metadata)
        case _ =>
          StructField(s"col${idx + 1}", child.dataType, child.nullable, Metadata.empty)
      }
    }
    StructType(fields)
  }

  override def nullable: Boolean = false

  //解析表达式返回一个InternalRow对象
  override def eval(input: InternalRow): Any = {
    InternalRow(children.map(_.eval(input)): _*)
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval = GenerateUnsafeProjection.createCode(ctx, children)
    ev.isNull = eval.isNull
    ev.primitive = eval.primitive
    eval.code
  }

  override def prettyName: String = "struct_unsafe"
}


/**
 * Creates a struct with the given field names and values. This is a variant that returns
 * UnsafeRow directly. The unsafe projection operator replaces [[CreateStruct]] with
 * this expression automatically at runtime.
 *
 * @param children Seq(name1, val1, name2, val2, ...)
 */
case class CreateNamedStructUnsafe(children: Seq[Expression]) extends Expression {

  private lazy val (nameExprs, valExprs) =
    children.grouped(2).map { case Seq(name, value) => (name, value) }.toList.unzip

  private lazy val names = nameExprs.map(_.eval(EmptyRow).toString)

  override lazy val dataType: StructType = {
    val fields = names.zip(valExprs).map { case (name, valExpr) =>
      StructField(name, valExpr.dataType, valExpr.nullable, Metadata.empty)
    }
    StructType(fields)
  }

  override def foldable: Boolean = valExprs.forall(_.foldable)

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    InternalRow(valExprs.map(_.eval(input)): _*)
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval = GenerateUnsafeProjection.createCode(ctx, valExprs)
    ev.isNull = eval.isNull
    ev.primitive = eval.primitive
    eval.code
  }

  override def prettyName: String = "named_struct_unsafe"
}
