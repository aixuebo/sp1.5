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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions.codegen.{GeneratedExpressionCode, CodeGenContext}
import org.apache.spark.sql.types._

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines all the expressions to extract values out of complex types.
//该文件定义了从复杂类型中去抽取值的所有的表达式
// For example, getting a field out of an array, map, or struct.
//例如从一个数组或者map或者对象中抽取一个field
////////////////////////////////////////////////////////////////////////////////////////////////////


object ExtractValue {
  /**
   * Returns the resolved `ExtractValue`. It will return one kind of concrete `ExtractValue`,
   * depend on the type of `child` and `extraction`.
   *  表示复杂数据类型  以及 抽取的条件
   *   `child`      |    `extraction`    |    concrete `ExtractValue`
   * ----------------------------------------------------------------
   *    Struct      |   Literal String   |        GetStructField 从Struct对象中抽取一个属性对应得知
   * Array[Struct]  |   Literal String   |     GetArrayStructFields  从Struct数组中抽取一个属性对应的值的集合
   *    Array       |   Integral type  表示按照数组的下标抽取,因此是int |         GetArrayItem  从数组中抽取指定index下标对应的值
   *     Map        |   map key type  表示按照字符串的key抽取   |         GetMapValue  从map中抽取指定key对应的值
   */
  def apply(
      child: Expression, //数据集合表达式
      extraction: Expression, //抽取表达式
      resolver: Resolver) //映射函数,可以映射表达式对应的Struct中的name和抽取表达式extraction中的name映射关系
     : Expression = {

    (child.dataType, extraction) match {
      case (StructType(fields), NonNullLiteral(v, StringType)) => //说明是一个StructType对象
        val fieldName = v.toString //要抽取的属性name
        val ordinal = findField(fields, fieldName, resolver) //找到该属性在StructType对象中,该属性在第几个位置上
        GetStructField(child, fields(ordinal).copy(name = fieldName), ordinal)

      case (ArrayType(StructType(fields), containsNull), NonNullLiteral(v, StringType)) => //从StructType数组中抽取一个属性的全部数据值
        val fieldName = v.toString
        val ordinal = findField(fields, fieldName, resolver)
        GetArrayStructFields(child, fields(ordinal).copy(name = fieldName),
          ordinal, fields.length, containsNull)

      case (_: ArrayType, _) => GetArrayItem(child, extraction) //从数组中抽取一个下标的值

      case (MapType(kt, _, _), _) => GetMapValue(child, extraction) //从map中抽取一个key对应的值

      case (otherType, _) =>
        val errorMsg = otherType match {
          case StructType(_) =>
            s"Field name should be String Literal, but it's $extraction"
          case other =>
            s"Can't extract value from $child"
        }
        throw new AnalysisException(errorMsg)
    }
  }

  /**
   * Find the ordinal of StructField, report error if no desired field or over one
   * desired fields are found.
   */
  private def findField(fields: Array[StructField], fieldName: String, resolver: Resolver): Int = {
    val checkField = (f: StructField) => resolver(f.name, fieldName) //true表示说明有name和数据结构的属性的映射
    val ordinal = fields.indexWhere(checkField) //获取对应的下标index
    if (ordinal == -1) {
      throw new AnalysisException(
        s"No such struct field $fieldName in ${fields.map(_.name).mkString(", ")}")
    } else if (fields.indexWhere(checkField, ordinal + 1) != -1) {
      throw new AnalysisException(
        s"Ambiguous reference to fields ${fields.filter(checkField).mkString(", ")}")
    } else {
      ordinal
    }
  }
}

/**
 * Returns the value of fields in the Struct `child`.
 *
 * No need to do type checking since it is handled by [[ExtractValue]].
 * 从InternalRow中获取一个属性对应的值,该属性是第ordinal个属性,属性对应的schema是StructField
 *
 * 参数child就是该row对象,获取该row对象的第ordinal个属性对应的值
 */
case class GetStructField(child: Expression, field: StructField, ordinal: Int)
  extends UnaryExpression {

  override def dataType: DataType = field.dataType //返回值就是属性对应的类型
  override def nullable: Boolean = child.nullable || field.nullable
  override def toString: String = s"$child.${field.name}"

  protected override def nullSafeEval(input: Any): Any =
    input.asInstanceOf[InternalRow].get(ordinal, field.dataType) //从行对象中获取第1个元素,从而得到对应的值

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, eval => {
      s"""
        if ($eval.isNullAt($ordinal)) {
          ${ev.isNull} = true;
        } else {
          ${ev.primitive} = ${ctx.getValue(eval, dataType, ordinal.toString)};
        }
      """
    })
  }
}

/**
 * Returns the array of value of fields in the Array of Struct `child`.
 *
 * No need to do type checking since it is handled by [[ExtractValue]].
 * 从strct数组中抽取某一个属性对应的值的集合
 */
case class GetArrayStructFields(
    child: Expression,//数组对象--该数组的元素都是row,
    field: StructField,//要获取的元素属性对应的值
    ordinal: Int,//该field是行里面第几个属性
    numFields: Int,//该行有多少个属性
    containsNull: Boolean) //是否允许是null
  extends UnaryExpression {

  override def dataType: DataType = ArrayType(field.dataType, containsNull)
  override def nullable: Boolean = child.nullable || containsNull || field.nullable
  override def toString: String = s"$child.${field.name}"

  protected override def nullSafeEval(input: Any): Any = {
    val array = input.asInstanceOf[ArrayData]
    val length = array.numElements() //数组的元素数量
    val result = new Array[Any](length) //获取所有的该属性对应的值
    var i = 0
    while (i < length) {
      if (array.isNullAt(i)) {//说明数组的元素是null
        result(i) = null
      } else {
        val row = array.getStruct(i, numFields) //获取第i行,该列是struct组成的行,有numFields个属性
        if (row.isNullAt(ordinal)) {
          result(i) = null
        } else {
          result(i) = row.get(ordinal, field.dataType)
        }
      }
      i += 1
    }
    new GenericArrayData(result)
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val arrayClass = classOf[GenericArrayData].getName
    nullSafeCodeGen(ctx, ev, eval => {
      s"""
        final int n = $eval.numElements();
        final Object[] values = new Object[n];
        for (int j = 0; j < n; j++) {
          if ($eval.isNullAt(j)) {
            values[j] = null;
          } else {
            final InternalRow row = $eval.getStruct(j, $numFields);
            if (row.isNullAt($ordinal)) {
              values[j] = null;
            } else {
              values[j] = ${ctx.getValue("row", field.dataType, ordinal.toString)};
            }
          }
        }
        ${ev.primitive} = new $arrayClass(values);
      """
    })
  }
}

/**
 * Returns the field at `ordinal` in the Array `child`.
 *
 * We need to do type checking here as `ordinal` expression maybe unresolved.
 * 从数组中获取一个下标对应的值
 *
 * 参数child 返回值一定是一个数组对象
 * 参数ordinal 表示要获取的数组下标,即整数类型
 */
case class GetArrayItem(child: Expression, ordinal: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  // We have done type checking for child in `ExtractValue`, so only need to check the `ordinal`.
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, IntegralType) //因为是下标,因此第二个参数是int

  override def toString: String = s"$child[$ordinal]"

  override def left: Expression = child
  override def right: Expression = ordinal

  /** `Null` is returned for invalid ordinals. */
  override def nullable: Boolean = true

  override def dataType: DataType = child.dataType.asInstanceOf[ArrayType].elementType //数据类型就是数组的元素类型

  //第一个参数是数组,第二个参数是int的下标
  protected override def nullSafeEval(value: Any, ordinal: Any): Any = {
    val baseValue = value.asInstanceOf[ArrayData] //得到数组
    val index = ordinal.asInstanceOf[Number].intValue() //得到要获取的下标
    if (index >= baseValue.numElements() || index < 0) {
      null
    } else { //直接获取下标对应的值
      baseValue.get(index, dataType)
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      s"""
        final int index = (int) $eval2;
        if (index >= $eval1.numElements() || index < 0) {
          ${ev.isNull} = true;
        } else {
          ${ev.primitive} = ${ctx.getValue(eval1, dataType, "index")};
        }
      """
    })
  }
}

/**
 * Returns the value of key `key` in Map `child`.
 * 从map中获取一个key对应的值
 * We need to do type checking here as `key` expression maybe unresolved.
 * 第一个参数child 表示返回值一定是一个map对象
 * 第二个参数key表示要获取map.get(key)中的key的name
 */
case class GetMapValue(child: Expression, key: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  //因为map的key和value所有数据都是相同的类型的
  private def keyType = child.dataType.asInstanceOf[MapType].keyType //获取key对应的数据类型

  // We have done type checking for child in `ExtractValue`, so only need to check the `key`.
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, keyType) //输入类型,是一个任意类型和key的类型

  override def toString: String = s"$child[$key]"

  override def left: Expression = child
  override def right: Expression = key

  /** `Null` is returned for invalid ordinals. */
  override def nullable: Boolean = true

  override def dataType: DataType = child.dataType.asInstanceOf[MapType].valueType //获取对应的value类型

  // todo: current search is O(n), improve it.
  //value 代表map对象,---该map对象是由两个数组组成的,即key和value两个数组
  //ordinal 表示key的name
  //该方法代表map.get(key)的实现
  protected override def nullSafeEval(value: Any, ordinal: Any): Any = {
    val map = value.asInstanceOf[MapData]
    val length = map.numElements() //map中元素数量
    val keys = map.keyArray() //map中key的集合

    var i = 0
    var found = false //是否找到了
    while (i < length && !found) { //没找到就不断找
      if (keys.get(i, keyType) == ordinal) {//获取第i个key是否是要的key
        found = true //说明发现了
      } else {
        i += 1
      }
    }

    if (!found) {
      null
    } else {
      map.valueArray().get(i, dataType) //获取第i个发现的对应的值
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val index = ctx.freshName("index")
    val length = ctx.freshName("length")
    val keys = ctx.freshName("keys")
    val found = ctx.freshName("found")
    val key = ctx.freshName("key")
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      s"""
        final int $length = $eval1.numElements();
        final ArrayData $keys = $eval1.keyArray();

        int $index = 0;
        boolean $found = false;
        while ($index < $length && !$found) {
          final ${ctx.javaType(keyType)} $key = ${ctx.getValue(keys, keyType, index)};
          if (${ctx.genEqual(keyType, key, eval2)}) {
            $found = true;
          } else {
            $index++;
          }
        }

        if ($found) {
          ${ev.primitive} = ${ctx.getValue(eval1 + ".valueArray()", dataType, index)};
        } else {
          ${ev.isNull} = true;
        }
      """
    })
  }
}
