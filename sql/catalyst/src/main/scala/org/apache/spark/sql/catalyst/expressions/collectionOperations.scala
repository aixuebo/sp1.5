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

import java.util.Comparator

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenContext, CodegenFallback, GeneratedExpressionCode}
import org.apache.spark.sql.types._

//该本表示集合相关的表达式


/**
 * Given an array or map, returns its size.
 * 返回数组或者map表达式对应的size
 */
case class Size(child: Expression) extends UnaryExpression with ExpectsInputTypes {
  override def dataType: DataType = IntegerType //返回值是整数
  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(ArrayType, MapType)) //输入参数是数组或者map

  override def nullSafeEval(value: Any): Int = child.dataType match {
    case _: ArrayType => value.asInstanceOf[ArrayData].numElements()
    case _: MapType => value.asInstanceOf[MapData].numElements()
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, c => s"${ev.primitive} = ($c).numElements();")
  }
}

/**
 * Sorts the input array in ascending / descending order according to the natural ordering of
 * the array elements and returns it.
 * 对一个数组进行排序
 */
case class SortArray(base: Expression, ascendingOrder: Expression)
  extends BinaryExpression //二元函数
  with ExpectsInputTypes //做数据类型校验
  with CodegenFallback {

  def this(e: Expression) = this(e, Literal(true))

  override def left: Expression = base //数组
  override def right: Expression = ascendingOrder //是否正序排列
  override def dataType: DataType = base.dataType //返回值类型就是数组的类型
  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType, BooleanType) //输入参数是数组集合和boolean类型的表示升序

  override def checkInputDataTypes(): TypeCheckResult = base.dataType match {
    case ArrayType(dt, _) if RowOrdering.isOrderable(dt) => //校验数组必须支持排序,支持就通过
      TypeCheckResult.TypeCheckSuccess
    case ArrayType(dt, _) => //说明数组不支持排序.因此抛异常
      TypeCheckResult.TypeCheckFailure(
        s"$prettyName does not support sorting array of type ${dt.simpleString}")
    case _ => //说明第一个参数就不是数组类型参数,因此抛异常
      TypeCheckResult.TypeCheckFailure(s"$prettyName only supports array input.")
  }

  //小于排序
  @transient
  private lazy val lt: Comparator[Any] = {
    val ordering = base.dataType match {
      case _ @ ArrayType(n: AtomicType, _) => n.ordering.asInstanceOf[Ordering[Any]] //获取原始的排序方式
    }

    //对数据进行排序
    new Comparator[Any]() {
      override def compare(o1: Any, o2: Any): Int = {
        if (o1 == null && o2 == null) {
          0
        } else if (o1 == null) {
          -1
        } else if (o2 == null) {
          1
        } else {
          ordering.compare(o1, o2)
        }
      }
    }
  }

  //大于排序
  @transient
  private lazy val gt: Comparator[Any] = {
    val ordering = base.dataType match {
      case _ @ ArrayType(n: AtomicType, _) => n.ordering.asInstanceOf[Ordering[Any]]
    }

    new Comparator[Any]() {
      override def compare(o1: Any, o2: Any): Int = {
        if (o1 == null && o2 == null) {
          0
        } else if (o1 == null) {
          1
        } else if (o2 == null) {
          -1
        } else {
          -ordering.compare(o1, o2)
        }
      }
    }
  }

  override def nullSafeEval(array: Any, ascending: Any): Any = {
    val elementType = base.dataType.asInstanceOf[ArrayType].elementType //数组元素类型
    val data = array.asInstanceOf[ArrayData].toArray[AnyRef](elementType) //创建新的数组
    java.util.Arrays.sort(data, if (ascending.asInstanceOf[Boolean]) lt else gt) //排序
    new GenericArrayData(data.asInstanceOf[Array[Any]])
  }

  override def prettyName: String = "sort_array"
}

/**
 * Checks if the array (left) has the element (right)
 * 校验左边集合是否包含右边表达式的元素
 */
case class ArrayContains(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = BooleanType //返回值boolean,表示是否包含

  override def inputTypes: Seq[AbstractDataType] = right.dataType match {
    case NullType => Seq()
    case _ => left.dataType match {
      case n @ ArrayType(element, _) => Seq(n, element)
      case _ => Seq()
    }
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    if (right.dataType == NullType) { //右边类型是否是null,不允许是null
      TypeCheckResult.TypeCheckFailure("Null typed values cannot be used as arguments")
    } else if (!left.dataType.isInstanceOf[ArrayType]
      || left.dataType.asInstanceOf[ArrayType].elementType != right.dataType) {//左边类型不是数组  或者 数据元素类型与右边不一样也会有异常
      TypeCheckResult.TypeCheckFailure(
        "Arguments must be an array followed by a value of same type as the array members")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  //是否有null
  override def nullable: Boolean = {
    left.nullable || right.nullable || left.dataType.asInstanceOf[ArrayType].containsNull
  }

  //该方法会循环数组内容,因此属于比较耗时的操作
  override def nullSafeEval(arr: Any, value: Any): Any = {
    var hasNull = false //左边数组内有null,则返回true
    arr.asInstanceOf[ArrayData].foreach(right.dataType, (i, v) => //循环数组内容
      if (v == null) {//数组元素是null,
        hasNull = true
      } else if (v == value) { //说明找到存在的value
        return true
      }
    )
    if (hasNull) {
      null
    } else {
      false
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    nullSafeCodeGen(ctx, ev, (arr, value) => {
      val i = ctx.freshName("i")
      val getValue = ctx.getValue(arr, right.dataType, i)
      s"""
      for (int $i = 0; $i < $arr.numElements(); $i ++) {
        if ($arr.isNullAt($i)) {
          ${ev.isNull} = true;
        } else if (${ctx.genEqual(right.dataType, value, getValue)}) {
          ${ev.isNull} = false;
          ${ev.primitive} = true;
          break;
        }
      }
     """
    })
  }

  override def prettyName: String = "array_contains"
}
