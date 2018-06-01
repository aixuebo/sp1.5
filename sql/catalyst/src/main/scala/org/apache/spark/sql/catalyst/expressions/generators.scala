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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

/**
 * An expression that produces zero or more rows given a single input row.
 * 该表达式可以允许一行数据作为参数,产生0和多个行数据
 *
 * Generators produce multiple output rows instead of a single value like other expressions,
 * and thus they must have a schema to associate with the rows that are output.
 * 该表达式产生多行,因此是与其他产生单个值的表达式不同的,因此必须要有schema表示最终的行的数据name和类型
 *
 * However, unlike row producing relational operators, which are either leaves or determine their
 * output schema functionally from their input, generators can contain other expressions that
 * might result in their modification by rules.  This structure means that they might be copied
 * multiple times after first determining their output schema. If a new output schema is created for
 * each copy references up the tree might be rendered invalid. As a result generators must
 * instead define a function `makeOutput` which is called only once when the schema is first
 * requested.  The attributes produced by this function will be automatically copied anytime rules
 * result in changes to the Generator or its children.
 */
trait Generator extends Expression {

  // TODO ideally we should return the type of ArrayType(StructType),
  // however, we don't keep the output field names in the Generator.
  override def dataType: DataType = throw new UnsupportedOperationException //该函数产生多行数据,因此没有返回值

  override def foldable: Boolean = false //这么复杂,肯定不是静态的

  override def nullable: Boolean = false

  /**
   * The output element data types in structure of Seq[(DataType, Nullable)]
   * TODO we probably need to add more information like metadata etc.
   * 如果是map,则需要两组数据类型,分别是key和value的类型,以及对应的是否为null  ;如果是数组,则显示最终行的类型就是数组的类型,boolean表示是否允许为null存在
   */
  def elementTypes: Seq[(DataType, Boolean)] //返回每一行的数据类型 以及是否允许null

  /** Should be implemented by child classes to perform specific Generators.
    * 将一行数据转换成多行数据的迭代器
    **/
  override def eval(input: InternalRow): TraversableOnce[InternalRow]

  /**
   * Notifies that there are no more rows to process, clean up code, and additional
   * rows can be made here.
   * 通知没有更多的行要去处理了,清理代码,暂时没有看到有人调用,因此不知道什么时候会用到
   */
  def terminate(): TraversableOnce[InternalRow] = Nil
}

/**
 * A generator that produces its output using the provided lambda function.
 * 通过一个用户定义的函数,将一行数据转换成多行数据的迭代器
 */
case class UserDefinedGenerator(
    elementTypes: Seq[(DataType, Boolean)],
    function: Row => TraversableOnce[InternalRow],//通过一个用户定义的函数,将一行数据转换成多行数据的迭代器-----此时的一行数据已经不是全是的行数据,而是经过children表达式转换过的一行数据
    children: Seq[Expression]) //函数需要的表达式参数
  extends Generator with CodegenFallback {

  @transient private[this] var inputRow: InterpretedProjection = _ //一行数据如何根据表达式转换成一行新的数据,列的数量可能有变化了
  @transient private[this] var convertToScala: (InternalRow) => Row = _

  //初始化
  private def initializeConverters(): Unit = {
    inputRow = new InterpretedProjection(children) //创建映射函数,将一行数据转换成children对应的返回值组成的一行数据
    //表示如何将一个对象转换成schema对应的对象,即如何将一行数据转换成Row
    convertToScala = {
      val inputSchema = StructType(children.map(e => StructField(e.simpleString, e.dataType, true)))//表达式的名字以及表达式的返回值,允许为null,这样由于表达式的数量,组成了新的数据结构
      CatalystTypeConverters.createToScalaConverter(inputSchema)
    }.asInstanceOf[InternalRow => Row]
  }

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    if (inputRow == null) {
      initializeConverters()
    }
    // Convert the objects into Scala Type before calling function, we need schema to support UDT
    //1.inputRow(input) 表示将一行数据 转换成新的一行数据,新的一行数据由children表达式结果集组成
    //2.convertToScala 表示将结果集转换成Row对象,因为已经有schema了,是可以转换的
    //3.function执行函数,将结果集转换成多行数据
    function(convertToScala(inputRow(input)))
  }

  override def toString: String = s"UserDefinedGenerator(${children.mkString(",")})"
}

/**
 * Given an input array produces a sequence of rows for each value in the array.
 * 对map或者数组形式的参数进行处理,数组元素个数多少个,就产生多少行数据,即每一个元素都产生一行数据
 *
 * 如果是数组,则新产生的每一行都是只有一列数据
 * 如果是map,则新产生的每一行都有2列,分别是key和value
 */
case class Explode(child: Expression) extends UnaryExpression with Generator with CodegenFallback {

  override def children: Seq[Expression] = child :: Nil

  override def checkInputDataTypes(): TypeCheckResult = {
    if (child.dataType.isInstanceOf[ArrayType] || child.dataType.isInstanceOf[MapType]) {//校验表达式的返回值一定是map或者数组形式的
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(//说明从参数有问题
        s"input to function explode should be array or map type, not ${child.dataType}")
    }
  }

  //如果是map,则需要两组数据类型,分别是key和value的类型,以及对应的是否为null  ;如果是数组,则显示最终行的类型就是数组的类型,boolean表示是否允许为null存在
  override def elementTypes: Seq[(DataType, Boolean)] = child.dataType match {
    case ArrayType(et, containsNull) => (et, containsNull) :: Nil //如果是数组,则显示最终行的类型就是数组的类型,boolean表示是否允许为null存在
    case MapType(kt, vt, valueContainsNull) => (kt, false) :: (vt, valueContainsNull) :: Nil //如果是map,则需要两组数据类型,分别是key和value的类型,以及对应的是否为null
  }

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    child.dataType match {
      case ArrayType(et, _) => //说明参数类型是数组,数组的元素类型是et   _表示是否允许包含null,暂时不考虑这个
        val inputArray = child.eval(input).asInstanceOf[ArrayData] //执行表达式返回数组
        if (inputArray == null) {
          Nil
        } else {
          val rows = new Array[InternalRow](inputArray.numElements()) //数组元素个数多少个,就产生多少行数据
          inputArray.foreach(et, (i, e) => {//et表示元素类型,i表示第几个元素,e表示元素值
            rows(i) = InternalRow(e)//元素值产生一行数据,赋予给row的第i行
          })
          rows
        }
      case MapType(kt, vt, _) => //key和value的类型
        val inputMap = child.eval(input).asInstanceOf[MapData] //执行表达式返回map类型
        if (inputMap == null) {
          Nil
        } else {
          val rows = new Array[InternalRow](inputMap.numElements()) //map中元素个数多少个,就产生多少行数据
          var i = 0
          inputMap.foreach(kt, vt, (k, v) => { //key和value类型,以及key和value对应的值
            rows(i) = InternalRow(k, v) //根据key-value值产生一行数据,添加到行中
            i += 1
          })
          rows
        }
    }
  }
}
