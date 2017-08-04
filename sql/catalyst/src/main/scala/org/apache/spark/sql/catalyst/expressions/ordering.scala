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
import org.apache.spark.sql.types._


/**
 * An interpreted row ordering comparator.
 * 如何对遗憾数据进行排序
 * 参数是排序的字段规则集合
 */
class InterpretedOrdering(ordering: Seq[SortOrder]) extends Ordering[InternalRow] {

  //参数inputSchema 表示order可能需要的字段集合.inputSchema的字段来自于from表内所有的字段集合
  def this(ordering: Seq[SortOrder], inputSchema: Seq[Attribute]) =
    this(ordering.map(BindReferences.bindReference(_, inputSchema)))

  //比较两行数据,每一行数据对应的列就是BoundReference对象
  def compare(a: InternalRow, b: InternalRow): Int = {
    var i = 0
    while (i < ordering.size) {
      val order = ordering(i) //循环每一个排序对象
      val left = order.child.eval(a) //获取对应的值
      val right = order.child.eval(b)

      if (left == null && right == null) {
        // Both null, continue looking.
      } else if (left == null) {
        return if (order.direction == Ascending) -1 else 1
      } else if (right == null) {
        return if (order.direction == Ascending) 1 else -1
      } else {
        val comparison = order.dataType match {
          case dt: AtomicType if order.direction == Ascending =>
            dt.ordering.asInstanceOf[Ordering[Any]].compare(left, right)
          case dt: AtomicType if order.direction == Descending =>
            dt.ordering.asInstanceOf[Ordering[Any]].reverse.compare(left, right)
          case s: StructType if order.direction == Ascending =>
            s.interpretedOrdering.asInstanceOf[Ordering[Any]].compare(left, right)
          case s: StructType if order.direction == Descending =>
            s.interpretedOrdering.asInstanceOf[Ordering[Any]].reverse.compare(left, right)
          case other =>
            throw new IllegalArgumentException(s"Type $other does not support ordered operations")
        }
        if (comparison != 0) {
          return comparison
        }
      }
      i += 1
    }
    return 0
  }
}

object InterpretedOrdering {

  /**
   * Creates a [[InterpretedOrdering]] for the given schema, in natural ascending order.
   * 将一组返回值类型对应起来
   */
  def forSchema(dataTypes: Seq[DataType]): InterpretedOrdering = {
    new InterpretedOrdering(dataTypes.zipWithIndex.map {
      case (dt, index) => new SortOrder(BoundReference(index, dt, nullable = true), Ascending)
    })
  }
}

object RowOrdering {

  /**
   * Returns true iff the data type can be ordered (i.e. can be sorted).
   * true 表示参数的数据类型是支持排序的
   */
  def isOrderable(dataType: DataType): Boolean = dataType match {
    case NullType => true
    case dt: AtomicType => true //原子类型是支持排序的
    case struct: StructType => struct.fields.forall(f => isOrderable(f.dataType)) //复合对象的所有属性都支持排序,则结果就支持排序
    case _ => false
  }

  /**
   * Returns true iff outputs from the expressions can be ordered.
   * 所有的表达式对应的返回值都是可以排序的.则返回true,说明表达式集合是可以排序的
   */
  def isOrderable(exprs: Seq[Expression]): Boolean = exprs.forall(e => isOrderable(e.dataType))
}
