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

package org.apache.spark.sql.catalyst

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * An abstract class for row used internal in Spark SQL, which only contain the columns as
 * internal types.
 * 该类主要用于spark sql的内部
 */
abstract class InternalRow extends SpecializedGetters with Serializable {

  def numFields: Int //这一行包含多少列

  // This is only use for test and will throw a null pointer exception if the position is null.
  //获取某一个列对应的值
  def getString(ordinal: Int): String = getUTF8String(ordinal).toString

  /**
   * Make a copy of the current [[InternalRow]] object.
   */
  def copy(): InternalRow

  /** Returns true if there are any NULL values in this row.
    * true表示只要有任何一个field的值是null,则返回true
    **/
  def anyNull: Boolean

  /* ---------------------- utility methods for Scala ---------------------- */

  /**
   * Return a Scala Seq representing the row. Elements are placed in the same order in the Seq.
   * 返回每一个属性对应的属性值集合
   * 因为每一个列对应的属性类型是不知道的.因此参数说明的就是每一列的属性类型
   */
  def toSeq(fieldTypes: Seq[DataType]): Seq[Any] = {
    val len = numFields
    assert(len == fieldTypes.length)

    val values = new Array[Any](len)
    var i = 0
    while (i < len) {
      values(i) = get(i, fieldTypes(i)) //获取每一个列的对应的类型的属性值
      i += 1
    }
    values
  }

  //每一个属性对应的DataType集合
  def toSeq(schema: StructType): Seq[Any] = toSeq(schema.map(_.dataType))
}

object InternalRow {
  /**
   * This method can be used to construct a [[InternalRow]] with the given values.
   */
  def apply(values: Any*): InternalRow = new GenericInternalRow(values.toArray)

  /**
   * This method can be used to construct a [[InternalRow]] from a [[Seq]] of values.
   */
  def fromSeq(values: Seq[Any]): InternalRow = new GenericInternalRow(values.toArray)

  /** Returns an empty [[InternalRow]]. */
  val empty = apply()
}
