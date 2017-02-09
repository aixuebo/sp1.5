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

package org.apache.spark.sql.execution.datasources.json

import org.apache.spark.sql.catalyst.InternalRow

import scala.collection.Map

import com.fasterxml.jackson.core._

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * 该类表示如何让一个row对象转换成json字符串
 */
private[sql] object JacksonGenerator {
  /** Transforms a single Row to JSON using Jackson
    * 将一个Row转换成json,使用的是jackson开源包处理
    * @param rowSchema the schema object used for conversion
    * @param gen a JsonGenerator object
    * @param row The row to convert 要转换的Row对象
    */
  def apply(rowSchema: StructType, gen: JsonGenerator)(row: Row): Unit = {
    def valWriter: (DataType, Any) => Unit = {//DataType就是StructType类型,即数据结构,Any就是Row对象
      case (_, null) | (NullType, _) => gen.writeNull()
      case (StringType, v: String) => gen.writeString(v) //说明是String类型的,因此直接写入v即可
      case (TimestampType, v: java.sql.Timestamp) => gen.writeString(v.toString)
      case (IntegerType, v: Int) => gen.writeNumber(v)
      case (ShortType, v: Short) => gen.writeNumber(v)
      case (FloatType, v: Float) => gen.writeNumber(v)
      case (DoubleType, v: Double) => gen.writeNumber(v)
      case (LongType, v: Long) => gen.writeNumber(v)
      case (DecimalType(), v: java.math.BigDecimal) => gen.writeNumber(v)
      case (ByteType, v: Byte) => gen.writeNumber(v.toInt)
      case (BinaryType, v: Array[Byte]) => gen.writeBinary(v)
      case (BooleanType, v: Boolean) => gen.writeBoolean(v)
      case (DateType, v) => gen.writeString(v.toString)
      case (udt: UserDefinedType[_], v) => valWriter(udt.sqlType, udt.serialize(v))

      case (ArrayType(ty, _), v: Seq[_]) => //说明是数组类型,因此 ArrayType(ty, _)表示ArrayType(elementType: DataType, containsNull: Boolean)    ,v表示数组的内容
        gen.writeStartArray() //写入数组开始
        v.foreach(valWriter(ty, _)) //即每一个V的元素都是ArrayType类型的,_表示V对应的值
        gen.writeEndArray() //写入数组结束

      case (MapType(kv, vv, _), v: Map[_, _]) => //是Map结构,因此value也是map对象
        gen.writeStartObject() //开始写入Map对象
        v.foreach { p => //循环每一个map对象的key-value
          gen.writeFieldName(p._1.toString)
          valWriter(vv, p._2)
        }
        gen.writeEndObject()

      case (StructType(ty), v: Row) =>
        gen.writeStartObject()
        ty.zip(v.toSeq).foreach { //tv表示每一个属性StructField对象,与v对应的属性值映射
          case (_, null) => //表示value为null,则忽略
          case (field, v) => //表示属性类型和对应的值
            gen.writeFieldName(field.name) //写入属性
            valWriter(field.dataType, v) //写入值
        }
        gen.writeEndObject()
    }

    valWriter(rowSchema, row)
  }

  /** Transforms a single InternalRow to JSON using Jackson
   * 将一个InternalRow转换成json,使用的是jackson开源包处理
   * TODO: make the code shared with the other apply method.
   *
   * @param rowSchema the schema object used for conversion
   * @param gen a JsonGenerator object
   * @param row The row to convert
   */
  def apply(rowSchema: StructType, gen: JsonGenerator, row: InternalRow): Unit = {
    def valWriter: (DataType, Any) => Unit = {
      case (_, null) | (NullType, _) => gen.writeNull()
      case (StringType, v) => gen.writeString(v.toString)
      case (TimestampType, v: java.sql.Timestamp) => gen.writeString(v.toString)
      case (IntegerType, v: Int) => gen.writeNumber(v)
      case (ShortType, v: Short) => gen.writeNumber(v)
      case (FloatType, v: Float) => gen.writeNumber(v)
      case (DoubleType, v: Double) => gen.writeNumber(v)
      case (LongType, v: Long) => gen.writeNumber(v)
      case (DecimalType(), v: Decimal) => gen.writeNumber(v.toJavaBigDecimal)
      case (ByteType, v: Byte) => gen.writeNumber(v.toInt)
      case (BinaryType, v: Array[Byte]) => gen.writeBinary(v)
      case (BooleanType, v: Boolean) => gen.writeBoolean(v)
      case (DateType, v) => gen.writeString(v.toString)
      case (udt: UserDefinedType[_], v) => valWriter(udt.sqlType, udt.serialize(v))

      case (ArrayType(ty, _), v: ArrayData) =>
        gen.writeStartArray()
        v.foreach(ty, (_, value) => valWriter(ty, value))
        gen.writeEndArray()

      case (MapType(kt, vt, _), v: MapData) =>
        gen.writeStartObject()
        v.foreach(kt, vt, { (k, v) =>
          gen.writeFieldName(k.toString)
          valWriter(vt, v)
        })
        gen.writeEndObject()

      case (StructType(ty), v: InternalRow) =>
        gen.writeStartObject()
        var i = 0
        while (i < ty.length) {
          val field = ty(i)
          val value = v.get(i, field.dataType)
          if (value != null) {
            gen.writeFieldName(field.name)
            valWriter(field.dataType, value)
          }
          i += 1
        }
        gen.writeEndObject()
    }

    valWriter(rowSchema, row)
  }
}
