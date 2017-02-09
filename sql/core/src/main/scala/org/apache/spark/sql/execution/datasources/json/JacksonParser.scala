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

import java.io.ByteArrayOutputStream

import com.fasterxml.jackson.core._

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.json.JacksonUtils.nextUntil
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
NOT_AVAILABLE:说明json不可用,json解析都有问题
START_OBJECT和END_OBJECT:说明此时json对应的是一个对象,即该name对应的又是一个json对象,映射到java中是StructType,里面每一个元素是StructField
START_ARRAY和END_ARRAY:说明此时json解析对应的是一个数组类型,映射到java中是ArrayType类型.里面包含数字的类型,相当于List
FIELD_NAME:说明此时json解析对应的是一个name,即此时需要进一步解析接下来的对象
VALUE_TRUE和VALUE_FALSE:说明此时json对应的value值是boolean类型的
VALUE_NULL:说明此时json对应的value值是NULL
VALUE_STRING:说明此时json对应的value值是String类型
VALUE_NUMBER_INT:说明此时json对应的value值是int或者long整数类型
VALUE_NUMBER_FLOAT:说明此时json对应的value值是float或者double整数类型
VALUE_EMBEDDED_OBJECT:暂时不太清楚含义

该类表示去解析json字符串,转换成一个行对象
 */
private[sql] object JacksonParser {
  def apply(
      json: RDD[String],
      schema: StructType,//每一行对象对应的scheme数据格式
      columnNameOfCorruptRecords: String): RDD[InternalRow] = {
    parseJson(json, schema, columnNameOfCorruptRecords)
  }

  /**
   * Parse the current token (and related children) according to a desired schema
   */
  private[sql] def convertField(
      factory: JsonFactory,
      parser: JsonParser,
      schema: DataType): Any = {
    import com.fasterxml.jackson.core.JsonToken._
    (parser.getCurrentToken, schema) match {
      case (null | VALUE_NULL, _) =>
        null

      case (FIELD_NAME, _) =>
        parser.nextToken()
        convertField(factory, parser, schema)

      case (VALUE_STRING, StringType) => //将字符串的json值转换成StringType对象
        UTF8String.fromString(parser.getText)

      case (VALUE_STRING, _) if parser.getTextLength < 1 =>
        // guard the non string type
        null

      case (VALUE_STRING, DateType) => //将字符串的json值转换成DateType对象
        DateTimeUtils.millisToDays(DateTimeUtils.stringToTime(parser.getText).getTime)

      case (VALUE_STRING, TimestampType) => //将字符串的json值转换成TimestampType对象
        DateTimeUtils.stringToTime(parser.getText).getTime * 1000L

      case (VALUE_NUMBER_INT, TimestampType) => //将Int的json值转换成TimestampType对象
        parser.getLongValue * 1000L

      case (_, StringType) => //将任意类型的的json值转换成StringType对象
        val writer = new ByteArrayOutputStream()
        val generator = factory.createGenerator(writer, JsonEncoding.UTF8)
        generator.copyCurrentStructure(parser)
        generator.close()
        UTF8String.fromBytes(writer.toByteArray)

      case (VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT, FloatType) =>
        parser.getFloatValue

      case (VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT, DoubleType) =>
        parser.getDoubleValue

      case (VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT, dt: DecimalType) =>
        Decimal(parser.getDecimalValue, dt.precision, dt.scale)

      case (VALUE_NUMBER_INT, ByteType) =>
        parser.getByteValue

      case (VALUE_NUMBER_INT, ShortType) =>
        parser.getShortValue

      case (VALUE_NUMBER_INT, IntegerType) =>
        parser.getIntValue

      case (VALUE_NUMBER_INT, LongType) =>
        parser.getLongValue

      case (VALUE_TRUE, BooleanType) =>
        true

      case (VALUE_FALSE, BooleanType) =>
        false

      case (START_OBJECT, st: StructType) => //将json的一个字符串转换成StructType对象
        convertObject(factory, parser, st)

      case (START_ARRAY, st: StructType) => //将json的一个字符串转换成数组
        // SPARK-3308: support reading top level JSON arrays and take every element
        // in such an array as a row
        convertArray(factory, parser, st)

      case (START_ARRAY, ArrayType(st, _)) => ////将json的一个字符串转换成数组
        convertArray(factory, parser, st)

      case (START_OBJECT, ArrayType(st, _)) => //解析对象
        // the business end of SPARK-3308:
        // when an object is found but an array is requested just wrap it in a list
        convertField(factory, parser, st) :: Nil

      case (START_OBJECT, MapType(StringType, kt, _)) =>
        convertMap(factory, parser, kt)

      case (_, udt: UserDefinedType[_]) => //自定义函数解析失败
        convertField(factory, parser, udt.sqlType)
    }
  }

  /**
   * Parse an object from the token stream into a new Row representing the schema.
   *
   * Fields in the json that are not defined in the requested schema will be dropped.
   * 将json的在一个字符串转换成StructType对象
   */
  private def convertObject(
      factory: JsonFactory,
      parser: JsonParser,
      schema: StructType) //表示最终的json字符串需要的结果
     : InternalRow = {
    val row = new GenericMutableRow(schema.length)
    while (nextUntil(parser, JsonToken.END_OBJECT)) {//直到对象结尾为止
      schema.getFieldIndex(parser.getCurrentName) match {//找到name对应的field indx
        case Some(index) =>
          row.update(index, convertField(factory, parser, schema(index).dataType)) //更新该值

        case None =>
          parser.skipChildren()
      }
    }

    row
  }

  /**
   * Parse an object as a Map, preserving all fields
   */
  private def convertMap(
      factory: JsonFactory,
      parser: JsonParser,
      valueType: DataType): MapData = {
    val keys = ArrayBuffer.empty[UTF8String]
    val values = ArrayBuffer.empty[Any]
    while (nextUntil(parser, JsonToken.END_OBJECT)) {
      keys += UTF8String.fromString(parser.getCurrentName)
      values += convertField(factory, parser, valueType)
    }
    ArrayBasedMapData(keys.toArray, values.toArray)
  }

  //将json的一个字符串转换成数组
  private def convertArray(
      factory: JsonFactory,
      parser: JsonParser,
      elementType: DataType): ArrayData = {
    val values = ArrayBuffer.empty[Any]
    while (nextUntil(parser, JsonToken.END_ARRAY)) {
      values += convertField(factory, parser, elementType)
    }

    new GenericArrayData(values.toArray)
  }

  //去解析json集合
  private def parseJson(
      json: RDD[String],
      schema: StructType,//json集合对应的数据结构
      columnNameOfCorruptRecords: String): RDD[InternalRow] = {//返回值是InternalRow组成的对象

    //参数record表示一行的原始内容
    def failedRecord(record: String): Seq[InternalRow] = {
      // create a row even if no corrupt record column is present
      val row = new GenericMutableRow(schema.length)
      for (corruptIndex <- schema.getFieldIndex(columnNameOfCorruptRecords)) {
        require(schema(corruptIndex).dataType == StringType) //corruptIndex序号对应的StructField必须是String类型的
        row.update(corruptIndex, UTF8String.fromString(record))
      }

      Seq(row)
    }

    //解析一个partition的数据
    json.mapPartitions { iter =>
      val factory = new JsonFactory()

      iter.flatMap { record => //一行的原始内容
        try {
          val parser = factory.createParser(record) //解析该json内容
          parser.nextToken()

          convertField(factory, parser, schema) match {//解析一行数据,返回InternalRow对象
            case null => failedRecord(record)
            case row: InternalRow => row :: Nil //转换成一行数据
            case array: ArrayData =>
              if (array.numElements() == 0) {
                Nil
              } else {
                array.toArray[InternalRow](schema) //转换成一组数据
              }
            case _ =>
              sys.error(
                s"Failed to parse record $record. Please make sure that each line of the file " +
                  "(or each string in the RDD) is a valid JSON object or an array of JSON objects.")
          }
        } catch {
          case _: JsonProcessingException =>
            failedRecord(record)
        }
      }
    }
  }
}
