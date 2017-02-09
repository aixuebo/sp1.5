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

import com.fasterxml.jackson.core._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion
import org.apache.spark.sql.execution.datasources.json.JacksonUtils.nextUntil
import org.apache.spark.sql.types._

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
 */
//随机抽取一组数据,然后推测数据的Schema
private[sql] object InferSchema {
  /**
   * Infer the type of a collection of json records in three stages:
   * 推断一个集合的类型的三个策略
   *   1. Infer the type of each record 推测每一个record的类型
   *   2. Merge types by choosing the lowest type necessary to cover equal keys //类型冲突的时候,选择最兼容的类型
   *   3. Replace any remaining null fields with string, the top type  //null类型最后都规范成StringType类型
   */
  def apply(
      json: RDD[String],
      samplingRatio: Double = 1.0,//抽样比例
      columnNameOfCorruptRecords: String): StructType = {
    require(samplingRatio > 0, s"samplingRatio ($samplingRatio) should be greater than 0")

    //返回抽样后的RDD结果
    val schemaData = if (samplingRatio > 0.99) {//抽样太大,因此就是全部json即可,不需要抽样
      json
    } else {
      json.sample(withReplacement = false, samplingRatio, 1)
    }

    // perform schema inference on each row and merge afterwards
    val rootType = schemaData.mapPartitions { iter => //对抽样的数据进行处理
      val factory = new JsonFactory()
      iter.map { row => //表示一行json内容
        try {
          val parser = factory.createParser(row)
          parser.nextToken()
          inferField(parser) //开始一个属性一个属性的推测
        } catch {
          case _: JsonParseException =>
            StructType(Seq(StructField(columnNameOfCorruptRecords, StringType)))
        }
      }
    }.treeAggregate[DataType](StructType(Seq()))(compatibleRootType, compatibleRootType)

    //对最终的结果进行规范化
    canonicalizeType(rootType) match {
      case Some(st: StructType) => st
      case _ =>
        // canonicalizeType erases all empty structs, including the only one we want to keep
        StructType(Seq())
    }
  }

  /**
   * Infer the type of a json document from the parser's token stream
   * 推测一个json文档中的类型
   */
  private def inferField(parser: JsonParser): DataType = {
    import com.fasterxml.jackson.core.JsonToken._
    parser.getCurrentToken match {
      case null | VALUE_NULL => NullType //是null值

      case FIELD_NAME => //说明是field的name,因此要进行继续inferField 判断类型,因为name可能对应的是map或者array或者json等
        parser.nextToken()
        inferField(parser)

      case VALUE_STRING if parser.getTextLength < 1 =>  //说明虽然是String类型,但是内容没有,因此返回也是null
        // Zero length strings and nulls have special handling to deal
        // with JSON generators that do not distinguish between the two.
        // To accurately infer types for empty strings that are really
        // meant to represent nulls we assume that the two are isomorphic
        // but will defer treating null fields as strings until all the
        // record fields' types have been combined.
        NullType

      case VALUE_STRING => StringType //说明是String类型的值
      case START_OBJECT => //说明此时是一个对象
        val builder = Seq.newBuilder[StructField]
        while (nextUntil(parser, END_OBJECT)) {//一直找到对象的结尾为止
          builder += StructField(parser.getCurrentName, inferField(parser), nullable = true) //解析json的名字 以及该名字对应的类型
        }

        StructType(builder.result().sortBy(_.name)) //对name进行排序,是因为json的name是没有任何顺序的,两条json内容对应的name可以不一致,因此有必要排序一下

      case START_ARRAY => //数组开始
        // If this JSON array is empty, we use NullType as a placeholder.
        // If this array is not empty in other JSON objects, we can resolve
        // the type as we pass through all JSON objects.
        var elementType: DataType = NullType
        while (nextUntil(parser, END_ARRAY)) { //一直找到数组的结尾为止
          elementType = compatibleType(elementType, inferField(parser)) //找到最兼容的类型
        }

        ArrayType(elementType) //数组类型

      case VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT => //说明是数字或者double类型的值
        import JsonParser.NumberType._
        parser.getNumberType match {
          // For Integer values, use LongType by default.
          case INT | LONG => LongType //长整型
          // Since we do not have a data type backed by BigInteger,
          // when we see a Java BigInteger, we use DecimalType.
          case BIG_INTEGER | BIG_DECIMAL => //小数类型
            val v = parser.getDecimalValue
            DecimalType(v.precision(), v.scale())
          case FLOAT | DOUBLE => //double类型
            // TODO(davies): Should we use decimal if possible?
            DoubleType
        }

      case VALUE_TRUE | VALUE_FALSE => BooleanType //boolean类型的值
    }
  }

  /**
   * Convert NullType to StringType and remove StructTypes with no fields
   * 规范化类型
   */
  private def canonicalizeType: DataType => Option[DataType] = {
    case at @ ArrayType(elementType, _) =>
      for {
        canonicalType <- canonicalizeType(elementType)
      } yield {
        at.copy(canonicalType)
      }

    case StructType(fields) =>
      val canonicalFields = for {
        field <- fields
        if field.name.nonEmpty
        canonicalType <- canonicalizeType(field.dataType)
      } yield {
        field.copy(dataType = canonicalType)
      }

      if (canonicalFields.nonEmpty) {
        Some(StructType(canonicalFields))
      } else {
        // per SPARK-8093: empty structs should be deleted
        None
      }

    case NullType => Some(StringType) //null类型最后都规范成StringType类型
    case other => Some(other)
  }

  /**
   * Remove top-level ArrayType wrappers and merge the remaining schemas
   */
  private def compatibleRootType: (DataType, DataType) => DataType = {
    case (ArrayType(ty1, _), ty2) => compatibleRootType(ty1, ty2)
    case (ty1, ArrayType(ty2, _)) => compatibleRootType(ty1, ty2)
    case (ty1, ty2) => compatibleType(ty1, ty2)
  }

  /**
   * Returns the most general data type for two given data types.
   * 兼容类型,找到两个类型中级别最低的类型
   */
  private[json] def compatibleType(t1: DataType, t2: DataType): DataType = {
    HiveTypeCoercion.findTightestCommonTypeOfTwo(t1, t2).getOrElse {
      // t1 or t2 is a StructType, ArrayType, or an unexpected type.
      (t1, t2) match {
        // Double support larger range than fixed decimal, DecimalType.Maximum should be enough
        // in most case, also have better precision.
        case (DoubleType, t: DecimalType) =>
          DoubleType
        case (t: DecimalType, DoubleType) =>
          DoubleType
        case (t1: DecimalType, t2: DecimalType) =>
          val scale = math.max(t1.scale, t2.scale)
          val range = math.max(t1.precision - t1.scale, t2.precision - t2.scale)
          if (range + scale > 38) {
            // DecimalType can't support precision > 38
            DoubleType
          } else {
            DecimalType(range + scale, scale)
          }

        case (StructType(fields1), StructType(fields2)) =>
          val newFields = (fields1 ++ fields2).groupBy(field => field.name).map {
            case (name, fieldTypes) =>
              val dataType = fieldTypes.view.map(_.dataType).reduce(compatibleType)
              StructField(name, dataType, nullable = true)
          }
          StructType(newFields.toSeq.sortBy(_.name))

        case (ArrayType(elementType1, containsNull1), ArrayType(elementType2, containsNull2)) =>
          ArrayType(compatibleType(elementType1, elementType2), containsNull1 || containsNull2)

        // strings and every string is a Json object.
        case (_, _) => StringType
      }
    }
  }
}
