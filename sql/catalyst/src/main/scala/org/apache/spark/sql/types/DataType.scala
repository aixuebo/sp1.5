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

package org.apache.spark.sql.types

import scala.util.Try
import scala.util.parsing.combinator.RegexParsers

import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.util.Utils


/**
 * :: DeveloperApi ::
 * The base type of all Spark SQL data types.
 * 表示一个数据类型
 */
@DeveloperApi
abstract class DataType extends AbstractDataType {
  /**
   * Enables matching against DataType for expressions:
   * {{{
   *   case Cast(child @ BinaryType(), StringType) =>
   *     ...
   * }}}
   * 判断该表达式是否是指定的数据类型
   */
  private[sql] def unapply(e: Expression): Boolean = e.dataType == this

  /**
   * The default size of a value of this data type, used internally for size estimation.
   * 该元素占用字节,有时候是预估字节.比如Map等
   */
  def defaultSize: Int

  /** Name of the type used in JSON serialization. 数据类型的name字符串形式*/
  def typeName: String = this.getClass.getSimpleName.stripSuffix("$").dropRight(4).toLowerCase

  private[sql] def jsonValue: JValue = typeName

  /** The compact JSON representation of this data type. 一种紧凑的json形式表示类型的name*/
  def json: String = compact(render(jsonValue))

  /** The pretty (i.e. indented) JSON representation of this data type.一种非紧凑的json形式表示类型的name */
  def prettyJson: String = pretty(render(jsonValue))

  /** Readable string representation for the type.仅仅表示数据类型的name */
  def simpleString: String = typeName

  /**
   * Check if `this` and `other` are the same data type when ignoring nullability
   * (`StructField.nullable`, `ArrayType.containsNull`, and `MapType.valueContainsNull`).
   */
  private[spark] def sameType(other: DataType): Boolean =
    DataType.equalsIgnoreNullability(this, other)

  /**
   * Returns the same data type but set all nullability fields are true
   * (`StructField.nullable`, `ArrayType.containsNull`, and `MapType.valueContainsNull`).
   */
  private[spark] def asNullable: DataType

  /**
   * Returns true if any `DataType` of this DataType tree satisfies the given function `f`.
   */
  private[spark] def existsRecursively(f: (DataType) => Boolean): Boolean = f(this)

  override private[sql] def defaultConcreteType: DataType = this

  //是否能够接收该参数类型
  override private[sql] def acceptsType(other: DataType): Boolean = sameType(other)
}

//数据类型提供的静态方法
object DataType {

  //从json字符串或者纯粹的字符串中转换成数据类型
  private[sql] def fromString(raw: String): DataType = {
    Try(DataType.fromJson(raw)).getOrElse(DataType.fromCaseClassString(raw))
  }

  //通过json字符串获取该数据类型
  def fromJson(json: String): DataType = parseDataType(parse(json))

  /**
   * @deprecated As of 1.2.0, replaced by `DataType.fromJson()`
   * 从字符串中转换成数据类型
   */
  @deprecated("Use DataType.fromJson instead", "1.2.0")
  def fromCaseClassString(string: String): DataType = CaseClassStringParser(string)

  //一个map类型,组成的支持的数据类型集合,里面不包含decimal类型
  private val nonDecimalNameToType = {
    Seq(NullType, DateType, TimestampType, BinaryType,
      IntegerType, BooleanType, LongType, DoubleType, FloatType, ShortType, ByteType, StringType)
      .map(t => t.typeName -> t).toMap
  }

  /** Given the string representation of a type, return its DataType */
  private def nameToType(name: String): DataType = {
    val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)""".r //描述decimal(int,int)数据类型的正则表达式
    name match {
      case "decimal" => DecimalType.USER_DEFAULT //说明是默认的decimal
      case FIXED_DECIMAL(precision, scale) => DecimalType(precision.toInt, scale.toInt) //说明设置了两个itn的decimal
      case other => nonDecimalNameToType(other)//说明是其他类型的数据类型
    }
  }

  private object JSortedObject {//复杂的对象类型
    def unapplySeq(value: JValue): Option[List[(String, JValue)]] = value match {
      case JObject(seq) => Some(seq.toList.sortBy(_._1))//按照首字母排序
      case _ => None
    }
  }

  // NOTE: Map fields must be sorted in alphabetical order to keep consistent with the Python side.
  //通过一个json解析成数据类型
  private def parseDataType(json: JValue): DataType = json match {
    case JString(name) =>
      nameToType(name) //说明只有数据类型名字

    case JSortedObject(
    ("containsNull", JBool(n)),//表示是否包含null,n表示具体的包含还是不包含
    ("elementType", t: JValue),//表示真实的元素对应的数据类型
    ("type", JString("array"))) => //表示该数据类型是一个数组
      ArrayType(parseDataType(t), n) //设置数据类型以及是否包含null,创建数组对象----注意此时可能是一个递归过程

    case JSortedObject(//表示一个map类型
    ("keyType", k: JValue),//key的元素类型
    ("type", JString("map")),//表示map类型
    ("valueContainsNull", JBool(n)),//是否允许为null
    ("valueType", v: JValue)) => //value的元素类型
      MapType(parseDataType(k), parseDataType(v), n)

    case JSortedObject(//表示struct类型
    ("fields", JArray(fields)),//fields是非常全的内容.而不仅仅是属性名字
    ("type", JString("struct"))) =>
      StructType(fields.map(parseStructField))//每一个fields元素进行处理

    // Scala/Java UDT
    case JSortedObject(//自定义类型
    ("class", JString(udtClass)),//对象class
    ("pyClass", _),
    ("sqlType", _),
    ("type", JString("udt"))) =>
      Utils.classForName(udtClass).newInstance().asInstanceOf[UserDefinedType[_]]

    // Python UDT
    case JSortedObject(
    ("pyClass", JString(pyClass)),
    ("serializedClass", JString(serialized)),
    ("sqlType", v: JValue),
    ("type", JString("udt"))) =>
        new PythonUserDefinedType(parseDataType(v), pyClass, serialized)
  }

  //处理每一个属性元素
  private def parseStructField(json: JValue): StructField = json match {
    case JSortedObject(
    ("metadata", metadata: JObject),//元数据信息
    ("name", JString(name)),//属性名字
    ("nullable", JBool(nullable)),//是否允许为null
    ("type", dataType: JValue)) => //属性对应的元素值的类型
      StructField(name, parseDataType(dataType), nullable, Metadata.fromJObject(metadata))
    // Support reading schema when 'metadata' is missing.
    case JSortedObject(
    ("name", JString(name)),
    ("nullable", JBool(nullable)),
    ("type", dataType: JValue)) =>
      StructField(name, parseDataType(dataType), nullable)
  }

  //通过字符串表示数据类型
  private object CaseClassStringParser extends RegexParsers {
    //原始类型正则解析器映射关系
    protected lazy val primitiveType: Parser[DataType] =
      ( "StringType" ^^^ StringType
        | "FloatType" ^^^ FloatType
        | "IntegerType" ^^^ IntegerType
        | "ByteType" ^^^ ByteType
        | "ShortType" ^^^ ShortType
        | "DoubleType" ^^^ DoubleType
        | "LongType" ^^^ LongType
        | "BinaryType" ^^^ BinaryType
        | "BooleanType" ^^^ BooleanType
        | "DateType" ^^^ DateType
        | "DecimalType()" ^^^ DecimalType.USER_DEFAULT
        | fixedDecimalType
        | "TimestampType" ^^^ TimestampType
        )

    //表示DecimalType类型的正则
    protected lazy val fixedDecimalType: Parser[DataType] =
      ("DecimalType(" ~> "[0-9]+".r) ~ ("," ~> "[0-9]+".r <~ ")") ^^ {
        case precision ~ scale => DecimalType(precision.toInt, scale.toInt)
      }

    //表示数组类型正则,比如ArrayType(数据类型,true),其中true表示的是是否允许null值
    protected lazy val arrayType: Parser[DataType] =
      "ArrayType" ~> "(" ~> dataType ~ "," ~ boolVal <~ ")" ^^ {
        case tpe ~ _ ~ containsNull => ArrayType(tpe, containsNull)
      }

    //表示map类型,格式是MapType(key类型,value类型,true),其中true表示的是是否允许null值
    protected lazy val mapType: Parser[DataType] =
      "MapType" ~> "(" ~> dataType ~ "," ~ dataType ~ "," ~ boolVal <~ ")" ^^ {
        case t1 ~ _ ~ t2 ~ _ ~ valueContainsNull => MapType(t1, t2, valueContainsNull)
      }

    //表示Struct类型中的一个属性,格式是StructField(name,数据类型,是否允许是null值)
    protected lazy val structField: Parser[StructField] =
      ("StructField(" ~> "[a-zA-Z0-9_]*".r) ~ ("," ~> dataType) ~ ("," ~> boolVal <~ ")") ^^ {
        case name ~ tpe ~ nullable =>
          StructField(name, tpe, nullable = nullable)
      }

    //表示boolean类型的属性
    protected lazy val boolVal: Parser[Boolean] =
      ( "true" ^^^ true
        | "false" ^^^ false
        )

    //表示Struct类型,表示字符串能否匹配 一组structField正则表达式
    protected lazy val structType: Parser[DataType] =
      "StructType\\([A-zA-z]*\\(".r ~> repsep(structField, ",") <~ "))" ^^ {
        case fields => StructType(fields)//说明匹配成功
      }

    //说明标准的数据类型
    protected lazy val dataType: Parser[DataType] =
      ( arrayType
        | mapType
        | structType
        | primitiveType
        )

    /**
     * Parses a string representation of a DataType.
     *
     * TODO: Generate parser as pickler...
     * 解析字符串
     */
    def apply(asString: String): DataType = parseAll(dataType, asString) match {//dataType表示定义的解析规则,asString表示具体要去解析的字符串
      case Success(result, _) => result//说明解析成功,返回解析后的类型
      case failure: NoSuccess =>
        throw new IllegalArgumentException(s"Unsupported dataType: $asString, $failure")
    }
  }

  //将数据类型 转换成 String形式,存储在builder中
  protected[types] def buildFormattedString(
    dataType: DataType,
    prefix: String,
    builder: StringBuilder): Unit = {
    dataType match {
      case array: ArrayType =>
        array.buildFormattedString(prefix, builder)
      case struct: StructType =>
        struct.buildFormattedString(prefix, builder)
      case map: MapType =>
        map.buildFormattedString(prefix, builder)
      case _ =>
    }
  }

  /**
   * Compares two types, ignoring nullability of ArrayType, MapType, StructType.
   * 忽略null属性
   * 比较两个数据类型是否相同
   * true表示两个类型相同
   */
  private[types] def equalsIgnoreNullability(left: DataType, right: DataType): Boolean = {
    (left, right) match {
      case (ArrayType(leftElementType, _), ArrayType(rightElementType, _)) => //说明是数组类型,则继续比较数组中的元素类型是否相同
        equalsIgnoreNullability(leftElementType, rightElementType)
      case (MapType(leftKeyType, leftValueType, _), MapType(rightKeyType, rightValueType, _)) => //说明是map类型,因此比较两个map的key和value类型必须都相同
        equalsIgnoreNullability(leftKeyType, rightKeyType) &&
          equalsIgnoreNullability(leftValueType, rightValueType)
      case (StructType(leftFields), StructType(rightFields)) => //是Struct类型,则需要比较属性是否都相同,即属性个数、name、类型
        leftFields.length == rightFields.length &&
          leftFields.zip(rightFields).forall { case (l, r) =>
            l.name == r.name && equalsIgnoreNullability(l.dataType, r.dataType)
          }
      case (l, r) => l == r //原始类型,简单判断是否类型相同即可
    }
  }

  /**
   * Compares two types, ignoring compatible nullability of ArrayType, MapType, StructType.
   *
   * Compatible nullability is defined as follows:
   *   - If `from` and `to` are ArrayTypes, `from` has a compatible nullability with `to`
   *   if and only if `to.containsNull` is true, or both of `from.containsNull` and
   *   `to.containsNull` are false.
   *   - If `from` and `to` are MapTypes, `from` has a compatible nullability with `to`
   *   if and only if `to.valueContainsNull` is true, or both of `from.valueContainsNull` and
   *   `to.valueContainsNull` are false.
   *   - If `from` and `to` are StructTypes, `from` has a compatible nullability with `to`
   *   if and only if for all every pair of fields, `to.nullable` is true, or both
   *   of `fromField.nullable` and `toField.nullable` are false.
   *   对比类型的兼容性,true表示兼容,false表示不兼容
   */
  private[sql] def equalsIgnoreCompatibleNullability(from: DataType, to: DataType): Boolean = {
    (from, to) match {
      case (ArrayType(fromElement, fn), ArrayType(toElement, tn)) =>
        (tn || !fn) && equalsIgnoreCompatibleNullability(fromElement, toElement)

      case (MapType(fromKey, fromValue, fn), MapType(toKey, toValue, tn)) =>
        (tn || !fn) &&
          equalsIgnoreCompatibleNullability(fromKey, toKey) &&
          equalsIgnoreCompatibleNullability(fromValue, toValue)

      case (StructType(fromFields), StructType(toFields)) =>
        fromFields.length == toFields.length &&
          fromFields.zip(toFields).forall { case (fromField, toField) =>
            fromField.name == toField.name &&
              (toField.nullable || !fromField.nullable) &&
              equalsIgnoreCompatibleNullability(fromField.dataType, toField.dataType)
          }

      case (fromDataType, toDataType) => fromDataType == toDataType
    }
  }
}
