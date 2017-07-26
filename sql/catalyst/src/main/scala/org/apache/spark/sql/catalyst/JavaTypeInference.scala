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

import java.beans.Introspector
import java.lang.{Iterable => JIterable}
import java.util.{Iterator => JIterator, Map => JMap}

import scala.language.existentials

import com.google.common.reflect.TypeToken
import org.apache.spark.sql.types._

/**
 * Type-inference utilities for POJOs and Java collections.
 * 根据类型接口,去推断spark支持的数据类型,即该类实现了数据类型转换功能
 */
private [sql] object JavaTypeInference {

  //反射获取java的集合对象的方法
  private val iterableType = TypeToken.of(classOf[JIterable[_]]) //java的Iterable对象
  private val mapType = TypeToken.of(classOf[JMap[_, _]]) //java的Map对象
  private val iteratorReturnType = classOf[JIterable[_]].getMethod("iterator").getGenericReturnType //返回java的Iterable对象的iterator方法,返回一个迭代器
  private val nextReturnType = classOf[JIterator[_]].getMethod("next").getGenericReturnType //返回java的Iterable对象的next方法,返回一个元素
  private val keySetReturnType = classOf[JMap[_, _]].getMethod("keySet").getGenericReturnType //返回java的Map对象的key集合
  private val valuesReturnType = classOf[JMap[_, _]].getMethod("values").getGenericReturnType  //返回java的Map对象的value集合

  /**
   * Infers the corresponding SQL data type of a JavaClean class.
   * @param beanClass Java type
   * @return (SQL data type, nullable)
   * 推断数据类型
   */
  def inferDataType(beanClass: Class[_]): (DataType, Boolean) = {
    inferDataType(TypeToken.of(beanClass))
  }

  /**
   * Infers the corresponding SQL data type of a Java type.
   * 推断出spark支持的数据类型
   * @param typeToken Java type 根据java类型的class,反射得到
   * @return (SQL data type, nullable) 返回catalyst对应的数据类型,以及是否允许为null
   */
  private def inferDataType(typeToken: TypeToken[_]): (DataType, Boolean) = {
    // TODO: All of this could probably be moved to Catalyst as it is mostly not Spark specific.
    typeToken.getRawType match {
      case c: Class[_] if c.isAnnotationPresent(classOf[SQLUserDefinedType]) => //说明是用户自定义类型
        (c.getAnnotation(classOf[SQLUserDefinedType]).udt().newInstance(), true) //返回类型对应的类实例

      case c: Class[_] if c == classOf[java.lang.String] => (StringType, true) //String可以为null
      case c: Class[_] if c == java.lang.Short.TYPE => (ShortType, false) //基本属性.是不能是null的
      case c: Class[_] if c == java.lang.Integer.TYPE => (IntegerType, false)
      case c: Class[_] if c == java.lang.Long.TYPE => (LongType, false)
      case c: Class[_] if c == java.lang.Double.TYPE => (DoubleType, false)
      case c: Class[_] if c == java.lang.Byte.TYPE => (ByteType, false)
      case c: Class[_] if c == java.lang.Float.TYPE => (FloatType, false)
      case c: Class[_] if c == java.lang.Boolean.TYPE => (BooleanType, false)

      case c: Class[_] if c == classOf[java.lang.Short] => (ShortType, true) //包装的类型,因此是可以为null的
      case c: Class[_] if c == classOf[java.lang.Integer] => (IntegerType, true)
      case c: Class[_] if c == classOf[java.lang.Long] => (LongType, true)
      case c: Class[_] if c == classOf[java.lang.Double] => (DoubleType, true)
      case c: Class[_] if c == classOf[java.lang.Byte] => (ByteType, true)
      case c: Class[_] if c == classOf[java.lang.Float] => (FloatType, true)
      case c: Class[_] if c == classOf[java.lang.Boolean] => (BooleanType, true)

      case c: Class[_] if c == classOf[java.math.BigDecimal] => (DecimalType.SYSTEM_DEFAULT, true)
      case c: Class[_] if c == classOf[java.sql.Date] => (DateType, true)
      case c: Class[_] if c == classOf[java.sql.Timestamp] => (TimestampType, true)

      case _ if typeToken.isArray => //java的数组
        val (dataType, nullable) = inferDataType(typeToken.getComponentType) //通过该方式可以返回数组对应的元素类型,以及是否是null
        (ArrayType(dataType, nullable), true) //组成新的spark对应的数组类型

      case _ if iterableType.isAssignableFrom(typeToken) => //传入的是java的迭代器,也转换成kava数组
        val (dataType, nullable) = inferDataType(elementType(typeToken))
        (ArrayType(dataType, nullable), true) //返回一个spark的数组类型

      case _ if mapType.isAssignableFrom(typeToken) => //是java的map对象
        val typeToken2 = typeToken.asInstanceOf[TypeToken[_ <: JMap[_, _]]] //强转成Map对象
        val mapSupertype = typeToken2.getSupertype(classOf[JMap[_, _]])
        val keyType = elementType(mapSupertype.resolveType(keySetReturnType))//所有key的集合
        val valueType = elementType(mapSupertype.resolveType(valuesReturnType)) //所有value的集合
        val (keyDataType, _) = inferDataType(keyType) //推测key集合类型---此时应该是返回的数组
        val (valueDataType, nullable) = inferDataType(valueType) //推测value类型集合---此时应该是返回的数组
        (MapType(keyDataType, valueDataType, nullable), true)

      case _ =>
        val beanInfo = Introspector.getBeanInfo(typeToken.getRawType)//java的bean对象,表示一个Struct的数据结构
        val properties = beanInfo.getPropertyDescriptors.filterNot(_.getName == "class")
        val fields = properties.map { property => //获取bean的所有属性
          val returnType = typeToken.method(property.getReadMethod).getReturnType //属性返回值
          val (dataType, nullable) = inferDataType(returnType) //根据属性返回值判断属性类型
          new StructField(property.getName, dataType, nullable) //组成一个spark的属性对象
        }
        (new StructType(fields), true)
    }
  }

  //参数是java的迭代器对象---返回值是集合的一个元素,通过该元素可以知道集合的元素类型
  private def elementType(typeToken: TypeToken[_]): TypeToken[_] = {
    val typeToken2 = typeToken.asInstanceOf[TypeToken[_ <: JIterable[_]]]  //转换成java的迭代器对象
    val iterableSupertype = typeToken2.getSupertype(classOf[JIterable[_]])
    val iteratorType = iterableSupertype.resolveType(iteratorReturnType) //调用iterator方法,返回迭代器
    val itemType = iteratorType.resolveType(nextReturnType) //调用next方法,返回集合的一个元素
    itemType
  }
}
