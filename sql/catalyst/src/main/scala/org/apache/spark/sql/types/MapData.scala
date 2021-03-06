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

//key和value的类型固定
abstract class MapData extends Serializable {

  def numElements(): Int

  def keyArray(): ArrayData //key的内容

  def valueArray(): ArrayData //value的内容

  def copy(): MapData

  //循环每一个元素,每一个元素的key 和 value调用f函数处理
  def foreach(keyType: DataType, valueType: DataType, f: (Any, Any) => Unit): Unit = {
    val length = numElements()
    val keys = keyArray()
    val values = valueArray()
    var i = 0
    while (i < length) {
      f(keys.get(i, keyType), values.get(i, valueType)) //分别获取每一个key和value.调用f函数
      i += 1
    }
  }
}
