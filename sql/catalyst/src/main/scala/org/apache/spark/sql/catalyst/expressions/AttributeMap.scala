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

/**
 * Builds a map that is keyed by an Attribute's expression id. Using the expression id allows values
 * to be looked up even when the attributes used differ cosmetically (i.e., the capitalization
 * of the name, or the expected nullability).
 */
object AttributeMap {
  def apply[A](kvs: Seq[(Attribute, A)]): AttributeMap[A] = {
    new AttributeMap(kvs.map(kv => (kv._1.exprId, kv)).toMap) //组成元组(id,(Attribute, A)),然后再转换成Map
  }
}

//一个map,表示一个属性对应一个值,即Map[Attribute, A]
//外层又套了一层,即同一个id的属性使用同一套Attribute, A
class AttributeMap[A](baseMap: Map[ExprId, (Attribute, A)])
  extends Map[Attribute, A] with Serializable {

  override def get(k: Attribute): Option[A] = baseMap.get(k.exprId).map(_._2) //获取属性对应的value,属性的通过id获取

  override def + [B1 >: A](kv: (Attribute, B1)): Map[Attribute, B1] = baseMap.values.toMap + kv //添加一个key value

  override def iterator: Iterator[(Attribute, A)] = baseMap.valuesIterator //循环所有的key-value

  override def -(key: Attribute): Map[Attribute, A] = baseMap.values.toMap - key //减去一个key
}
