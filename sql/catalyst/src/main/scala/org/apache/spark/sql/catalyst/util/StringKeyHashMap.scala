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

package org.apache.spark.sql.catalyst.util

/**
 * Build a map with String type of key, and it also supports either key case
 * sensitive or insensitive.
 * key是字符串的Map,value是Any
 */
object StringKeyHashMap {
  //参数是否对大小写敏感,true表示敏感
  def apply[T](caseSensitive: Boolean): StringKeyHashMap[T] = caseSensitive match {
    case false => new StringKeyHashMap[T](_.toLowerCase)//不敏感,则将字母都转换成小写,参数是一个函数.表示对参数如何处理
    case true => new StringKeyHashMap[T](identity) //identity 表示一个函数,该函数输入什么,输出就是什么
  }
}

//持有一个函数作为参数,函数是如何将字符串进行转换
class StringKeyHashMap[T](normalizer: (String) => String) {
  private val base = new collection.mutable.HashMap[String, T]()//内部持有一个Map

  //每次key都通过normalizer函数进行转换,然后再操作map
  def apply(key: String): T = base(normalizer(key))

  def get(key: String): Option[T] = base.get(normalizer(key))

  def put(key: String, value: T): Option[T] = base.put(normalizer(key), value)

  def remove(key: String): Option[T] = base.remove(normalizer(key))

  def iterator: Iterator[(String, T)] = base.toIterator
}
