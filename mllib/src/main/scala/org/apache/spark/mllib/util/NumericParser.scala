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

package org.apache.spark.mllib.util

import java.util.StringTokenizer

import scala.collection.mutable.{ArrayBuilder, ListBuffer}

import org.apache.spark.SparkException

/**
 * Simple parser for a numeric structure consisting of three types:
 *
 *  - number: a double in Java's floating number format 格式就是一个double
 *  - array: an array of numbers stored as `[v0,v1,...,vn]` 格式是一个数组[double,double]
 *  - tuple: a list of numbers, arrays, or tuples stored as `(...)` 是一个集合,可以嵌套()和[]，比如(double,double,[double,double],(double,double,[double,double]))
 */
private[mllib] object NumericParser {

  /** Parses a string into a Double, an Array[Double], or a Seq[Any],or tuple类型
    * 将字符串解析成Double, an Array[Double], or a Seq[Any].
    **/
  def parse(s: String): Any = {
    val tokenizer = new StringTokenizer(s, "()[],", true)//按照()[],这几个字符拆分字符串s,如果不是double,则一定是以(或者[开头的字符串
    if (tokenizer.hasMoreTokens()) {
      val token = tokenizer.nextToken()//获取第一个拆分字符
      if (token == "(") {
        parseTuple(tokenizer)//说明字符串是()组成的元组
      } else if (token == "[") {
        parseArray(tokenizer)//说明字符串是[]组成的数组
      } else {
        // expecting a number
        parseDouble(token)
      }
    } else {
      throw new SparkException(s"Cannot find any token from the input string.")
    }
  }

  //解析字符串是[]组成的数组
  private def parseArray(tokenizer: StringTokenizer): Array[Double] = {
    val values = ArrayBuilder.make[Double] //存储数组结果
    var parsing = true//说明正在解析中
    var allowComma = false
    var token: String = null
    while (parsing && tokenizer.hasMoreTokens()) {//正在解析中,则不断循环
      token = tokenizer.nextToken()
      if (token == "]") {
        parsing = false//说明解析结束
      } else if (token == ",") {
        if (allowComma) {
          allowComma = false
        } else {
          throw new SparkException("Found a ',' at a wrong position.")
        }
      } else {
        // expecting a number
        values += parseDouble(token)//添加该double值
        allowComma = true
      }
    }
    if (parsing) {
      throw new SparkException(s"An array must end with ']'.")
    }
    values.result()
  }

  //解析字符串是()组成的元组
  private def parseTuple(tokenizer: StringTokenizer): Seq[_] = {
    val items = ListBuffer.empty[Any] //存储最终结果
    var parsing = true //说明正在解析中
    var allowComma = false
    var token: String = null
    while (parsing && tokenizer.hasMoreTokens()) {
      token = tokenizer.nextToken()
      if (token == "(") {//说明元组里面套着元组
        items.append(parseTuple(tokenizer))
        allowComma = true
      } else if (token == "[") {//说明元组中嵌套数组
        items.append(parseArray(tokenizer))
        allowComma = true
      } else if (token == ",") {
        if (allowComma) {
          allowComma = false
        } else {
          throw new SparkException("Found a ',' at a wrong position.")
        }
      } else if (token == ")") {//说明解析完成
        parsing = false
      } else if (token.trim.isEmpty){
          // ignore whitespaces between delim chars, e.g. ", ["
      } else {
        // expecting a number
        items.append(parseDouble(token))
        allowComma = true
      }
    }
    if (parsing) {
      throw new SparkException(s"A tuple must end with ')'.")
    }
    items
  }

  private def parseDouble(s: String): Double = {
    try {
      java.lang.Double.parseDouble(s)
    } catch {
      case e: NumberFormatException =>
        throw new SparkException(s"Cannot parse a double from: $s", e)
    }
  }
}
