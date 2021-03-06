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

package org.apache.spark.mllib.regression

import scala.beans.BeanInfo

import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.util.NumericParser
import org.apache.spark.SparkException

/**
 * Class that represents the features and labels of a data point.
 *
 * @param label Label for this data point.
 * @param features List of features for this data point.
 */
@Since("0.8.0")
@BeanInfo
case class LabeledPoint @Since("1.0.0") (
    @Since("0.8.0") label: Double,//数据点的标签
    @Since("1.0.0") features: Vector) {//特征向量
  override def toString: String = {
    s"($label,$features)"
  }
}

/**
 * Parser for [[org.apache.spark.mllib.regression.LabeledPoint]].
 *
 */
@Since("1.1.0")
object LabeledPoint {
  /**
   * Parses a string resulted from `LabeledPoint#toString` into
   * an [[org.apache.spark.mllib.regression.LabeledPoint]].
   * 一个lable 标签以及对应很多属性值,表示一行记录
    * 格式
    * 1.lable,value1 value2 value3
   */
  @Since("1.1.0")
  def parse(s: String): LabeledPoint = {
    if (s.startsWith("(")) {
      NumericParser.parse(s) match {
        case Seq(label: Double, numeric: Any) => //any可能是一个double,也可能是一个数组或者一个Seq集合.因此该值最终存储的类型是Any
          LabeledPoint(label, Vectors.parseNumeric(numeric))
        case other =>
          throw new SparkException(s"Cannot parse $other.")
      }
    } else { // dense format used before v1.0  eg:label,value1 value2 value3
      val parts = s.split(',')
      val label = java.lang.Double.parseDouble(parts(0))
      val features = Vectors.dense(parts(1).trim().split(' ').map(java.lang.Double.parseDouble))
      LabeledPoint(label, features)
    }
  }
}
