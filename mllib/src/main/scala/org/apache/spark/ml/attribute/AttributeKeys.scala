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

package org.apache.spark.ml.attribute

/**
 * Keys used to store attributes.
  * 定义了若干个属性key--元数据
 */
private[attribute] object AttributeKeys {
  val ML_ATTR: String = "ml_attr" //sql中关于该字段元数据是否存在

  val TYPE: String = "type" //特征字段类型
  val NAME: String = "name" //特征字段name
  val INDEX: String = "idx" //特征字段序号
  //数值型特征的统计值
  val MIN: String = "min"
  val MAX: String = "max"
  val STD: String = "std"
  val SPARSITY: String = "sparsity"

  //分类特征的统计值
  val ORDINAL: String = "ord" //是否分类属性值有顺序
  val VALUES: String = "vals" //具体的分类值集合
  val NUM_VALUES: String = "num_vals" //分类size


  //定义向量
  val ATTRIBUTES: String = "attrs" //向量的特征集合
  val NUM_ATTRIBUTES: String = "num_attrs" //向量有多少个特征
}
