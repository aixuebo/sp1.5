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

package org.apache.spark.ml.feature

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.BinaryAttribute
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructType}

/**
 * :: Experimental ::
 * Binarize a column of continuous features given a threshold.
  * 将数值特征转换为二值特征的处理过程。threshold参数表示决定二值化的阈值。 值大于阈值的特征二值化为1,否则二值化为0
 */
@Experimental
final class Binarizer(override val uid: String)
  extends Transformer with HasInputCol with HasOutputCol {

  def this() = this(Identifiable.randomUID("binarizer"))

  /**
   * Param for threshold used to binarize continuous features.
   * The features greater than the threshold, will be binarized to 1.0.
   * The features equal to or less than the threshold, will be binarized to 0.0.
   * @group param
    * 阈值
   */
  val threshold: DoubleParam =
    new DoubleParam(this, "threshold", "threshold used to binarize continuous features")

  /** @group getParam */
  def getThreshold: Double = $(threshold)

  /** @group setParam */
  def setThreshold(value: Double): this.type = set(threshold, value)

  //默认阈值为0
  setDefault(threshold -> 0.0)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true) //校验 & 构造新的输出列集合
    val td = $(threshold)
    val binarizer = udf { in: Double => if (in > td) 1.0 else 0.0 } //值与阈值比较
    val outputColName = $(outputCol)
    val metadata = BinaryAttribute.defaultAttr.withName(outputColName).toMetadata()
    dataset.select(col("*"),
      binarizer(col($(inputCol))).as(outputColName, metadata))
  }

  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), DoubleType) //输入类型一定是double类型

    val inputFields = schema.fields //所有列集合
    val outputColName = $(outputCol)

    require(inputFields.forall(_.name != outputColName), //输出列不能存在
      s"Output column $outputColName already exists.")

    val attr = BinaryAttribute.defaultAttr.withName(outputColName) //输出列是一个0/1二值列
    val outputFields = inputFields :+ attr.toStructField()
    StructType(outputFields) //构造新的输出列集合
  }

  override def copy(extra: ParamMap): Binarizer = defaultCopy(extra)
}
