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

package org.apache.spark.ml.util

import scala.collection.immutable.HashMap

import org.apache.spark.ml.attribute._
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.sql.types.StructField


/**
 * Helper utilities for tree-based algorithms
 */
private[spark] object MetadataUtils {

  /**
   * Examine a schema to identify the number of classes in a label column.
   * Returns None if the number of labels is not specified, or if the label column is continuous.
    * 该特征的分类数量
   */
  def getNumClasses(labelSchema: StructField): Option[Int] = {
    Attribute.fromStructField(labelSchema) match {
      case binAttr: BinaryAttribute => Some(2) //二元就2个
      case nomAttr: NominalAttribute => nomAttr.getNumValues
      case _: NumericAttribute | UnresolvedAttribute => None
    }
  }

  /**
   * Examine a schema to identify categorical (Binary and Nominal) features.
   *
   * @param featuresSchema  Schema of the features column.
   *                        If a feature does not have metadata, it is assumed to be continuous.
   *                        If a feature is Nominal, then it must have the number of values
   *                        specified.
   * @return  Map: feature index --> number of categories.
   *          The map's set of keys will be the set of categorical feature indices.
    * 返回向量中每一个特征对应的分类数量的映射,key是特征index序号
   */
  def getCategoricalFeatures(featuresSchema: StructField): Map[Int, Int] = {
    val metadata = AttributeGroup.fromStructField(featuresSchema)
    if (metadata.attributes.isEmpty) {
      HashMap.empty[Int, Int]
    } else {
      metadata.attributes.get.zipWithIndex.flatMap { case (attr, idx) =>
        if (attr == null) {
          Iterator()
        } else {
          attr match {
            case _: NumericAttribute | UnresolvedAttribute => Iterator()
            case binAttr: BinaryAttribute => Iterator(idx -> 2)
            case nomAttr: NominalAttribute =>
              nomAttr.getNumValues match {
                case Some(numValues: Int) => Iterator(idx -> numValues)
                case None => throw new IllegalArgumentException(s"Feature $idx is marked as" +
                  " Nominal (categorical), but it does not have the number of values specified.")
              }
          }
        }
      }.toMap
    }
  }

  /**
   * Takes a Vector column and a list of feature names, and returns the corresponding list of
   * feature indices in the column, in order.
   * @param col  Vector column which must have feature names specified via attributes 该列必须是vector的向量类型列
   * @param names  List of feature names 获取向量中某些name对应的序号
    *
   */
  def getFeatureIndicesFromNames(col: StructField, names: Array[String]): Array[Int] = {
    require(col.dataType.isInstanceOf[VectorUDT], s"getFeatureIndicesFromNames expected column $col"
      + s" to be Vector type, but it was type ${col.dataType} instead.")
    val inputAttr = AttributeGroup.fromStructField(col) //转换成向量特征集合
    //返回name对应的向量下标
    names.map { name =>
      require(inputAttr.hasAttr(name),
        s"getFeatureIndicesFromNames found no feature with name $name in column $col.")
      inputAttr.getAttr(name).index.get
    }
  }
}
