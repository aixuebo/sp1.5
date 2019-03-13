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

package org.apache.spark.mllib.feature

import scala.collection.mutable.ArrayBuilder

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD

/**
 * :: Experimental ::
 * Chi Squared selector model.
 * 卡方检测特征相关度
 * @param selectedFeatures list of indices to select (filter). Must be ordered asc 参数必须是正序的,返回topN的特征序号,该序号集合是排序好的,按照序号顺序排序,而不是卡方分数排序
 */
@Since("1.3.0")
@Experimental
class ChiSqSelectorModel @Since("1.3.0") (
  @Since("1.3.0") val selectedFeatures: Array[Int]) extends VectorTransformer {

  require(isSorted(selectedFeatures), "Array has to be sorted asc") //必须参数正序排序

  //true表示参数是正叙排序号的数据
  protected def isSorted(array: Array[Int]): Boolean = {
    var i = 1
    val len = array.length
    while (i < len) {
      if (array(i) < array(i-1)) return false
      i += 1
    }
    true
  }

  /**
   * Applies transformation on a vector.
   *
   * @param vector vector to be transformed.
   * @return transformed vector.
    * 将特征vector向量进行压缩，抽取出需要的特征集合，返回新的特征
   */
  @Since("1.3.0")
  override def transform(vector: Vector): Vector = {
    compress(vector, selectedFeatures)
  }

  /**
   * Returns a vector with features filtered.
   * Preserves the order of filtered features the same as their indices are stored.
   * Might be moved to Vector as .slice
   * @param features vector
   * @param filterIndices indices of features to filter, must be ordered asc
    * 将特征vector向量进行压缩，抽取出需要的特征集合，返回新的特征
   */
  private def compress(features: Vector, filterIndices: Array[Int]): Vector = {
    features match {
      case SparseVector(size, indices, values) => //稀疏向量
        val newSize = filterIndices.length
        val newValues = new ArrayBuilder.ofDouble
        val newIndices = new ArrayBuilder.ofInt
        var i = 0
        var j = 0
        var indicesIdx = 0
        var filterIndicesIdx = 0
        while (i < indices.length && j < filterIndices.length) {
          indicesIdx = indices(i)
          filterIndicesIdx = filterIndices(j)
          if (indicesIdx == filterIndicesIdx) {
            newIndices += j
            newValues += values(i)
            j += 1
            i += 1
          } else {
            if (indicesIdx > filterIndicesIdx) {
              j += 1
            } else {
              i += 1
            }
          }
        }
        // TODO: Sparse representation might be ineffective if (newSize ~= newValues.size)
        Vectors.sparse(newSize, newIndices.result(), newValues.result())
      case DenseVector(values) => //密度向量
        val values = features.toArray
        Vectors.dense(filterIndices.map(i => values(i)))
      case other =>
        throw new UnsupportedOperationException(
          s"Only sparse and dense vectors are supported but got ${other.getClass}.")
    }
  }
}

/**
 * :: Experimental ::
 * Creates a ChiSquared feature selector.
 * @param numTopFeatures number of features that selector will select
 *                       (ordered by statistic value descending)
 */
@Since("1.3.0")
@Experimental
class ChiSqSelector @Since("1.3.0") (
  @Since("1.3.0") val numTopFeatures: Int) extends Serializable {

  /**
   * Returns a ChiSquared feature selector.
   * 返回topN的特征序号,该序号集合是排序好的,按照序号顺序排序,而不是卡方分数排序
   * @param data an `RDD[LabeledPoint]` containing the labeled dataset with categorical features.
   *             Real-valued features will be treated as categorical for each distinct value.
   *             Apply feature discretizer before using this function.
   */
  @Since("1.3.0")
  def fit(data: RDD[LabeledPoint]): ChiSqSelectorModel = {
    val indices = Statistics.chiSqTest(data) //Array[ChiSqTestResult] 每一个维度都对应一个ChiSqTestResult对象
      .zipWithIndex.sortBy { case (res, _) => -res.statistic } //按照分数倒排---因为分数越小，说明图形越在最左边，概率的相关度越大
      .take(numTopFeatures) //获取top维度
      .map { case (_, indices) => indices } //下标
      .sorted //对下标排序
    new ChiSqSelectorModel(indices)
  }
}
