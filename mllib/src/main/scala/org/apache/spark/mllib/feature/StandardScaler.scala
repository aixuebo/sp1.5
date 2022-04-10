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

import org.apache.spark.Logging
import org.apache.spark.annotation.{DeveloperApi, Experimental, Since}
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.rdd.RDD

/**
 * :: Experimental ::
 * Standardizes features by removing the mean and scaling to unit std using column summary
 * statistics on the samples in the training set.
 *
 * @param withMean False by default. Centers the data with mean before scaling. It will build a
 *                 dense output, so this does not work on sparse input and will raise an exception.
 * @param withStd True by default. Scales the data to unit standard deviation.
  *
均值的更新逻辑是  value-均值
标准差的计算逻辑是 value * (1/标准差)
均值 & 标准差计算逻辑是 (value - 均值) * (1/标准差)
当标准差为0的时候,输出默认值为0
 */
@Since("1.1.0")
@Experimental
class StandardScaler @Since("1.1.0") (withMean: Boolean, withStd: Boolean) extends Logging {

  @Since("1.1.0")
  def this() = this(false, true)

  if (!(withMean || withStd)) {
    logWarning("Both withMean and withStd are false. The model does nothing.")
  }

  /**
   * Computes the mean and variance and stores as a model to be used for later scaling.
   *
   * @param data The data used to compute the mean and variance to build the transformation model.
   * @return a StandardScalarModel
    * 跑数据集合,去计算集合中的标准差、均值,输出模型
   */
  @Since("1.1.0")
  def fit(data: RDD[Vector]): StandardScalerModel = {
    // TODO: skip computation if both withMean and withStd are false
    val summary = data.treeAggregate(new MultivariateOnlineSummarizer)(//汇总向量的统计信息
      (aggregator, data) => aggregator.add(data),
      (aggregator1, aggregator2) => aggregator1.merge(aggregator2))
    //输出每一个特征的标准差和均值
    new StandardScalerModel(
      Vectors.dense(summary.variance.toArray.map(v => math.sqrt(v))),//方差变成标准差
      summary.mean,//均值
      withStd,
      withMean)
  }
}

/**
 * :: Experimental ::
 * Represents a StandardScaler model that can transform vectors.
 *
 * @param std column standard deviation values
 * @param mean column mean values
 * @param withStd whether to scale the data to have unit standard deviation
 * @param withMean whether to center the data before scaling
  * 经过计算,已经存在的标准差和均值的模型
 */
@Since("1.1.0")
@Experimental
class StandardScalerModel @Since("1.3.0") (
    @Since("1.3.0") val std: Vector,//标准差和均值
    @Since("1.1.0") val mean: Vector,
    @Since("1.3.0") var withStd: Boolean,//是否使用标准差和均值
    @Since("1.3.0") var withMean: Boolean) extends VectorTransformer {

  /**
    * 参数存储每一个特征的标准差和均值
   */
  @Since("1.3.0")
  def this(std: Vector, mean: Vector) {
    this(std, mean, withStd = std != null, withMean = mean != null)
    require(this.withStd || this.withMean,
      "at least one of std or mean vectors must be provided")
    if (this.withStd && this.withMean) {
      require(mean.size == std.size,
        "mean and std vectors must have equal size if both are provided")
    }
  }

  @Since("1.3.0")
  def this(std: Vector) = this(std, null)

  //mean值与withMean必须成对出现
  @Since("1.3.0")
  @DeveloperApi
  def setWithMean(withMean: Boolean): this.type = {
    require(!(withMean && this.mean == null), "cannot set withMean to true while mean is null")
    this.withMean = withMean
    this
  }

  @Since("1.3.0")
  @DeveloperApi
  def setWithStd(withStd: Boolean): this.type = {
    require(!(withStd && this.std == null),
      "cannot set withStd to true while std is null")
    this.withStd = withStd
    this
  }

  // Since `shift` will be only used in `withMean` branch, we have it as
  // `lazy val` so it will be evaluated in that branch. Note that we don't
  // want to create this array multiple times in `transform` function.
  private lazy val shift: Array[Double] = mean.toArray //向量各个维度的均值集合

  /**
   * Applies standardization transformation on a vector.
   *
   * @param vector Vector to be standardized.
   * @return Standardized vector. If the std of a column is zero, it will return default `0.0`
   *         for the column with zero std.
    * 对数据进行缩放处理
   */
  @Since("1.1.0")
  override def transform(vector: Vector): Vector = { //对参数进行缩放
    require(mean.size == vector.size)
    if (withMean) {//均值
      // By default, Scala generates Java methods for member variables. So every time when
      // the member variables are accessed, `invokespecial` will be called which is expensive.
      // This can be avoid by having a local reference of `shift`.
      val localShift = shift
      vector match {
        case DenseVector(vs) =>
          val values = vs.clone()
          val size = values.size
          if (withStd) {//标准差
            var i = 0
            while (i < size) {
              // （每一个值-均值）* (1.0 / std(i))
              values(i) = if (std(i) != 0.0) (values(i) - localShift(i)) * (1.0 / std(i)) else 0.0
              i += 1
            }
          } else {//只按均值处理
            var i = 0
            while (i < size) {
              values(i) -= localShift(i) //每一个值-均值,更新数据
              i += 1
            }
          }
          Vectors.dense(values)
        case v => throw new IllegalArgumentException("Do not support vector type " + v.getClass)
      }
    } else if (withStd) {//标准差
      vector match {
        case DenseVector(vs) =>
          val values = vs.clone()
          val size = values.size
          var i = 0
          while(i < size) {
            values(i) *= (if (std(i) != 0.0) 1.0 / std(i) else 0.0) //value * (1.0 / std(i))
            i += 1
          }
          Vectors.dense(values)
        case SparseVector(size, indices, vs) =>
          // For sparse vector, the `index` array inside sparse vector object will not be changed,
          // so we can re-use it to save memory.
          val values = vs.clone()
          val nnz = values.size
          var i = 0
          while (i < nnz) {
            values(i) *= (if (std(indices(i)) != 0.0) 1.0 / std(indices(i)) else 0.0)
            i += 1
          }
          Vectors.sparse(size, indices, values)
        case v => throw new IllegalArgumentException("Do not support vector type " + v.getClass)
      }
    } else {//保持原样,不进行缩放
      // Note that it's safe since we always assume that the data in RDD should be immutable.
      vector
    }
  }
}
