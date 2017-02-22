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

package org.apache.spark.mllib.stat

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.mllib.linalg.{Vectors, Vector}

/**
 * :: DeveloperApi ::
 * MultivariateOnlineSummarizer implements [[MultivariateStatisticalSummary]] to compute the mean,
 * variance, minimum, maximum, counts, and nonzero counts for samples in sparse or dense vector
 * format in a online fashion.
 *
 * Two MultivariateOnlineSummarizer can be merged together to have a statistical summary of
 * the corresponding joint dataset.
 *
 * A numerically stable algorithm is implemented to compute sample mean and variance:
 * Reference: [[http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance variance-wiki]]
 * Zero elements (including explicit zero values) are skipped when calling add(),
 * to have time complexity O(nnz) instead of O(n) for each column.
  *
  * 专门处理特征向量
 */
@Since("1.1.0")
@DeveloperApi
class MultivariateOnlineSummarizer extends MultivariateStatisticalSummary with Serializable {

  private var n = 0 //每一个向量必须要有n个元素

  //每一个向量元素的位置进行统计,即以下数组都是n个,每一个位置都表示一个元素的统计
  private var currMean: Array[Double] = _ //该位置上每一个元素的平均值,只是计算非0元素的平均值
  private var currM2n: Array[Double] = _ //该位置上每一个非0的元素进行运算,求和,(求当前值-未添加该值前的均值)*(求当前值-添加该值后的均值)之和
  private var currM2: Array[Double] = _   //计算每一个位置上非0元素的平方和,比如一个元素是3,则此时该值就是9
  private var currL1: Array[Double] = _ //计算每一个位置上非0元素的绝对值之和
  private var nnz: Array[Double] = _ //该位置上非0元素的数量
  private var currMax: Array[Double] = _ //该位置上最大的元素
  private var currMin: Array[Double] = _ //该位置上最小的元素

  private var totalCnt: Long = 0 //一共计算了多少个向量vector
  /**
   * Add a new sample to this summarizer, and update the statistical summary.
   *
   * @param sample The sample in dense/sparse vector format to be added into this summarizer.
   * @return This MultivariateOnlineSummarizer object.
    * 添加一个新的向量进行统计
   */
  @Since("1.1.0")
  def add(sample: Vector): this.type = {
    if (n == 0) {
      require(sample.size > 0, s"Vector should have dimension larger than zero.")
      n = sample.size //向量元素数量

      currMean = Array.ofDim[Double](n)
      currM2n = Array.ofDim[Double](n)
      currM2 = Array.ofDim[Double](n)
      currL1 = Array.ofDim[Double](n)
      nnz = Array.ofDim[Double](n)
      currMax = Array.fill[Double](n)(Double.MinValue)
      currMin = Array.fill[Double](n)(Double.MaxValue)
    }

    //要求所有的向量都有相同的元素数量
    require(n == sample.size, s"Dimensions mismatch when adding new sample." +
      s" Expecting $n but got ${sample.size}.")

    val localCurrMean = currMean
    val localCurrM2n = currM2n
    val localCurrM2 = currM2
    val localCurrL1 = currL1
    val localNnz = nnz
    val localCurrMax = currMax
    val localCurrMin = currMin
    sample.foreachActive { (index, value) =>
      if (value != 0.0) {//找到非0的元素
        if (localCurrMax(index) < value) {
          localCurrMax(index) = value
        }
        if (localCurrMin(index) > value) {
          localCurrMin(index) = value
        }

        val prevMean = localCurrMean(index) //获取此时的均值
        val diff = value - prevMean //此时值与均值的diff差
        localCurrMean(index) = prevMean + diff / (localNnz(index) + 1.0) //计算平均值
        localCurrM2n(index) += (value - localCurrMean(index)) * diff
        localCurrM2(index) += value * value
        localCurrL1(index) += math.abs(value)

        localNnz(index) += 1.0
      }
    }

    totalCnt += 1 //总向量数+1
    this
  }

  /**
   * Merge another MultivariateOnlineSummarizer, and update the statistical summary.
   * (Note that it's in place merging; as a result, `this` object will be modified.)
   *
   * @param other The other MultivariateOnlineSummarizer to be merged.
   * @return This MultivariateOnlineSummarizer object.
    * 合并结果集
   */
  @Since("1.1.0")
  def merge(other: MultivariateOnlineSummarizer): this.type = {
    if (this.totalCnt != 0 && other.totalCnt != 0) {//两个元素都不是空的,进行真正的merge合并操作
      require(n == other.n, s"Dimensions mismatch when merging with another summarizer. " +
        s"Expecting $n but got ${other.n}.")
      totalCnt += other.totalCnt
      var i = 0
      while (i < n) {
        val thisNnz = nnz(i)
        val otherNnz = other.nnz(i)
        val totalNnz = thisNnz + otherNnz
        if (totalNnz != 0.0) {
          val deltaMean = other.currMean(i) - currMean(i)
          // merge mean together
          currMean(i) += deltaMean * otherNnz / totalNnz
          // merge m2n together
          currM2n(i) += other.currM2n(i) + deltaMean * deltaMean * thisNnz * otherNnz / totalNnz
          // merge m2 together
          currM2(i) += other.currM2(i)
          // merge l1 together
          currL1(i) += other.currL1(i)
          // merge max and min
          currMax(i) = math.max(currMax(i), other.currMax(i))
          currMin(i) = math.min(currMin(i), other.currMin(i))
        }
        nnz(i) = totalNnz
        i += 1
      }
    } else if (totalCnt == 0 && other.totalCnt != 0) {//返回other
      this.n = other.n
      this.currMean = other.currMean.clone()
      this.currM2n = other.currM2n.clone()
      this.currM2 = other.currM2.clone()
      this.currL1 = other.currL1.clone()
      this.totalCnt = other.totalCnt
      this.nnz = other.nnz.clone()
      this.currMax = other.currMax.clone()
      this.currMin = other.currMin.clone()
    }
    this //返回本身
  }

  /**
   * Sample mean of each dimension.
   * 计算全部元素值,包含0的,此时的均值
   */
  @Since("1.1.0")
  override def mean: Vector = {
    require(totalCnt > 0, s"Nothing has been added to this summarizer.")

    val realMean = Array.ofDim[Double](n)
    var i = 0 //计算到哪一列属性了
    while (i < n) {
      realMean(i) = currMean(i) * (nnz(i) / totalCnt)
      i += 1
    }
    Vectors.dense(realMean)
  }

  /**
   * Sample variance of each dimension.
   * 计算方差
   */
  @Since("1.1.0")
  override def variance: Vector = {
    require(totalCnt > 0, s"Nothing has been added to this summarizer.")

    val realVariance = Array.ofDim[Double](n)

    val denominator = totalCnt - 1.0

    // Sample variance is computed, if the denominator is less than 0, the variance is just 0.
    if (denominator > 0.0) {
      val deltaMean = currMean //该位置上每一个元素的平均值,只是计算非0元素的平均值
      var i = 0
      val len = currM2n.length //该位置上每一个非0的元素进行运算,求和,(求当前值-未添加该值前的均值)*(求当前值-添加该值后的均值)之和
      while (i < len) {
        realVariance(i) =
          currM2n(i) + deltaMean(i) * deltaMean(i) * nnz(i) * (totalCnt - nnz(i)) / totalCnt
        realVariance(i) /= denominator
        i += 1
      }
    }
    Vectors.dense(realVariance)
  }

  /**
   * Sample size.
    * 计算的向量样本数量
   */
  @Since("1.1.0")
  override def count: Long = totalCnt

  /**
   * Number of nonzero elements in each dimension.
    * 该向量表示每一个位置上非0元素的数量
   *
   */
  @Since("1.1.0")
  override def numNonzeros: Vector = {
    require(totalCnt > 0, s"Nothing has been added to this summarizer.")

    Vectors.dense(nnz)
  }

  /**
   * Maximum value of each dimension.
   * 返回最大值组成的向量
   */
  @Since("1.1.0")
  override def max: Vector = {
    require(totalCnt > 0, s"Nothing has been added to this summarizer.")

    //对最大值进行初始化,让其最大值至少也是0,即最大值不是负数
    var i = 0
    while (i < n) {
      if ((nnz(i) < totalCnt) && (currMax(i) < 0.0)) currMax(i) = 0.0
      i += 1
    }
    Vectors.dense(currMax)
  }

  /**
   * Minimum value of each dimension.
    * 返回最小值组成的向量
   *
   */
  @Since("1.1.0")
  override def min: Vector = {
    require(totalCnt > 0, s"Nothing has been added to this summarizer.")

    //最小的值进行初始化,最小值也是0
    var i = 0
    while (i < n) {
      if ((nnz(i) < totalCnt) && (currMin(i) > 0.0)) currMin(i) = 0.0
      i += 1
    }
    Vectors.dense(currMin)
  }

  /**
   * L2 (Euclidian) norm of each dimension.
    * 第二种度量标准
   * 每一个元素的平方和 然后开方
   */
  @Since("1.2.0")
  override def normL2: Vector = {
    require(totalCnt > 0, s"Nothing has been added to this summarizer.")

    val realMagnitude = Array.ofDim[Double](n)

    var i = 0
    val len = currM2.length //计算每一个位置上非0元素的平方和,比如一个元素是3,则此时该值就是9
    while (i < len) {
      realMagnitude(i) = math.sqrt(currM2(i)) //对数据进行开方
      i += 1
    }
    Vectors.dense(realMagnitude)
  }

  /**
   * L1 norm of each dimension.
    * 第一种标准
    * 计算每一个位置上非0元素的绝对值之和 组成的向量
   *
   */
  @Since("1.2.0")
  override def normL1: Vector = {
    require(totalCnt > 0, s"Nothing has been added to this summarizer.")

    Vectors.dense(currL1)
  }
}
