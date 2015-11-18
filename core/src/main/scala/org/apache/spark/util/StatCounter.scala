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

package org.apache.spark.util

/**
 * A class for tracking the statistics of a set of numbers (count, mean and variance) in a
 * numerically robust way. Includes support for merging two StatCounters. Based on Welford
 * and Chan's [[http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance algorithms]]
 * for running variance.
 *
 * @constructor Initialize the StatCounter with the given values.
 * 提供可遍历的double对象作为参数,来构造统计对象StatCounter
 */
class StatCounter(values: TraversableOnce[Double]) extends Serializable {
  private var n: Long = 0     // Running count of our values,value的个数
  private var mu: Double = 0  // Running mean of our values,平均数
  private var m2: Double = 0  // Running variance numerator (sum of (x - mean)^2) 变异 的平均数
  private var maxValue: Double = Double.NegativeInfinity // Running max of our values 最大值
  private var minValue: Double = Double.PositiveInfinity // Running min of our values 最小值

  merge(values)

  /** Initialize the StatCounter with no values. */
  def this() = this(Nil)

  /** Add a value into this StatCounter, updating the internal statistics. 
   * 添加一个值,进行统计,更新内部的统计信息  
   **/
  def merge(value: Double): StatCounter = {
    val delta = value - mu
    n += 1 //个数+1
    mu += delta / n
    m2 += delta * (value - mu)
    maxValue = math.max(maxValue, value)
    minValue = math.min(minValue, value)
    this
  }

  /** Add multiple values into this StatCounter, updating the internal statistics. 
   * 遍历数据,更新统计信息  
   **/
  def merge(values: TraversableOnce[Double]): StatCounter = {
    values.foreach(v => merge(v))
    this
  }

  /** Merge another StatCounter into this one, adding up the internal statistics. */
  def merge(other: StatCounter): StatCounter = {
    if (other == this) {
      merge(other.copy())  // Avoid overwriting fields in a weird order
    } else {
      if (n == 0) {
        mu = other.mu
        m2 = other.m2
        n = other.n
        maxValue = other.maxValue
        minValue = other.minValue
      } else if (other.n != 0) {
        val delta = other.mu - mu
        if (other.n * 10 < n) {
          mu = mu + (delta * other.n) / (n + other.n)
        } else if (n * 10 < other.n) {
          mu = other.mu - (delta * n) / (n + other.n)
        } else {
          mu = (mu * n + other.mu * other.n) / (n + other.n)
        }
        m2 += other.m2 + (delta * delta * n * other.n) / (n + other.n)
        n += other.n
        maxValue = math.max(maxValue, other.maxValue)
        minValue = math.min(minValue, other.minValue)
      }
      this
    }
  }

  /** Clone this StatCounter */
  def copy(): StatCounter = {
    val other = new StatCounter
    other.n = n
    other.mu = mu
    other.m2 = m2
    other.maxValue = maxValue
    other.minValue = minValue
    other
  }

  def count: Long = n

  def mean: Double = mu

  def sum: Double = n * mu

  def max: Double = maxValue

  def min: Double = minValue

  /** Return the variance of the values. */
  def variance: Double = {
    if (n == 0) {
      Double.NaN
    } else {
      m2 / n
    }
  }

  /**
   * Return the sample variance, which corrects for bias in estimating the variance by dividing
   * by N-1 instead of N.
   */
  def sampleVariance: Double = {
    if (n <= 1) {
      Double.NaN
    } else {
      m2 / (n - 1)
    }
  }

  /** Return the standard deviation of the values. */
  def stdev: Double = math.sqrt(variance)

  /**
   * Return the sample standard deviation of the values, which corrects for bias in estimating the
   * variance by dividing by N-1 instead of N.
   */
  def sampleStdev: Double = math.sqrt(sampleVariance)

  override def toString: String = {
    "(count: %d, mean: %f, stdev: %f, max: %f, min: %f)".format(count, mean, stdev, max, min)
  }
}

//提供可遍历的double对象或者double数组,来构造统计对象StatCounter
object StatCounter {
  /** Build a StatCounter from a list of values. */
  def apply(values: TraversableOnce[Double]): StatCounter = new StatCounter(values)

  /** Build a StatCounter from a list of values passed as variable-length arguments. */
  def apply(values: Double*): StatCounter = new StatCounter(values)
}
