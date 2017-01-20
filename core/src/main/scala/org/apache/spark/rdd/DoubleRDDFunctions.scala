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

package org.apache.spark.rdd

import org.apache.spark.annotation.Experimental
import org.apache.spark.{TaskContext, Logging}
import org.apache.spark.partial.BoundedDouble
import org.apache.spark.partial.MeanEvaluator
import org.apache.spark.partial.PartialResult
import org.apache.spark.partial.SumEvaluator
import org.apache.spark.util.StatCounter

/**
 * Extra functions available on RDDs of Doubles through an implicit conversion.
 * 通过一个隐式转换,为Double类型的RDD提供一些额外的有用的功能
 * 
 * 参数是Double类型的RDD
 */
class DoubleRDDFunctions(self: RDD[Double]) extends Logging with Serializable {
  /** Add up the elements in this RDD. 求和*/
  def sum(): Double = self.withScope {
    self.fold(0.0)(_ + _)
  }

  /**
   * Return a [[org.apache.spark.util.StatCounter]] object that captures the mean, variance and
   * count of the RDD's elements in one operation.
   * 获取统计对象
   */
  def stats(): StatCounter = self.withScope {
    self.mapPartitions(nums => Iterator(StatCounter(nums))).reduce((a, b) => a.merge(b)) //首先对每一个partiton迭代,转换成统计对象,然后合并每一个partition的结果
  }

  /** Compute the mean of this RDD's elements.
    * 平均自
    **/
  def mean(): Double = self.withScope {
    stats().mean
  }

  /** Compute the variance of this RDD's elements.
    * 方差
    **/
  def variance(): Double = self.withScope {
    stats().variance
  }

  /** Compute the standard deviation of this RDD's elements.
    * 方差的开方,标准差
    **/
  def stdev(): Double = self.withScope {
    stats().stdev
  }

  /**
   * Compute the sample variance of this RDD's elements (which corrects for bias in
   * estimating the variance by dividing by N-1 instead of N).
   * 样本方差
   */
  def sampleVariance(): Double = self.withScope {
    stats().sampleVariance
  }

  /**
   * Compute the sample standard deviation of this RDD's elements (which corrects for bias in
   * estimating the standard deviation by dividing by N-1 instead of N).
   * 样本方差的开方,样本标准差
   */
  def sampleStdev(): Double = self.withScope {
    stats().sampleStdev
  }

  /**
   * :: Experimental ::
   * Approximate operation to return the mean within a timeout.
   */
  @Experimental
  def meanApprox(
      timeout: Long,
      confidence: Double = 0.95): PartialResult[BoundedDouble] = self.withScope {
    val processPartition = (ctx: TaskContext, ns: Iterator[Double]) => StatCounter(ns)
    val evaluator = new MeanEvaluator(self.partitions.length, confidence)
    self.context.runApproximateJob(self, processPartition, evaluator, timeout)
  }

  /**
   * :: Experimental ::
   * Approximate operation to return the sum within a timeout.
   */
  @Experimental
  def sumApprox(
      timeout: Long,
      confidence: Double = 0.95): PartialResult[BoundedDouble] = self.withScope {
    val processPartition = (ctx: TaskContext, ns: Iterator[Double]) => StatCounter(ns)
    val evaluator = new SumEvaluator(self.partitions.length, confidence)
    self.context.runApproximateJob(self, processPartition, evaluator, timeout)
  }

  /**
   * Compute a histogram of the data using bucketCount number of buckets evenly
   *  spaced between the minimum and maximum of the RDD. For example if the min
   *  value is 0 and the max is 100 and there are two buckets the resulting
   *  buckets will be [0, 50) [50, 100]. bucketCount must be at least 1
   * If the RDD contains infinity, NaN throws an exception
   * If the elements in RDD do not vary (max == min) always returns a single bucket.
   * 
   * bucketCount 表示将最大值与最小值之间拆分成多少分,该值最少也要是1
   * 获取直方图数据
   *
   * 返回值第一个Array[Double] 表示分组后每一个指标的区间
   *
   */
  def histogram(bucketCount: Int): Pair[Array[Double], Array[Long]] = self.withScope {

    // Scala's built-in range has issues. See #SI-8782
    /**
     *
     * @param min 最小值
     * @param max 最大值
     * @param steps 最终多少个分组
     * @return 返回每个分组的区间
     */
    def customRange(min: Double, max: Double, steps: Int): IndexedSeq[Double] = {
      val span = max - min //最大值和最小值的差,就是总区间
      /**
Range.Int(0, 5, 1) 输出NumericRange(0, 1, 2, 3, 4)
Range.Int(0, 5, 2) 输出 NumericRange(0, 2, 4)
循环分组个数,每一个分组,最终返回的是分组后的区间结果集
       */
      Range.Int(0, steps, 1).map(s => min + (s * span) / steps) :+ max
    }

    // Compute the minimum and the maximum 先计算每一个partition的最大值和最小值,最终合并成全局的最大值和最小值
    val (max: Double, min: Double) = self.mapPartitions { items =>
      Iterator(items.foldRight(Double.NegativeInfinity,
        Double.PositiveInfinity)((e: Double, x: Pair[Double, Double]) =>
        (x._1.max(e), x._2.min(e)))) //每一个元素拿出来,对比出最大值和最小值
    }.reduce { (maxmin1, maxmin2) =>
      (maxmin1._1.max(maxmin2._1), maxmin1._2.min(maxmin2._2))
    }//合并所有的partition,成全局的最大值和最小值

    if (min.isNaN || max.isNaN || max.isInfinity || min.isInfinity ) {
      throw new UnsupportedOperationException(
        "Histogram on either an empty RDD or RDD containing +/-infinity or NaN")
    }

    val range = if (min != max) {//说明最大值和最小值不相同
      // Range.Double.inclusive(min, max, increment)
      // The above code doesn't always work. See Scala bug #SI-8782.
      // https://issues.scala-lang.org/browse/SI-8782
      customRange(min, max, bucketCount)
    } else {//说明最大值和最小值相同
      List(min, min)
    }
    val buckets = range.toArray
    (buckets, histogram(buckets, true))
  }

  /**
   * Compute a histogram using the provided buckets. The buckets are all open
   * to the right except for the last which is closed
   *  e.g. for the array
   *  [1, 10, 20, 50] the buckets are [1, 10) [10, 20) [20, 50]
   *  e.g 1<=x<10 , 10<=x<20, 20<=x<=50
   *  And on the input of 1 and 50 we would have a histogram of 1, 0, 1
   *
   * Note: if your histogram is evenly spaced (e.g. [0, 10, 20, 30]) this can be switched
   * from an O(log n) inseration to O(1) per element. (where n = # buckets) if you set evenBuckets
   * to true.
   * buckets must be sorted and not contain any duplicates.
   * buckets array must be at least two elements
   * All NaN entries are treated the same. If you have a NaN bucket it must be
   * the maximum value of the last position and all NaN entries will be counted
   * in that bucket.
   *
   * 参数buckets 表示直方图中分组的数字
   */
  def histogram(
      buckets: Array[Double],
      evenBuckets: Boolean = false): Array[Long] = self.withScope {
    if (buckets.length < 2) {
      throw new IllegalArgumentException("buckets array must have at least two elements")
    }
    // The histogramPartition function computes the partail histogram for a given
    // partition. The provided bucketFunction determines which bucket in the array
    // to increment or returns None if there is no bucket. This is done so we can
    // specialize for uniformly distributed buckets and save the O(log n) binary
    // search cost.
    //iter表示一个partition的迭代器
    //函数bucketFunction 表示一个double属于哪个标签内
    //返回每一个partition的每一个标签分组内有多少个数据
    def histogramPartition(bucketFunction: (Double) => Option[Int])(iter: Iterator[Double]):
        Iterator[Array[Long]] = {
      val counters = new Array[Long](buckets.length - 1) //柱状图的区间
      while (iter.hasNext) {
        bucketFunction(iter.next()) match {
          case Some(x: Int) => {counters(x) += 1}//该标签分组内数据量+1
          case _ => {}
        }
      }
      Iterator(counters) //返回每一个标签分组内有多少个数据
    }
    // Merge the counters.合并所有partition中每一个标签分组内的数据量
    def mergeCounters(a1: Array[Long], a2: Array[Long]): Array[Long] = {
      a1.indices.foreach(i => a1(i) += a2(i))
      a1
    }
    // Basic bucket function. This works using Java's built in Array
    // binary search. Takes log(size(buckets))
    //查看参数double属于哪个区间
    def basicBucketFunction(e: Double): Option[Int] = {
      val location = java.util.Arrays.binarySearch(buckets, e) //标签范围
      if (location < 0) {//没有在该标签内
        // If the location is less than 0 then the insertion point in the array
        // to keep it sorted is -location-1
        val insertionPoint = -location-1
        // If we have to insert before the first element or after the last one
        // its out of bounds.
        // We do this rather than buckets.lengthCompare(insertionPoint)
        // because Array[Double] fails to override it (for now).
        if (insertionPoint > 0 && insertionPoint < buckets.length) {
          Some(insertionPoint-1)
        } else {
          None
        }
      } else if (location < buckets.length - 1) {//返回具体的位置
        // Exact match, just insert here
        Some(location)
      } else {
        // Exact match to the last element
        Some(location - 1)
      }
    }

    // Determine the bucket function in constant time. Requires that buckets are evenly spaced
    //min和max表示柱状图的最小值和最大值的分区,count表示分区数量,一共多少个分组,e表示一个value值,输出是该value值属于第几个分区内
    def fastBucketFunction(min: Double, max: Double, count: Int)(e: Double): Option[Int] = {
      // If our input is not a number unless the increment is also NaN then we fail fast
      if (e.isNaN || e < min || e > max) {
        None
      } else {
        // Compute ratio of e's distance along range to total range first, for better precision
        val bucketNumber = (((e - min) / (max - min)) * count).toInt
        // should be less than count, but will equal count if e == max, in which case
        // it's part of the last end-range-inclusive bucket, so return count-1
        Some(math.min(bucketNumber, count - 1))
      }
    }
    // Decide which bucket function to pass to histogramPartition. We decide here
    // rather than having a general function so that the decision need only be made
    // once rather than once per shard
    val bucketFunction = if (evenBuckets) {
      fastBucketFunction(buckets.head, buckets.last, buckets.length - 1) _
    } else {
      basicBucketFunction _
    }

    //去计算每一个分区有多少条数据
    if (self.partitions.length == 0) {
      new Array[Long](buckets.length - 1)
    } else {
      // reduce() requires a non-empty RDD. This works because the mapPartitions will make
      // non-empty partitions out of empty ones. But it doesn't handle the no-partitions case,
      // which is below
      self.mapPartitions(histogramPartition(bucketFunction)).reduce(mergeCounters)
    }
  }

}
