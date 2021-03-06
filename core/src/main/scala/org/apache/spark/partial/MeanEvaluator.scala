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

package org.apache.spark.partial

import org.apache.commons.math3.distribution.{NormalDistribution, TDistribution}

import org.apache.spark.util.StatCounter

/**
 * An ApproximateEvaluator for means.
 * 估算一个平均值
 * totalOutputs 表示一共有多少个统计输出
 * confidence 置信值
 */
private[spark] class MeanEvaluator(totalOutputs: Int, confidence: Double)
  extends ApproximateEvaluator[StatCounter, BoundedDouble] {

  var outputsMerged = 0 //一共合并了几个输出
  var counter = new StatCounter //合并后的最终统计值

  /**
   * 合并partition的输出结果
   * @param outputId 表示哪个partition已经完成了
   * @param taskResult 表示该partition的输出结果
   */
  override def merge(outputId: Int, taskResult: StatCounter) {
    outputsMerged += 1 //累加合并的输出
    counter.merge(taskResult) //合并统计值
  }

  override def currentResult(): BoundedDouble = {
    if (outputsMerged == totalOutputs) {//已经全部合并完成
      new BoundedDouble(counter.mean, 1.0, counter.mean, counter.mean)
    } else if (outputsMerged == 0) {//还没有输出
      new BoundedDouble(0, 0.0, Double.NegativeInfinity, Double.PositiveInfinity)
    } else {
      val mean = counter.mean
      val stdev = math.sqrt(counter.sampleVariance / counter.count)
      val confFactor = {
        if (counter.count > 100) {
          new NormalDistribution().inverseCumulativeProbability(1 - (1 - confidence) / 2)
        } else {
          val degreesOfFreedom = (counter.count - 1).toInt
          new TDistribution(degreesOfFreedom).inverseCumulativeProbability(1 - (1 - confidence) / 2)
        }
      }
      val low = mean - confFactor * stdev
      val high = mean + confFactor * stdev
      new BoundedDouble(mean, confidence, low, high)
    }
  }
}
