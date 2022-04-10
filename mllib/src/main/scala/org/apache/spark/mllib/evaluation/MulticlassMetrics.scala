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

package org.apache.spark.mllib.evaluation

import scala.collection.Map

import org.apache.spark.SparkContext._
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.mllib.linalg.{Matrices, Matrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
 * ::Experimental::
 * Evaluator for multiclass classification.
 * 多分类评估,混淆矩阵,准确率、召回率评估
 * @param predictionAndLabels an RDD of (prediction, label) pairs.
 */
@Since("1.1.0")
@Experimental
class MulticlassMetrics @Since("1.1.0") (predictionAndLabels: RDD[(Double, Double)]) {//RDD:预测值和真实值

  /**
   * An auxiliary constructor taking a DataFrame.
   * @param predictionAndLabels a DataFrame with two double columns: prediction and label
   */
  private[mllib] def this(predictionAndLabels: DataFrame) = //参数是预测值 和 真实值 组成的RDD
    this(predictionAndLabels.map(r => (r.getDouble(0), r.getDouble(1))))

  //key是真实的label,value是该label出现次数,即样本中有该label的数据条数
  private lazy val labelCountByClass: Map[Double, Long] = predictionAndLabels.values.countByValue() //计算真实值出现的次数
  private lazy val labelCount: Long = labelCountByClass.values.sum //样本总数
  //key是真实label,value是预测正确的个数
  private lazy val tpByClass: Map[Double, Int] = predictionAndLabels //预测值-真实值
    .map { case (prediction, label) =>
      (label, if (label == prediction) 1 else 0) //转换为 真实值-预测是否正确
    }.reduceByKey(_ + _)
    .collectAsMap() //预测label正确的个数

  //key是真实label,value是预测错误的个数
  private lazy val fpByClass: Map[Double, Int] = predictionAndLabels
    .map { case (prediction, label) =>
      (prediction, if (prediction != label) 1 else 0) //预测不正确的个数
    }.reduceByKey(_ + _)
    .collectAsMap()

  //key是(真实值,预测值),value是产生这种结果的个数
  private lazy val confusions = predictionAndLabels
    .map { case (prediction, label) =>
      ((label, prediction), 1)
    }.reduceByKey(_ + _)
    .collectAsMap()

  /**
   * Returns confusion matrix:
   * predicted classes are in columns,
   * they are ordered by class label ascending,
   * as in "labels"
    * 混淆矩阵---每种case出现的次数,用于查找谁和谁总混淆
   */
  @Since("1.1.0")
  def confusionMatrix: Matrix = {
    val n = labels.size //label集合
    val values = Array.ofDim[Double](n * n)
    var i = 0
    while (i < n) {
      var j = 0
      while (j < n) {
        values(i + j * n) = confusions.getOrElse((labels(i), labels(j)), 0).toDouble
        j += 1
      }
      i += 1
    }
    Matrices.dense(n, n, values)
  }

  /**
   * Returns true positive rate for a given label (category)
   * @param label the label.
    * 该label的召回率
   */
  @Since("1.1.0")
  def truePositiveRate(label: Double): Double = recall(label)

  /**
   * Returns false positive rate for a given label (category)
   * @param label the label.
   */
  @Since("1.1.0")
  def falsePositiveRate(label: Double): Double = {
    val fp = fpByClass.getOrElse(label, 0)
    fp.toDouble / (labelCount - labelCountByClass(label))
  }

  /**
   * Returns precision for a given label (category)
   * @param label the label.
    * 某一个label的准确率:正确的/(正确的+错误的),即 正确的/label的全部数据
   */
  @Since("1.1.0")
  def precision(label: Double): Double = {
    val tp = tpByClass(label)
    val fp = fpByClass.getOrElse(label, 0)
    if (tp + fp == 0) 0 else tp.toDouble / (tp + fp)
  }

  /**
   * Returns recall for a given label (category)
   * @param label the label.
    * 该label的召回率:该lable预测正确的数量 / 该label总的正确的数量
   */
  @Since("1.1.0")
  def recall(label: Double): Double = tpByClass(label).toDouble / labelCountByClass(label)

  /**
   * Returns f-measure for a given label (category)
   * @param label the label.
   * @param beta the beta parameter.
   */
  @Since("1.1.0")
  def fMeasure(label: Double, beta: Double): Double = {
    val p = precision(label)
    val r = recall(label)
    val betaSqrd = beta * beta
    if (p + r == 0) 0 else (1 + betaSqrd) * p * r / (betaSqrd * p + r)
  }

  /**
   * Returns f1-measure for a given label (category)
   * @param label the label.
   */
  @Since("1.1.0")
  def fMeasure(label: Double): Double = fMeasure(label, 1.0)

  /**
   * Returns precision
   */
  @Since("1.1.0")
  lazy val precision: Double = tpByClass.values.sum.toDouble / labelCount

  /**
   * Returns recall
   * (equals to precision for multiclass classifier
   * because sum of all false positives is equal to sum
   * of all false negatives)
   */
  @Since("1.1.0")
  lazy val recall: Double = precision

  /**
   * Returns f-measure
   * (equals to precision and recall because precision equals recall)
   */
  @Since("1.1.0")
  lazy val fMeasure: Double = precision

  /**
   * Returns weighted true positive rate
   * (equals to precision, recall and f-measure)
   */
  @Since("1.1.0")
  lazy val weightedTruePositiveRate: Double = weightedRecall

  /**
   * Returns weighted false positive rate
   */
  @Since("1.1.0")
  lazy val weightedFalsePositiveRate: Double = labelCountByClass.map { case (category, count) =>
    falsePositiveRate(category) * count.toDouble / labelCount
  }.sum

  /**
   * Returns weighted averaged recall
   * (equals to precision, recall and f-measure)
   */
  @Since("1.1.0")
  lazy val weightedRecall: Double = labelCountByClass.map { case (category, count) =>
    recall(category) * count.toDouble / labelCount
  }.sum

  /**
   * Returns weighted averaged precision
   */
  @Since("1.1.0")
  lazy val weightedPrecision: Double = labelCountByClass.map { case (category, count) =>
    precision(category) * count.toDouble / labelCount
  }.sum

  /**
   * Returns weighted averaged f-measure
   * @param beta the beta parameter.
   */
  @Since("1.1.0")
  def weightedFMeasure(beta: Double): Double = labelCountByClass.map { case (category, count) =>
    fMeasure(category, beta) * count.toDouble / labelCount
  }.sum

  /**
   * Returns weighted averaged f1-measure
   */
  @Since("1.1.0")
  lazy val weightedFMeasure: Double = labelCountByClass.map { case (category, count) =>
    fMeasure(category, 1.0) * count.toDouble / labelCount
  }.sum

  /**
   * Returns the sequence of labels in ascending order
    * 返回label的集合
   */
  @Since("1.1.0")
  lazy val labels: Array[Double] = tpByClass.keys.toArray.sorted
}
