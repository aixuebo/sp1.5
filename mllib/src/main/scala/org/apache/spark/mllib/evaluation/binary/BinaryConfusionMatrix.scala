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

package org.apache.spark.mllib.evaluation.binary

/**
 * Trait for a binary confusion matrix.
  * confusion matrix 混淆矩阵也称误差矩阵
  * 用于二分类
  *
  * - True Positive(真正, TP)：将正类预测为正类数.
  * - True Negative(真负 , TN)：将负类预测为负类数.
  * - False Positive(假正, FP)：将负类预测为正类数 →→ 误报 (Type I error).
  * - False Negative(假负 , FN)：将正类预测为负类数 →→ 漏报 (Type II error).
  *
  * 横坐标是真实的数据,纵坐标表示预测值
  * 预测     Positive	   Negative  真实的
  * True	TP	           FP
  * False	FN	           TN
 */
private[evaluation] trait BinaryConfusionMatrix {
  //true表示真实是对的，即真正、真负
  /** number of true positives 真正, TP   将正类预测为正类数*/
  def numTruePositives: Long

  /** number of false positives 假正, FP  将负类预测为正类数 */
  def numFalsePositives: Long

  /** number of false negatives 假负 , FN  将正类预测为负类数*/
  def numFalseNegatives: Long

  /** number of true negatives 真负 , TN  将负类预测为负类数.*/
  def numTrueNegatives: Long

  /** number of positives 真实的样本中,正样本数量 */
  def numPositives: Long = numTruePositives + numFalseNegatives

  /** number of negatives 真实的样本中,负样本数量*/
  def numNegatives: Long = numFalsePositives + numTrueNegatives
}

/**
 * Implementation of [[org.apache.spark.mllib.evaluation.binary.BinaryConfusionMatrix]].
 *
 * @param count label counter for labels with scores greater than or equal to the current score
 * @param totalCount label counter for all labels
 */
private[evaluation] case class BinaryConfusionMatrixImpl(
    count: BinaryLabelCounter,
    totalCount: BinaryLabelCounter) extends BinaryConfusionMatrix {

  /** number of true positives 预测正样本数量*/
  override def numTruePositives: Long = count.numPositives

  /** number of false positives 预测为负样本数量*/
  override def numFalsePositives: Long = count.numNegatives

  /** number of false negatives 真实正样本数量 - 预测正样本数量 = 预测正样本错误的数量*/
  override def numFalseNegatives: Long = totalCount.numPositives - count.numPositives

  /** number of true negatives */
  override def numTrueNegatives: Long = totalCount.numNegatives - count.numNegatives

  /** number of positives 真实的真正正样本数量*/
  override def numPositives: Long = totalCount.numPositives

  /** number of negatives 真实的真正负样本数量*/
  override def numNegatives: Long = totalCount.numNegatives
}
