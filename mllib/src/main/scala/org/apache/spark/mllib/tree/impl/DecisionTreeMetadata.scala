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

package org.apache.spark.mllib.tree.impl

import scala.collection.mutable

import org.apache.spark.Logging
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.QuantileStrategy._
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.impurity.Impurity
import org.apache.spark.rdd.RDD

/**
 * Learning and dataset metadata for DecisionTree.
 * 决策树学习的原始数据
 * @param numClasses    For classification: labels can take values {0, ..., numClasses - 1}.预测的分类标签数量
 *                      For regression: fixed at 0 (no meaning).回归时该值为0
 * @param maxBins  Maximum number of bins, for all features.
 * @param featureArity  Map: categorical feature index --> arity.特征下标的重要性
 *                      I.e., the feature takes values in {0, ..., arity - 1}.
 * @param numBins  Number of bins for each feature.
 */
private[spark] class DecisionTreeMetadata(
    val numFeatures: Int,//特征维度数量,即向量特征有多少维
    val numExamples: Long,//样本空间大小,即样本条数
    val numClasses: Int,//样本lable的分类数量,即最终结果是由多少个分类组成的
    val maxBins: Int,//离散型特征组多允许多少个value数量
    val featureArity: Map[Int, Int],//哪些特征是离散型的特征 以及 对应的特征值数量
    val unorderedFeatures: Set[Int],//真实的特征数非常少，少于log2^maxCategories，我们认为他是无序的
    val numBins: Array[Int],//每一个特征对应多少个特征值
    val impurity: Impurity,
    val quantileStrategy: QuantileStrategy,
    val maxDepth: Int,
    val minInstancesPerNode: Int,
    val minInfoGain: Double,
    val numTrees: Int,//森林需要多少颗决策树
    val numFeaturesPerNode: Int) //每一个节点特征选择的数量
  extends Serializable {

  //true表示该特征非常多的特征值
  def isUnordered(featureIndex: Int): Boolean = unorderedFeatures.contains(featureIndex)

  def isClassification: Boolean = numClasses >= 2 //是否是分类问题

  def isMulticlass: Boolean = numClasses > 2 //是否是大于2个分类的分类问题

  def isMulticlassWithCategoricalFeatures: Boolean = isMulticlass && (featureArity.size > 0) //是否是大于2个以上分类 并且 有离散型特征

  def isCategorical(featureIndex: Int): Boolean = featureArity.contains(featureIndex) //true表示离散特征

  def isContinuous(featureIndex: Int): Boolean = !featureArity.contains(featureIndex) //true表示连续特征

  /**
   * Number of splits for the given feature.
   * For unordered features, there are 2 bins per split.
   * For ordered features, there is 1 more bin than split.
   */
  def numSplits(featureIndex: Int): Int = if (isUnordered(featureIndex)) {//特征太少,无序的,如何拆分
    numBins(featureIndex) >> 1
  } else {
    numBins(featureIndex) - 1
  }


  /**
   * Set number of splits for a continuous feature.
   * For a continuous feature, number of bins is number of splits plus 1.
    * 设置该特征对应的分位点数量
   */
  def setNumSplits(featureIndex: Int, numSplits: Int) {
    require(isContinuous(featureIndex),
      s"Only number of bin for a continuous feature can be set.")
    numBins(featureIndex) = numSplits + 1
  }

  /**
   * Indicates if feature subsampling is being used.
   */
  def subsamplingFeatures: Boolean = numFeatures != numFeaturesPerNode

}

private[spark] object DecisionTreeMetadata extends Logging {

  /**
   * Construct a [[DecisionTreeMetadata]] instance for this dataset and parameters.
   * This computes which categorical features will be ordered vs. unordered,
   * as well as the number of splits and bins for each feature.
    * 根据数据源去更新元数据内容
   */
  def buildMetadata(
      input: RDD[LabeledPoint],//样本集合
      strategy: Strategy,
      numTrees: Int,//森林需要的决策树数量
      featureSubsetStrategy: String) //决策树选择节点数量策略
    : DecisionTreeMetadata = {

    //因为元素特征的维度相同,因此获取第一个特征的维度size即可,
    //即该标签对应的向量有多少个维度
    val numFeatures = input.map(_.features.size).take(1).headOption.getOrElse {
      throw new IllegalArgumentException(s"DecisionTree requires size of input RDD > 0, " +
        s"but was given by empty one.")
    }
    val numExamples = input.count()//一共多少条数据
    val numClasses = strategy.algo match {
      case Classification => strategy.numClasses //分类数,即分成多少个类
      case Regression => 0
    }

    val maxPossibleBins = math.min(strategy.maxBins, numExamples).toInt //最多可能性也就是样本数量numExamples
    if (maxPossibleBins < strategy.maxBins) {//打印日志,因为例子numExamples数据太少,因此减少了maxBins的值
      //进入该条件,说明strategy.maxBins > numExamples 样本总数,因此要减少strategy.maxBins为样本总数
      logWarning(s"DecisionTree reducing maxBins from ${strategy.maxBins} to $maxPossibleBins" +
        s" (= number of training instances)")
    }

    // We check the number of bins here against maxPossibleBins.
    // This needs to be checked here instead of in Strategy since maxPossibleBins can be modified
    // based on the number of training examples.
    if (strategy.categoricalFeaturesInfo.nonEmpty) {
      val maxCategoriesPerFeature = strategy.categoricalFeaturesInfo.values.max //获取最大的value
      val maxCategory =
        strategy.categoricalFeaturesInfo.find(_._2 == maxCategoriesPerFeature).get._1 //获取最大value对应的key,即哪个下标定义了最大的权重
      //要求最大的特征离散value数量 <= maxPossibleBins
      require(maxCategoriesPerFeature <= maxPossibleBins,
        s"DecisionTree requires maxBins (= $maxPossibleBins) to be at least as large as the " +
        s"number of values in each categorical feature, but categorical feature $maxCategory " +
        s"has $maxCategoriesPerFeature values. Considering remove this and other categorical " +
        "features with a large number of values, or add more training examples.") //提示哪个key对应的权重设置大了
    }

    val unorderedFeatures = new mutable.HashSet[Int]()//真实的特征数非常少，少于log2^maxCategories，我们认为他是无序的
    //经过下面这行代码后,每一个特征有多少个不同的特征值就已经确定了,默认值就是不管连续性还是离散型都是最大的特征值数量
    val numBins = Array.fill[Int](numFeatures)(maxPossibleBins) //创建int一维数组,size大小为特征数，默认值都是maxPossibleBins---知道某一个离散型特征有多少个分类值
    if (numClasses > 2) {//多分类问题
      // Multiclass classification //基本上等于log2^maxPossibleBins 比如最大分类数为126，那么返回值是6.但是不是绝对的等于log2^maxPossibleBins
      val maxCategoriesForUnorderedFeature =
        ((math.log(maxPossibleBins / 2 + 1) / math.log(2.0)) + 1).floor.toInt
      strategy.categoricalFeaturesInfo.foreach { case (featureIndex, numCategories) => //特征序号,特征对应多少个value值
        // Hack: If a categorical feature has only 1 category, we treat it as continuous.
        //如果离散型特征只有一个特征值，我们认为他是连续性的
        // TODO(SPARK-9957): Handle this properly by filtering out those features.
        if (numCategories > 1) {//多余1个以上离散值
          // Decide if some categorical features should be treated as unordered features,
          //是否一些离散的特征被处理成无序的特征
          //  which require 2 * ((1 << numCategories - 1) - 1) bins.
          // We do this check with log values to prevent overflows in case numCategories is large.
          //log校验，防止numCategories真实的分类数太多，导致溢出
          // The next check is equivalent to: 2 * ((1 << numCategories - 1) - 1) <= maxBins
          if (numCategories <= maxCategoriesForUnorderedFeature) {//真实的特征数非常少，少于log2^maxCategories，我们认为他是无序的
            unorderedFeatures.add(featureIndex)
            numBins(featureIndex) = numUnorderedBins(numCategories) //扩大真实分类的箱数
          } else {
            numBins(featureIndex) = numCategories //设置该特征真实有多少个分类
          }
        }
      }
    } else {
      // Binary classification or regression
      strategy.categoricalFeaturesInfo.foreach { case (featureIndex, numCategories) => //特征序号,特征对应多少个value值
        // If a categorical feature has only 1 category, we treat it as continuous: SPARK-9957
        if (numCategories > 1) {
          numBins(featureIndex) = numCategories
        }
      }
    }

    // Set number of features to use per node (for random forests).
    //每一个节点特征选择的数量
    val _featureSubsetStrategy = featureSubsetStrategy match {
      case "auto" => //如果是自动的,该如何处理
        if (numTrees == 1) {
          "all"
        } else {
          if (strategy.algo == Classification) {
            "sqrt"
          } else {
            "onethird"
          }
        }
      case _ => featureSubsetStrategy
    }
    val numFeaturesPerNode: Int = _featureSubsetStrategy match {
      case "all" => numFeatures //所有特征都参与
      case "sqrt" => math.sqrt(numFeatures).ceil.toInt //特征开方,比如特征100,则每一个节点只选择10个特征
      case "log2" => math.max(1, (math.log(numFeatures) / math.log(2)).ceil.toInt) //最少也要去1个特征,正常情况是特征的对数,具体逻辑没太懂
      case "onethird" => (numFeatures / 3.0).ceil.toInt //特征的三分之一
    }

    new DecisionTreeMetadata(numFeatures, numExamples, numClasses, numBins.max,
      strategy.categoricalFeaturesInfo, unorderedFeatures.toSet, numBins,
      strategy.impurity, strategy.quantileCalculationStrategy, strategy.maxDepth,
      strategy.minInstancesPerNode, strategy.minInfoGain, numTrees, numFeaturesPerNode)
  }

  /**
   * Version of [[DecisionTreeMetadata#buildMetadata]] for DecisionTree.
    * 默认一棵树 以及 全部特征用于节点计算
   */
  def buildMetadata(
      input: RDD[LabeledPoint],
      strategy: Strategy): DecisionTreeMetadata = {
    buildMetadata(input, strategy, numTrees = 1, featureSubsetStrategy = "all")
  }

    /**
   * Given the arity of a categorical feature (arity = number of categories),
   * return the number of bins for the feature if it is to be treated as an unordered feature.
   * There is 1 split for every partitioning of categories into 2 disjoint, non-empty sets;
   * there are math.pow(2, arity - 1) - 1 such splits.
   * Each split has 2 corresponding bins.
      * 给定一个特征的离散分类数,返回该功能的箱数(此时假设该特征为无序的特征),
      * 该值返回的大概是2^n次方个箱数
      **/
  def numUnorderedBins(arity: Int): Int = 2 * ((1 << arity - 1) - 1)

}
