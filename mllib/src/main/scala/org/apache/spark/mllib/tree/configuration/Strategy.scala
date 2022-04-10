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

package org.apache.spark.mllib.tree.configuration

import scala.beans.BeanProperty
import scala.collection.JavaConverters._

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.mllib.tree.impurity.{Variance, Entropy, Gini, Impurity}
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.QuantileStrategy._

/**
 * :: Experimental ::
 * Stores all the configuration options for tree construction
 * 用于存储决策树需要的全部配置
 * @param algo  Learning goal.  Supported:
 *              [[org.apache.spark.mllib.tree.configuration.Algo.Classification]],
 *              [[org.apache.spark.mllib.tree.configuration.Algo.Regression]]
  *              目标是分类还是回归
 * @param impurity Criterion used for information gain calculation.如何计算信息的混乱程度
 *                 Supported for Classification: [[org.apache.spark.mllib.tree.impurity.Gini]],
 *                  [[org.apache.spark.mllib.tree.impurity.Entropy]].
 *                 Supported for Regression: [[org.apache.spark.mllib.tree.impurity.Variance]].
  *                 混乱程度--分类:熵和gini、回归:方差
 * @param maxDepth Maximum depth of the tree.
 *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
  *                树的最大深度
 * @param numClasses Number of classes for classification.
 *                                    (Ignored for regression.)
 *                                    Default value is 2 (binary classification).
  *                  分类数量
 * @param maxBins Maximum number of bins used for discretizing continuous features and
 *                for choosing how to split on features at each node.
 *                More bins give higher granularity.
  *               最多分配多少个拆分点，被用来对连续值特征进行离散化
  *               分类问题时,不允许超过这个阈值,否则分类数量过大不好
 * @param quantileCalculationStrategy Algorithm for calculating quantiles.  Supported:
 *                             [[org.apache.spark.mllib.tree.configuration.QuantileStrategy.Sort]]
  *                             如何计算分位数
 * @param categoricalFeaturesInfo A map storing information about the categorical variables and the
 *                                number of discrete values they take. For example, an entry (n ->
 *                                k) implies the feature n is categorical with k categories 0,
 *                                1, 2, ... , k-1. It's important to note that features are
 *                                zero-indexed.
  *                                存储分类变量和离散的值信息,entry (n -> k)该表是n这个特征有k个分类
  *                                注意:从0开始计数
 * @param minInstancesPerNode Minimum number of instances each child must have after split.
 *                            Default value is 1. If a split cause left or right child
 *                            to have less than minInstancesPerNode,
 *                            this split will not be considered as a valid split.
  *                           拆分后每一个子节点必须有的最小实例样本数,避免过拟合。
 * @param minInfoGain Minimum information gain a split must get. Default value is 0.0.
 *                    If a split has less information gain than minInfoGain,
 *                    this split will not be considered as a valid split.
  *                   最少的gini值,默认是0,为了防止过拟合,如果小于该值,是不允许进行split拆分的
 * @param maxMemoryInMB Maximum memory in MB allocated to histogram aggregation. Default value is
 *                      256 MB.
  *                      内存使用
 * @param subsamplingRate Fraction of the training data used for learning decision tree.
  *                        抽样系数比例
 * @param useNodeIdCache If this is true, instead of passing trees to executors, the algorithm will
 *                      maintain a separate RDD of node Id cache for each row.
 * @param checkpointInterval How often to checkpoint when the node Id cache gets updated.
 *                           E.g. 10 means that the cache will get checkpointed every 10 updates. If
 *                           the checkpoint directory is not set in
 *                           [[org.apache.spark.SparkContext]], this setting is ignored.
  *                           如果checkpoint目录没有被设置,则可以被忽略该设置。
  *                           设置为10的时候,表示每10次更新,则要将缓存checkpointed一次
 */
@Since("1.0.0")
@Experimental
class Strategy @Since("1.3.0") (
    @Since("1.0.0") @BeanProperty var algo: Algo,//决策树用于分类还是回归
    @Since("1.0.0") @BeanProperty var impurity: Impurity,
    @Since("1.0.0") @BeanProperty var maxDepth: Int,
    @Since("1.2.0") @BeanProperty var numClasses: Int = 2,//目标产生多少个分类
    @Since("1.0.0") @BeanProperty var maxBins: Int = 32,
    @Since("1.0.0") @BeanProperty var quantileCalculationStrategy: QuantileStrategy = Sort,
    @Since("1.0.0") @BeanProperty var categoricalFeaturesInfo: Map[Int, Int] = Map[Int, Int](),//表示哪些特征是离散型特征
    @Since("1.2.0") @BeanProperty var minInstancesPerNode: Int = 1,
    @Since("1.2.0") @BeanProperty var minInfoGain: Double = 0.0,
    @Since("1.0.0") @BeanProperty var maxMemoryInMB: Int = 256,
    @Since("1.2.0") @BeanProperty var subsamplingRate: Double = 1,
    @Since("1.2.0") @BeanProperty var useNodeIdCache: Boolean = false,
    @Since("1.2.0") @BeanProperty var checkpointInterval: Int = 10) extends Serializable {

  /**
   * true表示多个分类
   */
  @Since("1.2.0")
  def isMulticlassClassification: Boolean = {
    algo == Classification && numClasses > 2
  }

  /**
   */
  @Since("1.2.0")
  def isMulticlassWithCategoricalFeatures: Boolean = {
    isMulticlassClassification && (categoricalFeaturesInfo.size > 0)
  }

  /**
   * Java-friendly constructor for [[org.apache.spark.mllib.tree.configuration.Strategy]]
   */
  @Since("1.1.0")
  def this(
      algo: Algo,
      impurity: Impurity,
      maxDepth: Int,
      numClasses: Int,
      maxBins: Int,
      categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer]) {
    this(algo, impurity, maxDepth, numClasses, maxBins, Sort,
      categoricalFeaturesInfo.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap)
  }

  /**
   * Sets Algorithm using a String.
   */
  @Since("1.2.0")
  def setAlgo(algo: String): Unit = algo match {
    case "Classification" => setAlgo(Classification)
    case "Regression" => setAlgo(Regression)
  }

  /**
   * Sets categoricalFeaturesInfo using a Java Map.
    * 设置分类的特征需要拆分多少个节点
   */
  @Since("1.2.0")
  def setCategoricalFeaturesInfo(
      categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer]): Unit = {
    this.categoricalFeaturesInfo =
      categoricalFeaturesInfo.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap
  }

  /**
   * Check validity of parameters.
   * Throws exception if invalid.
   */
  private[tree] def assertValid(): Unit = {
    algo match {
      case Classification =>
        require(numClasses >= 2,
          s"DecisionTree Strategy for Classification must have numClasses >= 2," +
          s" but numClasses = $numClasses.")
        require(Set(Gini, Entropy).contains(impurity),
          s"DecisionTree Strategy given invalid impurity for Classification: $impurity." +
          s"  Valid settings: Gini, Entropy")
      case Regression =>
        require(impurity == Variance,
          s"DecisionTree Strategy given invalid impurity for Regression: $impurity." +
          s"  Valid settings: Variance")
      case _ =>
        throw new IllegalArgumentException(
          s"DecisionTree Strategy given invalid algo parameter: $algo." +
          s"  Valid settings are: Classification, Regression.")
    }
    require(maxDepth >= 0, s"DecisionTree Strategy given invalid maxDepth parameter: $maxDepth." +
      s"  Valid values are integers >= 0.")
    require(maxBins >= 2, s"DecisionTree Strategy given invalid maxBins parameter: $maxBins." +
      s"  Valid values are integers >= 2.")
    require(minInstancesPerNode >= 1,
      s"DecisionTree Strategy requires minInstancesPerNode >= 1 but was given $minInstancesPerNode")
    require(maxMemoryInMB <= 10240,
      s"DecisionTree Strategy requires maxMemoryInMB <= 10240, but was given $maxMemoryInMB")
    require(subsamplingRate > 0 && subsamplingRate <= 1,
      s"DecisionTree Strategy requires subsamplingRate <=1 and >0, but was given " +
      s"$subsamplingRate")
  }

  /**
   * Returns a shallow copy of this instance.
   */
  @Since("1.2.0")
  def copy: Strategy = {
    new Strategy(algo, impurity, maxDepth, numClasses, maxBins,
      quantileCalculationStrategy, categoricalFeaturesInfo, minInstancesPerNode, minInfoGain,
      maxMemoryInMB, subsamplingRate, useNodeIdCache, checkpointInterval)
  }
}

@Since("1.2.0")
@Experimental
object Strategy {

  /**
   * Construct a default set of parameters for [[org.apache.spark.mllib.tree.DecisionTree]]
   * @param algo  "Classification" or "Regression"
   */
  @Since("1.2.0")
  def defaultStrategy(algo: String): Strategy = {
    defaultStrategy(Algo.fromString(algo))
  }

  /**
   * Construct a default set of parameters for [[org.apache.spark.mllib.tree.DecisionTree]]
   * @param algo Algo.Classification or Algo.Regression
   */
  @Since("1.3.0")
  def defaultStrategy(algo: Algo): Strategy = algo match {
    case Algo.Classification =>
      new Strategy(algo = Classification, impurity = Gini, maxDepth = 10,
        numClasses = 2)
    case Algo.Regression =>
      new Strategy(algo = Regression, impurity = Variance, maxDepth = 10,
        numClasses = 0)
  }

  @deprecated("Use Strategy.defaultStrategy instead.", "1.5.0")
  @Since("1.2.0")
  def defaultStategy(algo: Algo): Strategy = defaultStrategy(algo)

}
