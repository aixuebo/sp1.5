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

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.loss.{LogLoss, SquaredError, Loss}

/**
 * :: Experimental ::
 * Configuration options for [[org.apache.spark.mllib.tree.GradientBoostedTrees]].
 *
 * @param treeStrategy Parameters for the tree algorithm. We support regression and binary
 *                     classification for boosting. Impurity setting will be ignored.
 * @param loss Loss function used for minimization during gradient boosting.
 * @param numIterations Number of iterations of boosting.  In other words, the number of
 *                      weak hypotheses used in the final model.
 * @param learningRate Learning rate for shrinking the contribution of each estimator. The
 *                     learning rate should be between in the interval (0, 1]
 * @param validationTol Useful when runWithValidation is used. If the error rate on the
 *                      validation input between two iterations is less than the validationTol
 *                      then stop.  Ignored when
 *                      [[org.apache.spark.mllib.tree.GradientBoostedTrees.run()]] is used.
 */
@Since("1.2.0")
@Experimental
case class BoostingStrategy @Since("1.4.0") (
    // Required boosting parameters
    @Since("1.2.0") @BeanProperty var treeStrategy: Strategy,
    @Since("1.2.0") @BeanProperty var loss: Loss,
    // Optional boosting parameters
    @Since("1.2.0") @BeanProperty var numIterations: Int = 100,
    @Since("1.2.0") @BeanProperty var learningRate: Double = 0.1,
    @Since("1.4.0") @BeanProperty var validationTol: Double = 1e-5) extends Serializable {

  /**
   * Check validity of parameters.
   * Throws exception if invalid.
   */
  private[tree] def assertValid(): Unit = {
    treeStrategy.algo match {
      case Classification =>
        require(treeStrategy.numClasses == 2,
          "Only binary classification is supported for boosting.") //只支持2分类
      case Regression =>
        // nothing
      case _ =>
        throw new IllegalArgumentException(
          s"BoostingStrategy given invalid algo parameter: ${treeStrategy.algo}." +
            s"  Valid settings are: Classification, Regression.")
    }
    require(learningRate > 0 && learningRate <= 1,
      "Learning rate should be in range (0, 1]. Provided learning rate is " + s"$learningRate.")
  }
}

@Since("1.2.0")
@Experimental
object BoostingStrategy {

  /**
   * Returns default configuration for the boosting algorithm
   * @param algo Learning goal.  Supported: "Classification" or "Regression"
   * @return Configuration for boosting algorithm
    * 设置目标是回归还是分类
   */
  @Since("1.2.0")
  def defaultParams(algo: String): BoostingStrategy = {
    defaultParams(Algo.fromString(algo))
  }

  /**
   * Returns default configuration for the boosting algorithm
   * @param algo Learning goal.  Supported:
   *             [[org.apache.spark.mllib.tree.configuration.Algo.Classification]],
   *             [[org.apache.spark.mllib.tree.configuration.Algo.Regression]]
   * @return Configuration for boosting algorithm
   */
  @Since("1.3.0")
  def defaultParams(algo: Algo): BoostingStrategy = {
    val treeStrategy = Strategy.defaultStrategy(algo)
    treeStrategy.maxDepth = 3
    algo match {
      case Algo.Classification =>
        treeStrategy.numClasses = 2
        new BoostingStrategy(treeStrategy, LogLoss)
      case Algo.Regression =>
        new BoostingStrategy(treeStrategy, SquaredError) //默认用均方差算法
      case _ =>
        throw new IllegalArgumentException(s"$algo is not supported by boosting.")
    }
  }
}
