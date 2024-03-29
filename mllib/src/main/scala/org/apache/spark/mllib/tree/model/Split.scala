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

package org.apache.spark.mllib.tree.model

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.mllib.tree.configuration.FeatureType.FeatureType
import org.apache.spark.mllib.tree.configuration.FeatureType
import org.apache.spark.mllib.tree.configuration.FeatureType.FeatureType

/**
 * :: DeveloperApi ::
 * Split applied to a feature
 * @param feature feature index
 * @param threshold Threshold for continuous feature.
 *                  Split left if feature <= threshold, else right.
 * @param featureType type of feature -- categorical or continuous
 * @param categories Split left if categorical feature value is in this set, else right.如何是分类属性的拆分点,则该节点存储左边分类属性的值
  *   表示决策树的一个分位点
 */
@Since("1.0.0")
@DeveloperApi
case class Split(
    @Since("1.0.0") feature: Int,//第几个特征
    @Since("1.0.0") threshold: Double,//该分位点的具体值
    @Since("1.0.0") featureType: FeatureType,//该特征是连续值还是离散值
    @Since("1.0.0") categories: List[Double]) {

  override def toString: String = {
    s"Feature = $feature, threshold = $threshold, featureType = $featureType, " +
      s"categories = $categories"
  }
}

/**
 * Split with minimum threshold for continuous features. Helps with the smallest bin creation.
 * @param feature feature index
 * @param featureType type of feature -- categorical or continuous 分类还是回归问题
  *  最左边边界
 */
private[tree] class DummyLowSplit(feature: Int, featureType: FeatureType)
  extends Split(feature, Double.MinValue, featureType, List())

/**
 * Split with maximum threshold for continuous features. Helps with the highest bin creation.
 * @param feature feature index
 * @param featureType type of feature -- categorical or continuous
  * 最右边边界
 */
private[tree] class DummyHighSplit(feature: Int, featureType: FeatureType)
  extends Split(feature, Double.MaxValue, featureType, List())

/**
 * Split with no acceptable feature values for categorical features. Helps with the first bin
 * creation.
 * @param feature feature index
 * @param featureType type of feature -- categorical or continuous
 */
private[tree] class DummyCategoricalSplit(feature: Int, featureType: FeatureType)
  extends Split(feature, Double.MaxValue, featureType, List())
