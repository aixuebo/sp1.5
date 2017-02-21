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

package org.apache.spark.ml

import scala.annotation.varargs

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.param.{ParamMap, ParamPair}
import org.apache.spark.sql.DataFrame

/**
 * :: DeveloperApi ::
 * Abstract class for estimators that fit models to data.
 * 根据参数集合产生一个该评估模型具体类Estimator,然后调用fit方法将数据集合DataFrame产生合适的model模型
 */
@DeveloperApi
abstract class Estimator[M <: Model[M]] extends PipelineStage {

  /**
   * Fits a single model to the input data with optional parameters.
   *
   * @param dataset input dataset
   * @param firstParamPair the first param pair, overrides embedded params
   * @param otherParamPairs other param pairs.  These values override any specified in this
   *                        Estimator's embedded ParamMap.
   * @return fitted model
   */
  @varargs
  def fit(dataset: DataFrame, firstParamPair: ParamPair[_], otherParamPairs: ParamPair[_]*): M = {
    val map = new ParamMap()
      .put(firstParamPair)
      .put(otherParamPairs: _*)
    fit(dataset, map)
  }

  /**
   * Fits a model to the input data.
   * 针对该评估模型,处理该参数对应的数据集合
   */
  def fit(dataset: DataFrame): M

  /**
   * Fits a single model to the input data with provided parameter map.
   *
   * @param dataset input dataset
   * @param paramMap Parameter map.
   *                 These values override any specified in this Estimator's embedded ParamMap.
   * @return fitted model
   * 根据参数集合,返回适合的评估模型,然后处理数据集合dataset
   */
  def fit(dataset: DataFrame, paramMap: ParamMap): M = {
    copy(paramMap).fit(dataset)
  }

  /**
   * Fits multiple models to the input data with multiple sets of parameters.
   * The default implementation uses a for loop on each parameter map.
   * Subclasses could override this to optimize multi-model training.
   *
   * @param dataset input dataset
   * @param paramMaps An array of parameter maps.
   *                  These values override any specified in this Estimator's embedded ParamMap.
   * @return fitted models, matching the input parameter maps
   * 每一个paramMaps元素,都是一个参数集合,都可以产生一个评估模型去评估数据集合
   **/
  def fit(dataset: DataFrame, paramMaps: Array[ParamMap]): Seq[M] = {
    paramMaps.map(fit(dataset, _))
  }

  //通过参数集合,返回一个评估模型
  override def copy(extra: ParamMap): Estimator[M]
}
