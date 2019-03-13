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

package org.apache.spark.mllib.linalg.distributed

import breeze.linalg.{DenseMatrix => BDM}

import org.apache.spark.annotation.Since

/**
 * Represents a distributively stored matrix backed by one or more RDDs.
  * 代表分布式环境下如何存储一个矩阵
 */
@Since("1.0.0")
trait DistributedMatrix extends Serializable {

  /** Gets or computes the number of rows. 矩阵行数*/
  @Since("1.0.0")
  def numRows(): Long

  /** Gets or computes the number of columns. 矩阵列数*/
  @Since("1.0.0")
  def numCols(): Long

  /** Collects data and assembles a local dense breeze matrix (for test only).
    * 仅仅用于测试,表示本地存储的矩阵集合
    **/
  private[mllib] def toBreeze(): BDM[Double]
}
