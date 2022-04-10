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

package org.apache.spark.mllib.optimization

import scala.math._

import breeze.linalg.{norm => brzNorm, axpy => brzAxpy, Vector => BV}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.linalg.{Vectors, Vector}

/**
 * :: DeveloperApi ::
 * Class used to perform steps (weight update) using Gradient Descent methods.
 * 使用梯度下降的方式更新权重。。步长 * 梯度反方向
  *
 * For general minimization problems, or for regularized problems of the form
 *         min  L(w) + regParam * R(w), 其中L(w)表示损失函数
 * the compute function performs the actual update step, when given some
 * (e.g. stochastic) gradient direction for the loss L(w),
 * and a desired step-size (learning rate).
 *
 * The updater is responsible to also perform the update coming from the
 * regularization term R(w) (if any regularization is used).
  * regularization 表示规格化
 */
@DeveloperApi
abstract class Updater extends Serializable {
  /**
   * Compute an updated value for weights given the gradient, stepSize, iteration number and
   * regularization parameter. Also returns the regularization value regParam * R(w)
   * computed using the *updated* weights.
   * 给定梯度、步长、迭代次数、规格化参数，去更新权重
    * 同时也返回一个规格化后的regParam * R(w)
   * @param weightsOld - Column matrix of size dx1 where d is the number of features.
   * @param gradient - Column matrix of size dx1 where d is the number of features.
   * @param stepSize - step size across iterations
   * @param iter - Iteration number
   * @param regParam - Regularization parameter
   *
   * @return A tuple of 2 elements. The first element is a column matrix containing updated weights,
   *         and the second element is the regularization value computed using updated weights.
    *         第一个返回值是更新后的权重，第二个参数值是规格化后的值
   */
  def compute(
      weightsOld: Vector,
      gradient: Vector,
      stepSize: Double,
      iter: Int,
      regParam: Double): (Vector, Double)
}

/**
 * :: DeveloperApi ::
 * A simple updater for gradient descent *without* any regularization.
 * Uses a step-size decreasing with the square root of the number of iterations.
  * 没有规格化的，最简单的梯度下降更新权重的方法
 */
@DeveloperApi
class SimpleUpdater extends Updater {
  override def compute(
      weightsOld: Vector,//老权重
      gradient: Vector,//diff损失梯度向量--即梯度向量
      stepSize: Double,//步长
      iter: Int,//迭代次数
      regParam: Double): (Vector, Double) = { //返回新的权重
    //原则是一开始大步走,后期小步走
    val thisIterStepSize = stepSize / math.sqrt(iter) //因为迭代次数越大,math.sqrt(iter) 变越越小，比如81-100迭代都是转换成10. 总体上步长相同,迭代次数越多,分母越大,该值越小,即步长越小
    val brzWeights: BV[Double] = weightsOld.toBreeze.toDenseVector //老权重转换成向量
    //更新老权重,因为是梯度下降,因此反方向,即-,步长 * 梯度方向
    brzAxpy(-thisIterStepSize, gradient.toBreeze, brzWeights) //y += x * a ,即 brzWeights = brzWeights + -thisIterStepSize * gradient

    (Vectors.fromBreeze(brzWeights), 0) //返回更新后的权重brzWeights
  }
}

/**
 * :: DeveloperApi ::
 * Updater for L1 regularized problems.
 *          R(w) = ||w||_1
 * Uses a step-size decreasing with the square root of the number of iterations.

 * Instead of subgradient of the regularizer, the proximal operator for the
 * L1 regularization is applied after the gradient step. This is known to
 * result in better sparsity of the intermediate solution.
 *
 * The corresponding proximal operator for the L1 norm is the soft-thresholding
 * function. That is, each weight component is shrunk towards 0 by shrinkageVal.
 *
 * If w >  shrinkageVal, set weight component to w-shrinkageVal.
 * If w < -shrinkageVal, set weight component to w+shrinkageVal.
 * If -shrinkageVal < w < shrinkageVal, set weight component to 0.
 *
 * Equivalently, set weight component to signum(w) * max(0.0, abs(w) - shrinkageVal)
  *
  * 加入L1范式
 */
@DeveloperApi
class L1Updater extends Updater {
  override def compute(
      weightsOld: Vector,
      gradient: Vector,
      stepSize: Double,
      iter: Int,
      regParam: Double) //泛化系数
    : (Vector, Double) = {
    val thisIterStepSize = stepSize / math.sqrt(iter)
    // Take gradient step
    val brzWeights: BV[Double] = weightsOld.toBreeze.toDenseVector
    brzAxpy(-thisIterStepSize, gradient.toBreeze, brzWeights) //更新老权重
    // Apply proximal operator (soft thresholding) 计算泛化系数
    val shrinkageVal = regParam * thisIterStepSize
    var i = 0
    val len = brzWeights.length
    while (i < len) {
      val wi = brzWeights(i) //新的权重
      //signum(wi) 如果x大于0则返回1.0，小于0则返回-1.0，等于0则返回0
      brzWeights(i) = signum(wi) * max(0.0, abs(wi) - shrinkageVal) //更新权重
      i += 1
    }

    (Vectors.fromBreeze(brzWeights), brzNorm(brzWeights, 1.0) * regParam)
  }
}

/**
 * :: DeveloperApi ::
 * Updater for L2 regularized problems.
 *          R(w) = 1/2 ||w||^2
 * Uses a step-size decreasing with the square root of the number of iterations.
  * 加入L2范式
 */
@DeveloperApi
class SquaredL2Updater extends Updater {
  override def compute(
      weightsOld: Vector,
      gradient: Vector,
      stepSize: Double,
      iter: Int,
      regParam: Double): (Vector, Double) = {
    // add up both updates from the gradient of the loss (= step) as well as
    // the gradient of the regularizer (= regParam * weightsOld)
    // w' = w - thisIterStepSize * (gradient + regParam * w)
    // w' = (1 - thisIterStepSize * regParam) * w - thisIterStepSize * gradient
    val thisIterStepSize = stepSize / math.sqrt(iter)
    val brzWeights: BV[Double] = weightsOld.toBreeze.toDenseVector
    brzWeights :*= (1.0 - thisIterStepSize * regParam) //表示 向量brzWeights每一个特征 * 固定系数,系数的值就是(1.0 - thisIterStepSize * regParam)
    brzAxpy(-thisIterStepSize, gradient.toBreeze, brzWeights)
    val norm = brzNorm(brzWeights, 2.0) //2次正则化norm = 向量每一个维度^2之和，然后开根号，比如 10,10,10的结果就是17.320508075688775
    //如果brzNorm(brzWeights, 1.0) 表示1次正则化，即绝对值之和,比如-10,-10,-10,结果就是30
    (Vectors.fromBreeze(brzWeights), 0.5 * regParam * norm * norm)
  }
}

