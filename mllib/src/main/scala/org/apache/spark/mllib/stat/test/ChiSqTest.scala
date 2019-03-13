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

package org.apache.spark.mllib.stat.test

import breeze.linalg.{DenseMatrix => BDM}
import org.apache.commons.math3.distribution.ChiSquaredDistribution

import org.apache.spark.{SparkException, Logging}
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * Conduct the chi-squared test for the input RDDs using the specified method.
 * Goodness-of-fit test is conducted on two `Vectors`, whereas test of independence is conducted
 * on an input of type `Matrix` in which independence between columns is assessed.
 * We also provide a method for computing the chi-squared statistic between each feature and the
 * label for an input `RDD[LabeledPoint]`, return an `Array[ChiSquaredTestResult]` of size =
 * number of features in the input RDD.
 *
 * Supported methods for goodness of fit: `pearson` (default)
 * Supported methods for independence: `pearson` (default)
 *
 * More information on Chi-squared test: http://en.wikipedia.org/wiki/Chi-squared_test
  * 卡方检测
 */
private[stat] object ChiSqTest extends Logging {

  /**
   * @param name String name for the method.计算方法名字
   * @param chiSqFunc Function for computing the statistic given the observed and expected counts.如何根据参数输出具体的计算结果
    *  卡方分布的计算规则
   */
  case class Method(name: String, chiSqFunc: (Double, Double) => Double)

  // Pearson's chi-squared test: http://en.wikipedia.org/wiki/Pearson%27s_chi-squared_test
  //卡方分布计算公式,(实验值-预测值)^2/预测值
  val PEARSON = new Method("pearson", (observed: Double, expected: Double) => {
    val dev = observed - expected
    dev * dev / expected
  })

  // Null hypothesis for the two different types of chi-squared tests to be included in the result.
  object NullHypothesis extends Enumeration {
    type NullHypothesis = Value
    val goodnessOfFit = Value("observed follows the same distribution as expected.")  //不显著,即支持原假设
    val independence = Value("the occurrence of the outcomes is statistically independent.") //显著,即拒绝原假设
  }

  // Method identification based on input methodName string 目前只支持皮尔逊卡方分布计算方法
  private def methodFromString(methodName: String): Method = {
    methodName match {
      case PEARSON.name => PEARSON
      case _ => throw new IllegalArgumentException("Unrecognized method for Chi squared test.")
    }
  }

  /**
   * Conduct Pearson's independence test for each feature against the label across the input RDD.
   * The contingency table is constructed from the raw (feature, label) pairs and used to conduct
   * the independence test.
   * Returns an array containing the ChiSquaredTestResult for every feature against the label.
    * 每一个特征对应一个ChiSqTestResult对象
   */
  def chiSquaredFeatures(data: RDD[LabeledPoint],
      methodName: String = PEARSON.name): Array[ChiSqTestResult] = {
    val maxCategories = 10000 //分类最多数量，其实够用了，正常分类不会有一万个这么多
    val numCols = data.first().features.size //特征数量
    val results = new Array[ChiSqTestResult](numCols) //每一个特征对应一个ChiSqTestResult对象
    var labels: Map[Double, Int] = null //所有的lable集合,key是lable的value,value是序号
    // at most 1000 columns at a time 一次处理最多1000个特征
    val batchSize = 1000
    var batch = 0 //批量处理特征
    while (batch * batchSize < numCols) { //无限循环批量处理特征，直到特征都处理完
      // The following block of code can be cleaned up and made public as
      // chiSquared(data: RDD[(V1, V2)])
      val startCol = batch * batchSize //计算本次处理特征序号的开始位置和结束位置
      val endCol = startCol + math.min(batchSize, numCols - startCol)

      //pairCounts 返回Map[(col, feature, label), Long],即(特征序号,特征值,对应lable标签)出现了多少次相同的结果,即特征下某一个特征值与对应的lable出现了多少次
      val pairCounts = data.mapPartitions { iter =>
        val distinctLabels = mutable.HashSet.empty[Double] //存储所有出现的标签集合
        val allDistinctFeatures: Map[Int, mutable.HashSet[Double]] =
          Map((startCol until endCol).map(col => (col, mutable.HashSet.empty[Double])): _*) //key是每一个特征序号，value是该特征下对应的不同的特征value集合
        var i = 1
        iter.flatMap { case LabeledPoint(label, features) =>
          if (i % 1000 == 0) {
            //校验label的数量 以及 特征的value值，不能超过1万
            if (distinctLabels.size > maxCategories) {
              throw new SparkException(s"Chi-square test expect factors (categorical values) but "
                + s"found more than $maxCategories distinct label values.")
            }
            allDistinctFeatures.foreach { case (col, distinctFeatures) =>
              if (distinctFeatures.size > maxCategories) {
                throw new SparkException(s"Chi-square test expect factors (categorical values) but "
                  + s"found more than $maxCategories distinct values in column $col.")
              }
            }
          }
          i += 1
          distinctLabels += label //存储标签集合
          features.toArray.view.zipWithIndex.slice(startCol, endCol).map { case (feature, col) =>
            allDistinctFeatures(col) += feature //存储特征集合
            (col, feature, label) //特征序号,特征值,对应lable标签
          }
        }
      }.countByValue() //返回Map[(col, feature, label), Long],即(特征序号,特征值,对应lable标签)出现了多少次相同的结果,即特征下某一个特征值与对应的lable出现了多少次

      if (labels == null) {
        // Do this only once for the first column since labels are invariant across features.
        labels =
          pairCounts.keys.filter(_._1 == startCol) //只要第一特征对应的所有集合,即(特征序号,特征值,对应lable标签)
            .map(_._3).toArray.distinct.zipWithIndex.toMap //获取所有的lable集合,过滤重复,并且设置编号----这一步需要内存
      }
      val numLabels = labels.size //总label数量
      //获取(特征序号,特征值,对应lable标签)集合,按照特征序号分组
      pairCounts.keys.groupBy(_._1).map { case (col, keys) => //col表示特征序号,keys表示该特征下不同的(特征序号,特征值,对应lable标签)组合集合
        val features = keys.map(_._2).toArray.distinct.zipWithIndex.toMap //获取所有出现的特征值,并且过滤重复后编号,<double,int>,即特征值和编号
        val numRows = features.size //有多少个不同的特征值
        val contingency = new BDM(numRows, numLabels, new Array[Double](numRows * numLabels)) //矩阵,特征值数量*lable数量,特征值横坐标，纵坐标是lable
        keys.foreach { case (_, feature, label) => //特征值和lable映射
          val i = features(feature) //特征值对应的序号
          val j = labels(label) //lable对应的序号
          contingency(i, j) += pairCounts((col, feature, label)) //特征序号,lable = 出现次数
        }
        results(col) = chiSquaredMatrix(Matrices.fromBreeze(contingency), methodName)
      }
      batch += 1 //下一轮迭代
    }
    results
  }

  /*
   * Pearson's goodness of fit test on the input observed and expected counts/relative frequencies.
   * Uniform distribution is assumed when `expected` is not passed in.
   */
  def chiSquared(observed: Vector,
      expected: Vector = Vectors.dense(Array[Double]()),
      methodName: String = PEARSON.name): ChiSqTestResult = {

    // Validate input arguments
    val method = methodFromString(methodName)
    if (expected.size != 0 && observed.size != expected.size) {
      throw new IllegalArgumentException("observed and expected must be of the same size.")
    }
    val size = observed.size
    if (size > 1000) {//说明自由度太多了，可能效果并不是很好
      logWarning("Chi-squared approximation may not be accurate due to low expected frequencies "
        + s" as a result of a large number of categories: $size.")
    }
    val obsArr = observed.toArray
    val expArr = if (expected.size == 0) Array.tabulate(size)(_ => 1.0 / size) else expected.toArray

    //校验真实值和对照值必须有>0的数据
    if (!obsArr.forall(_ >= 0.0)) {
      throw new IllegalArgumentException("Negative entries disallowed in the observed vector.")
    }
    if (expected.size != 0 && ! expArr.forall(_ >= 0.0)) {
      throw new IllegalArgumentException("Negative entries disallowed in the expected vector.")
    }

    // Determine the scaling factor for expected
    val obsSum = obsArr.sum
    val expSum = if (expected.size == 0.0) 1.0 else expArr.sum
    val scale = if (math.abs(obsSum - expSum) < 1e-7) 1.0 else obsSum / expSum

    // compute chi-squared statistic
    val statistic = obsArr.zip(expArr).foldLeft(0.0) { case (stat, (obs, exp)) =>
      if (exp == 0.0) {
        if (obs == 0.0) {
          throw new IllegalArgumentException("Chi-squared statistic undefined for input vectors due"
            + " to 0.0 values in both observed and expected.")
        } else {
          return new ChiSqTestResult(0.0, size - 1, Double.PositiveInfinity, PEARSON.name,
            NullHypothesis.goodnessOfFit.toString)
        }
      }
      if (scale == 1.0) {
        stat + method.chiSqFunc(obs, exp)
      } else {
        stat + method.chiSqFunc(obs, exp * scale)
      }
    }
    val df = size - 1 //自由度
    val pValue = 1.0 - new ChiSquaredDistribution(df).cumulativeProbability(statistic)
    new ChiSqTestResult(pValue, df, statistic, PEARSON.name, NullHypothesis.goodnessOfFit.toString)
  }

  /*
   * Pearson's independence test on the input contingency matrix.
   * TODO: optimize for SparseMatrix when it becomes supported.
   * 参数矩阵:矩阵,特征值数量*lable数量,特征值横坐标，纵坐标是lable,矩阵内的值是特征值+lable下出现的次数
   * 例如:实验值
   * 10 20
   * 30 50
   * ===》转换为
   * 10 20 30
   * 30 50 80
   * 40 70 110
   * 计算预测值
   * 40/110 = 概率
   * 70/100 = 概率
   * 预测值为
   * 30 * (40/110) 30 * (70/110)
   * 80 * (40/110) 80 * (70/110)
   * 即行总和*列总和/all
   */
  def chiSquaredMatrix(counts: Matrix, methodName: String = PEARSON.name): ChiSqTestResult = {
    val method = methodFromString(methodName)
    val numRows = counts.numRows //特征值数量
    val numCols = counts.numCols //lable数量

    // get row and column sums
    val colSums = new Array[Double](numCols)
    val rowSums = new Array[Double](numRows)
    val colMajorArr = counts.toArray //矩阵按照列的顺序,将元素转换成numRows*numCols的数组
    val colMajorArrLen = colMajorArr.length //数组大小

    //填充矩阵中出现的次数
    var i = 0
    while (i < colMajorArrLen) { //循环每一个元素
      val elem = colMajorArr(i)
      if (elem < 0.0) { //因为是出现次数,不可能出现负数
        throw new IllegalArgumentException("Contingency table cannot contain negative entries.")
      }
      //假设row=50,即50个特征值,col=10,即10个分类标签
      //colMajorArr数组0-49为第一个列,即第一个分类的所有特征值出现的个数
      colSums(i / numRows) += elem //列求和
      rowSums(i % numRows) += elem //行求和
      i += 1
    }
    val total = colSums.sum //总次数

    // second pass to collect statistic
    var statistic = 0.0
    var j = 0
    while (j < colMajorArrLen) { //再次遍历整个矩阵数据
      val col = j / numRows //第几列的值
      val colSum = colSums(col) //该列对应的总和
      if (colSum == 0.0) {
        throw new IllegalArgumentException("Chi-squared statistic undefined for input matrix due to"
          + s"0 sum in column [$col].")
      }
      val row = j % numRows //行
      val rowSum = rowSums(row) //行的值
      if (rowSum == 0.0) {
        throw new IllegalArgumentException("Chi-squared statistic undefined for input matrix due to"
          + s"0 sum in row [$row].")
      }
      //计算预期值
      val expected = colSum * rowSum / total
      statistic += method.chiSqFunc(colMajorArr(j), expected) //计算
      j += 1
    }
    val df = (numCols - 1) * (numRows - 1) //自由度
    if (df == 0) {
      // 1 column or 1 row. Constant distribution is independent of anything.
      // pValue = 1.0 and statistic = 0.0 in this case.
      new ChiSqTestResult(1.0, 0, 0.0, methodName, NullHypothesis.independence.toString)
    } else {
      //比如卡方分布自由度为1的时候,5这个值以内出现的概率是多大，结果是0.974652681322532。那么5之外的概率就是1-0.974652681322532,其实已经很小了
      //println(new ChiSquaredDistribution(1).cumulativeProbability(5))
      val pValue = 1.0 - new ChiSquaredDistribution(df).cumulativeProbability(statistic)
      new ChiSqTestResult(pValue, df, statistic, methodName, NullHypothesis.independence.toString)
    }
  }
}
