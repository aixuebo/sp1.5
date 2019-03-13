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

package org.apache.spark.mllib.util

import scala.reflect.ClassTag

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV}

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PartitionwiseSampledRDD
import org.apache.spark.util.random.BernoulliCellSampler
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{SparseVector, DenseVector, Vector, Vectors}
import org.apache.spark.mllib.linalg.BLAS.dot
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
 * Helper methods to load, save and pre-process data used in ML Lib.
 */
@Since("0.8.0")
object MLUtils {

  private[mllib] lazy val EPSILON = {
    var eps = 1.0
    while ((1.0 + (eps / 2.0)) != 1.0) {
      eps /= 2.0
    }
    eps
  }

  /**
   * Loads labeled data in the LIBSVM format into an RDD[LabeledPoint].
   * The LIBSVM format is a text-based format used by LIBSVM and LIBLINEAR.
   * Each line represents a labeled sparse feature vector using the following format:
   * {{{label index1:value1 index2:value2 ...}}}
   * where the indices are one-based and in ascending order.
   * This method parses each line into a [[org.apache.spark.mllib.regression.LabeledPoint]],
   * where the feature indices are converted to zero-based.
   * 格式是 lable 特征向量序号:特征向量value 特征向量序号:特征向量value
    * 注意 特征序号要保证增序排序
    * lable一定是double类型的 特征序号是int类型,特征值是double类型
    * 序号要从1开始计算，方法会将1变成0,即方法内会转换成0开头的序号
    *
    * @param sc Spark context
   * @param path file or directory path in any Hadoop-supported file system URI
   * @param numFeatures number of features, which will be determined from the input data if a
   *                    nonpositive value is given. This is useful when the dataset is already split
   *                    into multiple files and you want to load them separately, because some
   *                    features may not present in certain files, which leads to inconsistent
   *                    feature dimensions.有多少个特征
   * @param minPartitions min number of partitions //多少个并行任务
   * @return labeled data stored as an RDD[LabeledPoint]
    *
    *        demo:
    *        val filepath1 = "/Users/maming/Desktop/mm/document/gittest/python/decision_demo.txt"
    *        val data = MLUtils.loadLibSVMFile(sc, filepath1)
    *       data.foreach(println(_))
    *
    *        1 1:1 2:1 3:1
    *        1 1:1 2:1 3:2
    *        1 1:1 2:2 3:1
    *        0 1:1 2:2 3:2
    *        1 1:2 2:1 3:1
    *        0 1:3 2:1 3:2
    *        0 1:3 2:1 3:1
    *        0 1:3 2:2 3:1
   */
  @Since("1.0.0")
  def loadLibSVMFile(
      sc: SparkContext,
      path: String,
      numFeatures: Int,
      minPartitions: Int): RDD[LabeledPoint] = {
    val parsed = sc.textFile(path, minPartitions)
      .map(_.trim)
      .filter(line => !(line.isEmpty || line.startsWith("#"))) //备注可用##进行过滤,但是必须以#开头正行进行注释
      .map { line =>
        val items = line.split(' ')
        val label = items.head.toDouble
      //list<Double>转换成List<Int,Double>
        val (indices, values) = items.tail.filter(_.nonEmpty).map { item =>
          val indexAndValue = item.split(':')
          val index = indexAndValue(0).toInt - 1 // Convert 1-based indices to 0-based.
          val value = indexAndValue(1).toDouble
          (index, value)
        }//List<Int,Double>
          .unzip//转换成元祖<List<Int>,List<Double>>

        // check if indices are one-based and in ascending order
      //校验序号顺序一定是按照增序排序的
        var previous = -1 //前一个序号
        var i = 0
        val indicesLength = indices.length //总长度
        while (i < indicesLength) {//从头到尾遍历
          val current = indices(i) //序号
          require(current > previous, "indices should be one-based and in ascending order" )
          previous = current
          i += 1
        }

        (label, indices.toArray, values.toArray)
      }

    // Determine number of features.计算有多少个不同的特征
    val d = if (numFeatures > 0) {
      numFeatures
    } else {
      parsed.persist(StorageLevel.MEMORY_ONLY)
      parsed.map { case (label, indices, values) =>
        indices.lastOption.getOrElse(0) //获取特征向量的序号,即特征向量有多少个特征值
      }.reduce(math.max) + 1 //计算最大的特征值 + 1,因为下标是从0 开始计算的
    }

    parsed.map { case (label, indices, values) =>
      LabeledPoint(label, Vectors.sparse(d, indices, values))//稀疏向量
    }
  }

  // Convenient methods for `loadLibSVMFile`.

  @Since("1.0.0")
  @deprecated("use method without multiclass argument, which no longer has effect", "1.1.0")
  def loadLibSVMFile(
      sc: SparkContext,
      path: String,
      multiclass: Boolean,//multiclass参数不在有效
      numFeatures: Int,
      minPartitions: Int): RDD[LabeledPoint] =
    loadLibSVMFile(sc, path, numFeatures, minPartitions)

  /**
   * Loads labeled data in the LIBSVM format into an RDD[LabeledPoint], with the default number of
   * partitions.
   */
  @Since("1.0.0")
  def loadLibSVMFile(
      sc: SparkContext,
      path: String,
      numFeatures: Int): RDD[LabeledPoint] =
    loadLibSVMFile(sc, path, numFeatures, sc.defaultMinPartitions)

  @Since("1.0.0")
  @deprecated("use method without multiclass argument, which no longer has effect", "1.1.0")
  def loadLibSVMFile(
      sc: SparkContext,
      path: String,
      multiclass: Boolean,
      numFeatures: Int): RDD[LabeledPoint] =
    loadLibSVMFile(sc, path, numFeatures)

  @Since("1.0.0")
  @deprecated("use method without multiclass argument, which no longer has effect", "1.1.0")
  def loadLibSVMFile(
      sc: SparkContext,
      path: String,
      multiclass: Boolean): RDD[LabeledPoint] =
    loadLibSVMFile(sc, path)

  /**
   * Loads binary labeled data in the LIBSVM format into an RDD[LabeledPoint], with number of
   * features determined automatically and the default number of partitions.
   */
  @Since("1.0.0")
  def loadLibSVMFile(sc: SparkContext, path: String): RDD[LabeledPoint] =
    loadLibSVMFile(sc, path, -1)

  /**
   * Save labeled data in LIBSVM format.
   * @param data an RDD of LabeledPoint to be saved
   * @param dir directory to save the data 存储的磁盘
   *
   * @see [[org.apache.spark.mllib.util.MLUtils#loadLibSVMFile]]
    *  将标签格式转换成文本格式,存储到磁盘上
   */
  @Since("1.0.0")
  def saveAsLibSVMFile(data: RDD[LabeledPoint], dir: String) {
    // TODO: allow to specify label precision and feature precision.
    val dataStr = data.map { case LabeledPoint(label, features) =>
      val sb = new StringBuilder(label.toString)
      features.foreachActive { case (i, v) =>
        sb += ' '
        sb ++= s"${i + 1}:$v"
      }
      sb.mkString
    }
    dataStr.saveAsTextFile(dir)
  }

  /**
   * Loads vectors saved using `RDD[Vector].saveAsTextFile`.
   * @param sc Spark context
   * @param path file or directory path in any Hadoop-supported file system URI
   * @param minPartitions min number of partitions
   * @return vectors stored as an RDD[Vector]
    * path路径存储的是一个向量数据
   */
  @Since("1.1.0")
  def loadVectors(sc: SparkContext, path: String, minPartitions: Int): RDD[Vector] =
    sc.textFile(path, minPartitions).map(Vectors.parse)

  /**
   * Loads vectors saved using `RDD[Vector].saveAsTextFile` with the default number of partitions.
   */
  @Since("1.1.0")
  def loadVectors(sc: SparkContext, path: String): RDD[Vector] =
    sc.textFile(path, sc.defaultMinPartitions).map(Vectors.parse)

  /**
   * Loads labeled points saved using `RDD[LabeledPoint].saveAsTextFile`.
   * @param sc Spark context
   * @param path file or directory path in any Hadoop-supported file system URI
   * @param minPartitions min number of partitions
   * @return labeled points stored as an RDD[LabeledPoint]
    * 格式:lable,value1 value2 value3
   */
  @Since("1.1.0")
  def loadLabeledPoints(sc: SparkContext, path: String, minPartitions: Int): RDD[LabeledPoint] =
    sc.textFile(path, minPartitions).map(LabeledPoint.parse)

  /**
   * Loads labeled points saved using `RDD[LabeledPoint].saveAsTextFile` with the default number of
   * partitions.
   */
  @Since("1.1.0")
  def loadLabeledPoints(sc: SparkContext, dir: String): RDD[LabeledPoint] =
    loadLabeledPoints(sc, dir, sc.defaultMinPartitions)

  /**
   * Load labeled data from a file. The data format used here is
   * L, f1 f2 ...
   * where f1, f2 are feature values in Double and L is the corresponding label as Double.
   *
   * @param sc SparkContext
   * @param dir Directory to the input data files.
   * @return An RDD of LabeledPoint. Each labeled point has two elements: the first element is
   *         the label, and the second element represents the feature values (an array of Double).
   *
   * @deprecated Should use [[org.apache.spark.rdd.RDD#saveAsTextFile]] for saving and
   *            [[org.apache.spark.mllib.util.MLUtils#loadLabeledPoints]] for loading.
    * 格式:lable,value1 value2 value3
   */
  @Since("1.0.0")
  @deprecated("Should use MLUtils.loadLabeledPoints instead.", "1.0.1")
  def loadLabeledData(sc: SparkContext, dir: String): RDD[LabeledPoint] = {
    sc.textFile(dir).map { line =>
      val parts = line.split(',')
      val label = parts(0).toDouble
      val features = Vectors.dense(parts(1).trim().split(' ').map(_.toDouble))
      LabeledPoint(label, features)
    }
  }

  /**
   * Save labeled data to a file. The data format used here is
   * L, f1 f2 ...
   * where f1, f2 are feature values in Double and L is the corresponding label as Double.
   *
   * @param data An RDD of LabeledPoints containing data to be saved.
   * @param dir Directory to save the data.
   *
   * @deprecated Should use [[org.apache.spark.rdd.RDD#saveAsTextFile]] for saving and
   *            [[org.apache.spark.mllib.util.MLUtils#loadLabeledPoints]] for loading.
   */
  @Since("1.0.0")
  @deprecated("Should use RDD[LabeledPoint].saveAsTextFile instead.", "1.0.1")
  def saveLabeledData(data: RDD[LabeledPoint], dir: String) {
    val dataStr = data.map(x => x.label + "," + x.features.toArray.mkString(" "))
    dataStr.saveAsTextFile(dir)
  }

  /**
   * :: Experimental ::
   * Return a k element array of pairs of RDDs with the first element of each pair
   * containing the training data, a complement of the validation data and the second
   * element, the validation data, containing a unique 1/kth of the data. Where k=numFolds.
   */
  @Since("1.0.0")
  @Experimental
  def kFold[T: ClassTag](rdd: RDD[T], numFolds: Int, seed: Int): Array[(RDD[T], RDD[T])] = {
    val numFoldsF = numFolds.toFloat
    (1 to numFolds).map { fold =>
      val sampler = new BernoulliCellSampler[T]((fold - 1) / numFoldsF, fold / numFoldsF,
        complement = false)
      val validation = new PartitionwiseSampledRDD(rdd, sampler, true, seed)
      val training = new PartitionwiseSampledRDD(rdd, sampler.cloneComplement(), true, seed)
      (training, validation)
    }.toArray
  }

  /**
   * Returns a new vector with `1.0` (bias) appended to the input vector.
    * 向参数向量的最后一个位置追加一个向量值,固定值为1,返回新的向量
   */
  @Since("1.0.0")
  def appendBias(vector: Vector): Vector = {
    vector match {
      case dv: DenseVector =>
        val inputValues = dv.values
        val inputLength = inputValues.length
        val outputValues = Array.ofDim[Double](inputLength + 1)
        System.arraycopy(inputValues, 0, outputValues, 0, inputLength)
        outputValues(inputLength) = 1.0
        Vectors.dense(outputValues)
      case sv: SparseVector =>
        val inputValues = sv.values
        val inputIndices = sv.indices
        val inputValuesLength = inputValues.length
        val dim = sv.size
        val outputValues = Array.ofDim[Double](inputValuesLength + 1)
        val outputIndices = Array.ofDim[Int](inputValuesLength + 1)
        System.arraycopy(inputValues, 0, outputValues, 0, inputValuesLength)
        System.arraycopy(inputIndices, 0, outputIndices, 0, inputValuesLength)
        outputValues(inputValuesLength) = 1.0
        outputIndices(inputValuesLength) = dim
        Vectors.sparse(dim + 1, outputIndices, outputValues)
      case _ => throw new IllegalArgumentException(s"Do not support vector type ${vector.getClass}")
    }
  }

  /**
   * Returns the squared Euclidean distance between two vectors. The following formula will be used
   * if it does not introduce too much numerical error:
   * <pre>
   *   \|a - b\|_2^2 = \|a\|_2^2 + \|b\|_2^2 - 2 a^T b.
   * </pre>
   * When both vector norms are given, this is faster than computing the squared distance directly,
   * especially when one of the vectors is a sparse vector.
   *
   * @param v1 the first vector
   * @param norm1 the norm of the first vector, non-negative
   * @param v2 the second vector
   * @param norm2 the norm of the second vector, non-negative
   * @param precision desired relative precision for the squared distance
   * @return squared distance between v1 and v2 within the specified precision
   */
  private[mllib] def fastSquaredDistance(
      v1: Vector,
      norm1: Double,
      v2: Vector,
      norm2: Double,
      precision: Double = 1e-6): Double = {
    val n = v1.size
    require(v2.size == n)
    require(norm1 >= 0.0 && norm2 >= 0.0)
    val sumSquaredNorm = norm1 * norm1 + norm2 * norm2
    val normDiff = norm1 - norm2
    var sqDist = 0.0
    /*
     * The relative error is
     * <pre>
     * EPSILON * ( \|a\|_2^2 + \|b\\_2^2 + 2 |a^T b|) / ( \|a - b\|_2^2 ),
     * </pre>
     * which is bounded by
     * <pre>
     * 2.0 * EPSILON * ( \|a\|_2^2 + \|b\|_2^2 ) / ( (\|a\|_2 - \|b\|_2)^2 ).
     * </pre>
     * The bound doesn't need the inner product, so we can use it as a sufficient condition to
     * check quickly whether the inner product approach is accurate.
     */
    val precisionBound1 = 2.0 * EPSILON * sumSquaredNorm / (normDiff * normDiff + EPSILON)
    if (precisionBound1 < precision) {
      sqDist = sumSquaredNorm - 2.0 * dot(v1, v2)
    } else if (v1.isInstanceOf[SparseVector] || v2.isInstanceOf[SparseVector]) {
      val dotValue = dot(v1, v2)
      sqDist = math.max(sumSquaredNorm - 2.0 * dotValue, 0.0)
      val precisionBound2 = EPSILON * (sumSquaredNorm + 2.0 * math.abs(dotValue)) /
        (sqDist + EPSILON)
      if (precisionBound2 > precision) {
        sqDist = Vectors.sqdist(v1, v2)
      }
    } else {
      sqDist = Vectors.sqdist(v1, v2)
    }
    sqDist
  }

  /**
   * When `x` is positive and large, computing `math.log(1 + math.exp(x))` will lead to arithmetic
   * overflow. This will happen when `x > 709.78` which is not a very large number.
   * It can be addressed by rewriting the formula into `x + math.log1p(math.exp(-x))` when `x > 0`.
   *
   * @param x a floating-point value as input.
   * @return the result of `math.log(1 + math.exp(x))`.
   */
  private[spark] def log1pExp(x: Double): Double = {
    if (x > 0) {
      x + math.log1p(math.exp(-x))
    } else {
      math.log1p(math.exp(x))
    }
  }
}
