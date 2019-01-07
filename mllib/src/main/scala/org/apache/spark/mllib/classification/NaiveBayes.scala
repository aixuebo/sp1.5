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

package org.apache.spark.mllib.classification

import java.lang.{Iterable => JIterable}

import scala.collection.JavaConverters._

import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.{Logging, SparkContext, SparkException}
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.{BLAS, DenseMatrix, DenseVector, SparseVector, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Model for Naive Bayes Classifiers.
 *
 * @param labels list of labels
 * @param pi log of class priors, whose dimension is C, number of labels
 * @param theta log of class conditional probabilities, whose dimension is C-by-D,
 *              where D is number of features
 * @param modelType The type of NB model to fit  can be "multinomial" or "bernoulli"
 */
@Since("0.9.0")
class NaiveBayesModel private[spark] (
    @Since("1.0.0") val labels: Array[Double],//标签集合
    @Since("0.9.0") val pi: Array[Double],//类的先验概率P(Y)的对数值
    @Since("0.9.0") val theta: Array[Array[Double]],//条件概率P(X|Y)的对数值
    @Since("1.4.0") val modelType: String) //多项式 还是 伯努利
  extends ClassificationModel with Serializable with Saveable {

  import NaiveBayes.{Bernoulli, Multinomial, supportedModelTypes}

  //先验概率
  private val piVector = new DenseVector(pi)
  //矩阵
  private val thetaMatrix = new DenseMatrix(labels.length, theta(0).length, theta.flatten, true)

  private[mllib] def this(labels: Array[Double], pi: Array[Double], theta: Array[Array[Double]]) =
    this(labels, pi, theta, NaiveBayes.Multinomial)

  /** A Java-friendly constructor that takes three Iterable parameters. */
  private[mllib] def this(
      labels: JIterable[Double],
      pi: JIterable[Double],
      theta: JIterable[JIterable[Double]]) =
    this(labels.asScala.toArray, pi.asScala.toArray, theta.asScala.toArray.map(_.asScala.toArray))

  require(supportedModelTypes.contains(modelType),
    s"Invalid modelType $modelType. Supported modelTypes are $supportedModelTypes.")

  // Bernoulli scoring requires log(condprob) if 1, log(1-condprob) if 0.
  // This precomputes log(1.0 - exp(theta)) and its sum which are used for the linear algebra
  // application of this condition (in predict function).
  private val (thetaMinusNegTheta, negThetaSum) = modelType match {
    case Multinomial => (None, None)
    case Bernoulli =>
      val negTheta = thetaMatrix.map(value => math.log(1.0 - math.exp(value)))
      val ones = new DenseVector(Array.fill(thetaMatrix.numCols){1.0})
      val thetaMinusNegTheta = thetaMatrix.map { value =>
        value - math.log(1.0 - math.exp(value))
      }
      (Option(thetaMinusNegTheta), Option(negTheta.multiply(ones)))
    case _ =>
      // This should never happen.
      throw new UnknownError(s"Invalid modelType: $modelType.")
  }

  //对参数集合进行预测--返回每一行数据属于哪个label
  @Since("1.0.0")
  override def predict(testData: RDD[Vector]): RDD[Double] = {
    val bcModel = testData.context.broadcast(this) //广播该模型
    testData.mapPartitions { iter =>
      val model = bcModel.value //在节点还原模型
      iter.map(model.predict) //每一个数据进行预测,返回double 属于哪个label
    }
  }

  //模型对一个向量进行预测,返回该向量对应的label标签
  @Since("1.0.0")
  override def predict(testData: Vector): Double = {
    modelType match {
      case Multinomial =>
        labels(multinomialCalculation(testData).argmax) //argmax最大向量的位置---返回该位置对应的标签
      case Bernoulli =>
        labels(bernoulliCalculation(testData).argmax)
    }
  }

  /**
   * Predict values for the given data set using the model trained.
   *
   * @param testData RDD representing data points to be predicted
   * @return an RDD[Vector] where each entry contains the predicted posterior class probabilities,
   *         in the same order as class labels
    *         预测一个单一向量对应每一个标签的概率，返回值是所有label对应的概率向量
   */
  @Since("1.5.0")
  def predictProbabilities(testData: RDD[Vector]): RDD[Vector] = {
    val bcModel = testData.context.broadcast(this) //广播模型
    testData.mapPartitions { iter =>
      val model = bcModel.value //节点还原模型
      iter.map(model.predictProbabilities)
    }
  }

  /**
   * Predict posterior class probabilities for a single data point using the model trained.
   * 使用模型,对单一向量数据,预测他的后验概率
   * @param testData array representing a single data point 单一向量数据
   * @return predicted posterior class probabilities from the trained model,
   *         in the same order as class labels 返回预测后验概率值对应的向量
   */
  @Since("1.5.0")
  def predictProbabilities(testData: Vector): Vector = {
    modelType match {
      case Multinomial =>
        posteriorProbabilities(multinomialCalculation(testData))
      case Bernoulli =>
        posteriorProbabilities(bernoulliCalculation(testData))
    }
  }

  //返回值是一个向量，每一个label对应向量的一个概率值
  /**
    *
  30个label,50个维度
30 * 50   50*1 = 30 * 1
每一行与参数列向量乘积，就是30个label对应的一个值，即每一个label的概率 = 该label所在的行 * 参数 之和。
行的内容相当于每一个维度对应的权重，因此计算就相当于计算该向量在每一个label下的加权和
    */
  private def multinomialCalculation(testData: Vector) = {
    val prob = thetaMatrix.multiply(testData) //多少行,说明多少个lable,返回一个所有label组成的向量
    BLAS.axpy(1.0, piVector, prob) //prob += a * piVector  //因为是log操作，所以*变成了+
    prob
  }

  //返回值是一个向量，每一个label对应向量的一个概率值
  private def bernoulliCalculation(testData: Vector) = {
    //校验参数必须是0或者1
    testData.foreachActive((_, value) =>
      if (value != 0.0 && value != 1.0) {
        throw new SparkException(
          s"Bernoulli naive Bayes requires 0 or 1 feature values but found $testData.")
      }
    )
    val prob = thetaMinusNegTheta.get.multiply(testData)
    BLAS.axpy(1.0, piVector, prob)
    BLAS.axpy(1.0, negThetaSum.get, prob)
    prob
  }

  //计算概率
  private def posteriorProbabilities(logProb: DenseVector) = {
    val logProbArray = logProb.toArray //label分数集合
    val maxLog = logProbArray.max //最大的值
    val scaledProbs = logProbArray.map(lp => math.exp(lp - maxLog)) //还原,因为原始内容是log后的,所以需要还原
    val probSum = scaledProbs.sum //求和
    new DenseVector(scaledProbs.map(_ / probSum)) //每一个还原后的值 在总体中占比
  }

  @Since("1.3.0")
  override def save(sc: SparkContext, path: String): Unit = {
    val data = NaiveBayesModel.SaveLoadV2_0.Data(labels, pi, theta, modelType)
    NaiveBayesModel.SaveLoadV2_0.save(sc, path, data)
  }

  override protected def formatVersion: String = "2.0"
}

//模型的序列化与反序列化到磁盘
@Since("1.3.0")
object NaiveBayesModel extends Loader[NaiveBayesModel] {

  import org.apache.spark.mllib.util.Loader._

  private[mllib] object SaveLoadV2_0 {

    def thisFormatVersion: String = "2.0"

    /** Hard-code class name string in case it changes in the future */
    def thisClassName: String = "org.apache.spark.mllib.classification.NaiveBayesModel"

    /** Model data for model import/export */
    case class Data(
        labels: Array[Double],
        pi: Array[Double],
        theta: Array[Array[Double]],
        modelType: String)

    def save(sc: SparkContext, path: String, data: Data): Unit = {
      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      // Create JSON metadata.
      val metadata = compact(render(
        ("class" -> thisClassName) ~ ("version" -> thisFormatVersion) ~
          ("numFeatures" -> data.theta(0).length) ~ ("numClasses" -> data.pi.length)))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(metadataPath(path))

      // Create Parquet data.
      val dataRDD: DataFrame = sc.parallelize(Seq(data), 1).toDF()
      dataRDD.write.parquet(dataPath(path))
    }

    @Since("1.3.0")
    def load(sc: SparkContext, path: String): NaiveBayesModel = {
      val sqlContext = new SQLContext(sc)
      // Load Parquet data.
      val dataRDD = sqlContext.read.parquet(dataPath(path))
      // Check schema explicitly since erasure makes it hard to use match-case for checking.
      checkSchema[Data](dataRDD.schema)
      val dataArray = dataRDD.select("labels", "pi", "theta", "modelType").take(1)
      assert(dataArray.length == 1, s"Unable to load NaiveBayesModel data from: ${dataPath(path)}")
      val data = dataArray(0)
      val labels = data.getAs[Seq[Double]](0).toArray
      val pi = data.getAs[Seq[Double]](1).toArray
      val theta = data.getAs[Seq[Seq[Double]]](2).map(_.toArray).toArray
      val modelType = data.getString(3)
      new NaiveBayesModel(labels, pi, theta, modelType)
    }

  }

  private[mllib] object SaveLoadV1_0 {

    def thisFormatVersion: String = "1.0"

    /** Hard-code class name string in case it changes in the future */
    def thisClassName: String = "org.apache.spark.mllib.classification.NaiveBayesModel"

    /** Model data for model import/export */
    case class Data(
        labels: Array[Double],
        pi: Array[Double],
        theta: Array[Array[Double]])

    def save(sc: SparkContext, path: String, data: Data): Unit = {
      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      // Create JSON metadata.
      val metadata = compact(render(
        ("class" -> thisClassName) ~ ("version" -> thisFormatVersion) ~
          ("numFeatures" -> data.theta(0).length) ~ ("numClasses" -> data.pi.length)))//numFeatures 特征数量  ,numClasses label数量
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(metadataPath(path))

      // Create Parquet data.
      val dataRDD: DataFrame = sc.parallelize(Seq(data), 1).toDF()
      dataRDD.write.parquet(dataPath(path))
    }

    def load(sc: SparkContext, path: String): NaiveBayesModel = {
      val sqlContext = new SQLContext(sc)
      // Load Parquet data.
      val dataRDD = sqlContext.read.parquet(dataPath(path))
      // Check schema explicitly since erasure makes it hard to use match-case for checking.
      checkSchema[Data](dataRDD.schema)
      val dataArray = dataRDD.select("labels", "pi", "theta").take(1)
      assert(dataArray.length == 1, s"Unable to load NaiveBayesModel data from: ${dataPath(path)}")
      val data = dataArray(0)
      val labels = data.getAs[Seq[Double]](0).toArray
      val pi = data.getAs[Seq[Double]](1).toArray
      val theta = data.getAs[Seq[Seq[Double]]](2).map(_.toArray).toArray
      new NaiveBayesModel(labels, pi, theta)
    }
  }

  override def load(sc: SparkContext, path: String): NaiveBayesModel = {
    val (loadedClassName, version, metadata) = loadMetadata(sc, path)
    val classNameV1_0 = SaveLoadV1_0.thisClassName
    val classNameV2_0 = SaveLoadV2_0.thisClassName
    val (model, numFeatures, numClasses) = (loadedClassName, version) match {
      case (className, "1.0") if className == classNameV1_0 =>
        val (numFeatures, numClasses) = ClassificationModel.getNumFeaturesClasses(metadata)
        val model = SaveLoadV1_0.load(sc, path)
        (model, numFeatures, numClasses)
      case (className, "2.0") if className == classNameV2_0 =>
        val (numFeatures, numClasses) = ClassificationModel.getNumFeaturesClasses(metadata)
        val model = SaveLoadV2_0.load(sc, path)
        (model, numFeatures, numClasses)
      case _ => throw new Exception(
        s"NaiveBayesModel.load did not recognize model with (className, format version):" +
        s"($loadedClassName, $version).  Supported:\n" +
        s"  ($classNameV1_0, 1.0)")
    }
    assert(model.pi.length == numClasses,
      s"NaiveBayesModel.load expected $numClasses classes," +
        s" but class priors vector pi had ${model.pi.length} elements")
    assert(model.theta.length == numClasses,
      s"NaiveBayesModel.load expected $numClasses classes," +
        s" but class conditionals array theta had ${model.theta.length} elements")
    assert(model.theta.forall(_.length == numFeatures),
      s"NaiveBayesModel.load expected $numFeatures features," +
        s" but class conditionals array theta had elements of size:" +
        s" ${model.theta.map(_.length).mkString(",")}")
    model
  }
}

/**
 * Trains a Naive Bayes model given an RDD of `(label, features)` pairs.
 *
 * This is the Multinomial NB ([[http://tinyurl.com/lsdw6p]]) which can handle all kinds of
 * discrete data.  For example, by converting documents into TF-IDF vectors, it can be used for
 * document classification.  By making every vector a 0-1 vector, it can also be used as
 * Bernoulli NB ([[http://tinyurl.com/p7c96j6]]). The input feature values must be nonnegative.
 */
@Since("0.9.0")
class NaiveBayes private (
    private var lambda: Double,//平滑因子 默认是1
    private var modelType: String) //多项式还是伯努利 贝叶斯方式类型
   extends Serializable with Logging {

  import NaiveBayes.{Bernoulli, Multinomial}

  @Since("1.4.0")
  def this(lambda: Double) = this(lambda, NaiveBayes.Multinomial)

  //平滑因子默认是1.默认是多项式贝叶斯
  @Since("0.9.0")
  def this() = this(1.0, NaiveBayes.Multinomial)

  /** Set the smoothing parameter. Default: 1.0. */
  @Since("0.9.0")
  def setLambda(lambda: Double): NaiveBayes = {
    this.lambda = lambda
    this
  }

  /** Get the smoothing parameter. */
  @Since("1.4.0")
  def getLambda: Double = lambda

  /**
   * Set the model type using a string (case-sensitive).
   * Supported options: "multinomial" (default) and "bernoulli".
   * 设置贝叶斯类型
   */
  @Since("1.4.0")
  def setModelType(modelType: String): NaiveBayes = {
    require(NaiveBayes.supportedModelTypes.contains(modelType),
      s"NaiveBayes was created with an unknown modelType: $modelType.")
    this.modelType = modelType
    this
  }

  /** Get the model type. */
  @Since("1.4.0")
  def getModelType: String = this.modelType

  /**
   * Run the algorithm with the configured parameters on an input RDD of LabeledPoint entries.
   * 运行贝叶斯算法去训练模型
   * @param data RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   */
  @Since("0.9.0")
  def run(data: RDD[LabeledPoint]): NaiveBayesModel = {
      
    //定义方法 多项式---校验值必须不是负数
    val requireNonnegativeValues: Vector => Unit = (v: Vector) => {
      val values = v match {
        case sv: SparseVector => sv.values
        case dv: DenseVector => dv.values
      }
      if (!values.forall(_ >= 0.0)) {
        throw new SparkException(s"Naive Bayes requires nonnegative feature values but found $v.")
      }
    }

    //定义方法 伯努利---校验值只能是0和1
    val requireZeroOneBernoulliValues: Vector => Unit = (v: Vector) => {
      val values = v match {
        case sv: SparseVector => sv.values
        case dv: DenseVector => dv.values
      }
      if (!values.forall(v => v == 0.0 || v == 1.0)) {
        throw new SparkException(
          s"Bernoulli naive Bayes requires 0 or 1 feature values but found $v.")
      }
    }

    // Aggregates term frequencies per label.聚合label词频,即相同的向量出现次数
    // TODO: Calling combineByKey and collect creates two stages, we can implement something
    // TODO: similar to reduceByKeyLocally to save one stage.
    //返回值是label,(Long, DenseVector)组成的集合,即label 出现了都少次，同一个label对应的向量和
    val aggregated = data.map(p => (p.label, p.features)).combineByKey[(Long, DenseVector)](//表示将V(向量) 转换成(Long, DenseVector)作为中间结果存储
      createCombiner = (v: Vector) => {
        //先数据校验
        if (modelType == Bernoulli) {
          requireZeroOneBernoulliValues(v)
        } else {
          requireNonnegativeValues(v)
        }
        (1L, v.copy.toDense) //默认出现1次词频---即向量本身出现一次
      },
      mergeValue = (c: (Long, DenseVector), v: Vector) => {
        requireNonnegativeValues(v)
        BLAS.axpy(1.0, v, c._2)//c2 = c2 + v*1
        (c._1 + 1L, c._2) //累加计数器1
      },
      mergeCombiners = (c1: (Long, DenseVector), c2: (Long, DenseVector)) => {
        BLAS.axpy(1.0, c2._2, c1._2)
        (c1._1 + c2._1, c1._2) //全局性进行累加计数器
      }
    ).collect()//此时会在一个节点汇总，可能会浪费性能，但是由于label本身分类不多，因此一个节点汇总也是可以的
    .sortBy(_._1) //按照label排序

    val numLabels = aggregated.length //有多少个label
    var numDocuments = 0L //有多少条总数据
    aggregated.foreach { case (_, (n, _)) =>
      numDocuments += n
    }
    val numFeatures = aggregated.head match { case (_, (_, v)) => v.size } //获取特征向量维数

    val labels = new Array[Double](numLabels) //所有出现的标签集合
    val pi = new Array[Double](numLabels) //类的先验概率P(Y)的对数值,即label出现的次数/总次数
    //我觉得有一个前提，是向量都要进行归一化处理，否则某一个维度的值太大，就有问题了
    val theta = Array.fill(numLabels)(new Array[Double](numFeatures))//条件概率P(X|Y)的对数值---该维度对应的lable所有数据和/该label对应的所有数据和---表示该维度在label下的权重占比

    val piLogDenom = math.log(numDocuments + numLabels * lambda)
    var i = 0
    aggregated.foreach { case (label, (n, sumTermFreqs)) =>
      labels(i) = label
      pi(i) = math.log(n + lambda) - piLogDenom
      val thetaLogDenom = modelType match {
        case Multinomial => math.log(sumTermFreqs.values.sum + numFeatures * lambda) //特征维度,因此用特征数量*lambda
        case Bernoulli => math.log(n + 2.0 * lambda)
        case _ =>
          // This should never happen.
          throw new UnknownError(s"Invalid modelType: $modelType.")
      }
      var j = 0
      while (j < numFeatures) {
        theta(i)(j) = math.log(sumTermFreqs(j) + lambda) - thetaLogDenom
        j += 1
      }
      i += 1
    }

    new NaiveBayesModel(labels, pi, theta, modelType)
  }
}

/**
 * Top-level methods for calling naive Bayes.
 */
@Since("0.9.0")
object NaiveBayes {

  /** String name for multinomial model type. 多项式贝叶斯*/
  private[spark] val Multinomial: String = "multinomial"

  /** String name for Bernoulli model type. 伯努利贝叶斯*/
  private[spark] val Bernoulli: String = "bernoulli"

  /* Set of modelTypes that NaiveBayes supports */
  private[spark] val supportedModelTypes = Set(Multinomial, Bernoulli)

  /**
   * Trains a Naive Bayes model given an RDD of `(label, features)` pairs.
   *
   * This is the default Multinomial NB ([[http://tinyurl.com/lsdw6p]]) which can handle all
   * kinds of discrete data.  For example, by converting documents into TF-IDF vectors, it
   * can be used for document classification.
   *
   * This version of the method uses a default smoothing parameter of 1.0.
   *
   * @param input RDD of `(label, array of features)` pairs.  Every vector should be a frequency
   *              vector or a count vector.
   *  输入是label标签和特征向量作为的一行数据。组成的RDD
   */
  @Since("0.9.0")
  def train(input: RDD[LabeledPoint]): NaiveBayesModel = {
    new NaiveBayes().run(input)
  }

  /**
   * Trains a Naive Bayes model given an RDD of `(label, features)` pairs.
   *
   * This is the default Multinomial NB ([[http://tinyurl.com/lsdw6p]]) which can handle all
   * kinds of discrete data.  For example, by converting documents into TF-IDF vectors, it
   * can be used for document classification.
   *
   * @param input RDD of `(label, array of features)` pairs.  Every vector should be a frequency
   *              vector or a count vector.
   * @param lambda The smoothing parameter 平滑因子
   */
  @Since("0.9.0")
  def train(input: RDD[LabeledPoint], lambda: Double): NaiveBayesModel = {
    new NaiveBayes(lambda, Multinomial).run(input)
  }

  /**
   * Trains a Naive Bayes model given an RDD of `(label, features)` pairs.
   *
   * The model type can be set to either Multinomial NB ([[http://tinyurl.com/lsdw6p]])
   * or Bernoulli NB ([[http://tinyurl.com/p7c96j6]]). The Multinomial NB can handle
   * discrete count data and can be called by setting the model type to "multinomial".
   * For example, it can be used with word counts or TF_IDF vectors of documents.
   * The Bernoulli model fits presence or absence (0-1) counts. By making every vector a
   * 0-1 vector and setting the model type to "bernoulli", the  fits and predicts as
   * Bernoulli NB.
   *
   * @param input RDD of `(label, array of features)` pairs.  Every vector should be a frequency
   *              vector or a count vector.
   * @param lambda The smoothing parameter
   *
   * @param modelType The type of NB model to fit from the enumeration NaiveBayesModels, can be
   *              multinomial or bernoulli
   */
  @Since("1.4.0")
  def train(input: RDD[LabeledPoint], lambda: Double, modelType: String): NaiveBayesModel = {
    require(supportedModelTypes.contains(modelType),
      s"NaiveBayes was created with an unknown modelType: $modelType.")
    new NaiveBayes(lambda, modelType).run(input)
  }

}
