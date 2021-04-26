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

package org.apache.spark.mllib.feature

import java.lang.{Iterable => JavaIterable}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuilder

import com.github.fommil.netlib.BLAS.{getInstance => blas}

import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.Logging
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.{Vector, Vectors, DenseMatrix, BLAS, DenseVector}
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd._
import org.apache.spark.util.Utils
import org.apache.spark.util.random.XORShiftRandom
import org.apache.spark.sql.{SQLContext, Row}

/**
 *  Entry in vocabulary 词汇实体
 */
private case class VocabWord(
  var word: String,//分词
  var cn: Int,//词频--词出现次数
  var point: Array[Int],//存储路径，即经过得结点
  var code: Array[Int],//记录Huffman编码  根节点到叶子节点的huffman编码
  var codeLen: Int //存储到达该叶子结点，要经过多少个结点
)

/**
 * :: Experimental ::
 * Word2Vec creates vector representation of words in a text corpus.
 * The algorithm first constructs a vocabulary from the corpus
 * and then learns vector representation of words in the vocabulary.
 * The vector representation can be used as features in
 * natural language processing and machine learning algorithms.
 *
 * We used skip-gram model in our implementation and hierarchical softmax
 * method to train the model. The variable names in the implementation
 * matches the original C implementation.
 *
 * For original C implementation, see https://code.google.com/p/word2vec/
 * For research papers, see
 * Efficient Estimation of Word Representations in Vector Space
 * and
 * Distributed Representations of Words and Phrases and their Compositionality.
  * 词向量，将每一个词转换成向量，存储起来。
  * ont-hot形式是将每一个词转换成维度很大的向量，但非常稀松。
  * Word2Vec是将每一个词转换成固定长度的向量，向量不稀松
 */
@Since("1.1.0")
@Experimental
class Word2Vec extends Serializable with Logging {

  private var vectorSize = 100 //每一个词多少维度
  private var learningRate = 0.025
  private var numPartitions = 1 //分区并行数量
  private var numIterations = 1
  private var seed = Utils.random.nextLong()
  private var minCount = 5 //至少词出现5次以上,才进入到算法,如果少于该字符,说明出现次数过少,应该删除,避免占用字典

  /**
   * Sets vector size (default: 100).
   */
  @Since("1.1.0")
  def setVectorSize(vectorSize: Int): this.type = {
    this.vectorSize = vectorSize
    this
  }

  /**
   * Sets initial learning rate (default: 0.025).
   */
  @Since("1.1.0")
  def setLearningRate(learningRate: Double): this.type = {
    this.learningRate = learningRate
    this
  }

  /**
   * Sets number of partitions (default: 1). Use a small number for accuracy.
   */
  @Since("1.1.0")
  def setNumPartitions(numPartitions: Int): this.type = {
    require(numPartitions > 0, s"numPartitions must be greater than 0 but got $numPartitions")
    this.numPartitions = numPartitions
    this
  }

  /**
   * Sets number of iterations (default: 1), which should be smaller than or equal to number of
   * partitions.
   */
  @Since("1.1.0")
  def setNumIterations(numIterations: Int): this.type = {
    this.numIterations = numIterations
    this
  }

  /**
   * Sets random seed (default: a random long integer).
   */
  @Since("1.1.0")
  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }

  /**
   * Sets minCount, the minimum number of times a token must appear to be included in the word2vec
   * model's vocabulary (default: 5).
   */
  @Since("1.3.0")
  def setMinCount(minCount: Int): this.type = {
    this.minCount = minCount
    this
  }

  private val EXP_TABLE_SIZE = 1000 //计算1000种不同的逻辑回归,缓存起来,方便后期避免重复计算
  private val MAX_EXP = 6 // sigmoid(x)的x定义域理论上是无穷，但因为x在[-6，6)时，y已极度接近(0,1)，故简化计算，用MAX_EXP取近似
  private val MAX_CODE_LENGTH = 40
  private val MAX_SENTENCE_LENGTH = 1000 //一轮获取多少个词

  /** context words from [-window, window] */
  private val window = 5

  private var trainWordsCount = 0//总词频数量
  private var vocabSize = 0//有多少个词
  private var vocab: Array[VocabWord] = null //记录所有词字典,字典是按照词频顺序排序的,出现越多的词越排在前面
  private var vocabHash = mutable.HashMap.empty[String, Int] //记录每一个词在vocab中的下标位置

  /**
    * 初始化操作
    * 1.构建词字典，并且按照出现次数排序
    * 2.构建字典的词与字典序号的关系。
    * 3.计算总词频
    */
  private def learnVocab(words: RDD[String]): Unit = {
    vocab = words.map(w => (w, 1)) //对词统计数字
      .reduceByKey(_ + _) //每一个词统计出现次数
      .map(x => VocabWord( //构造成对象
        x._1,//词本身
        x._2,//出现次数
        new Array[Int](MAX_CODE_LENGTH),
        new Array[Int](MAX_CODE_LENGTH),
        0))
      .filter(_.cn >= minCount) //一定要出现一定次数才算
      .collect() //非常占用内存
      .sortWith((a, b) => a.cn > b.cn) //按照次数排序

    vocabSize = vocab.length
    require(vocabSize > 0, "The vocabulary size should be > 0. You may need to check " +
      "the setting of minCount, which could be large enough to remove all your words in sentences.")

    var a = 0
    while (a < vocabSize) {
      vocabHash += vocab(a).word -> a //词与下标的映射
      trainWordsCount += vocab(a).cn //总词频数量
      a += 1
    }
    logInfo("trainWordsCount = " + trainWordsCount)
  }

  //逻辑回归公式计算
  private def createExpTable(): Array[Float] = {
    val expTable = new Array[Float](EXP_TABLE_SIZE)
    var i = 0
    while (i < EXP_TABLE_SIZE) {
      //将数据分成EXP_TABLE_SIZE分，让其覆盖-1到1之间。因此是2*系数-1，系数控制在0-1之间就可以满足结果是-1到1之间
      val tmp = math.exp((2.0 * i / EXP_TABLE_SIZE - 1.0) * MAX_EXP)
      expTable(i) = (tmp / (tmp + 1.0)).toFloat
      i += 1
    }
    expTable
  }

  //Create Huffman Tree
  //为每一个词产生一个唯一编码,采用哈夫曼树进行编码,原理是更常用的词编码越少.以节省空间---这部分编码不属于word2vec的核心逻辑
  private def createBinaryTree(): Unit = {
    //前N个位置,存储每一个词的词频  后N个词存储1000000000固定值
    val count = new Array[Long](vocabSize * 2 + 1) //前面vocabSize个存储每一个词的出现次数,后面存储连续两个节点权重之和,用于分叉
    val binary = new Array[Int](vocabSize * 2 + 1) //标注节点是左节点还是右节点,1是左节点
    val parentNode = new Array[Int](vocabSize * 2 + 1) //每一个节点的父节点是谁
    val code = new Array[Int](MAX_CODE_LENGTH)
    val point = new Array[Int](MAX_CODE_LENGTH)
    var a = 0
    while (a < vocabSize) { //循环每一词
      count(a) = vocab(a).cn
      a += 1
    }
    while (a < 2 * vocabSize) { //后N个词存储1000000000固定值--始于初始化过程
      count(a) = 1e9.toInt //1后面9个0
      a += 1
    }

    //从最后2个词开始加工数据
    var pos1 = vocabSize - 1
    var pos2 = vocabSize

    var min1i = 0
    var min2i = 0

    a = 0
    while (a < vocabSize - 1) {//获取每一个词
      if (pos1 >= 0) {
        if (count(pos1) < count(pos2)) {//获取2个词的词频
          min1i = pos1
          pos1 -= 1
        } else {
          min1i = pos2
          pos2 += 1
        }
      } else {
        min1i = pos2
        pos2 += 1
      }
      if (pos1 >= 0) {
        if (count(pos1) < count(pos2)) {
          min2i = pos1
          pos1 -= 1
        } else {
          min2i = pos2
          pos2 += 1
        }
      } else {
        min2i = pos2
        pos2 += 1
      }
      count(vocabSize + a) = count(min1i) + count(min2i)
      parentNode(min1i) = vocabSize + a
      parentNode(min2i) = vocabSize + a
      binary(min2i) = 1
      a += 1
    }
    // Now assign binary code to each vocabulary word
    var i = 0
    a = 0 //表示第几个词
    while (a < vocabSize) { //循环每一个词
      var b = a //从叶子节点开始计算
      i = 0 //记录该词有多少步可以到叶子节点
      while (b != vocabSize * 2 - 2) {
        code(i) = binary(b) //记录是左右哪个节点,即0还是1
        point(i) = b //记录路径在全部code里面的走的顺序
        i += 1
        b = parentNode(b)//找到父节点
      }
      vocab(a).codeLen = i
      vocab(a).point(0) = vocabSize - 2
      b = 0 //一层一层走
      while (b < i) {//一共才i层,所以一层一层找
        vocab(a).code(i - b - 1) = code(b)
        vocab(a).point(i - b) = point(b) - vocabSize
        b += 1
      }
      a += 1
    }
  }

  /**
   * Computes the vector representation of each word in vocabulary.
   * @param dataset an RDD of words,RDD的内容是一个迭代器
   * @return a Word2VecModel
    * 传入的参数是一个迭代器,每一个迭代器表示一个完整的句子，需要我们自己拆分。
   */
  @Since("1.1.0")
  def fit[S <: Iterable[String]](dataset: RDD[S]): Word2VecModel = {

    val words = dataset.flatMap(x => x) //参数x本身是一个迭代器

    learnVocab(words) //初始化词字典

    createBinaryTree()

    val sc = dataset.context

    val expTable = sc.broadcast(createExpTable())
    val bcVocab = sc.broadcast(vocab) //字典
    val bcVocabHash = sc.broadcast(vocabHash) //单词与字典顺序的映射

    //返回每一个词在字典的下标
    val sentences: RDD[Array[Int]] = words.mapPartitions { iter => //循环每一个词
      new Iterator[Array[Int]] {
        def hasNext: Boolean = iter.hasNext

        //获取每一个词在字典中的下标
        def next(): Array[Int] = {
          val sentence = ArrayBuilder.make[Int] //创建动态数组,存储每一个词对应的下标
          var sentenceLength = 0 //词数量,sentence数组长度
          while (iter.hasNext && sentenceLength < MAX_SENTENCE_LENGTH) { //每次查询1000个词
            val word = bcVocabHash.value.get(iter.next())//获取该词对应的字典下标
            word match {
              case Some(w) => //存在该词,找到下标
                sentence += w
                sentenceLength += 1
              case None => //不存在该词
            }
          }
          sentence.result()
        }
      }
    }

    val newSentences = sentences.repartition(numPartitions).cache() //numPartitions个任务并发,每次获取1000个词处理
    val initRandom = new XORShiftRandom(seed)

    //确保所有的词与词的向量,能够存储在256M的内存中
    if (vocabSize.toLong * vectorSize * 8 >= Int.MaxValue) { //避免OOM--256M
      throw new RuntimeException("Please increase minCount or decrease vectorSize in Word2Vec" +
        " to avoid an OOM. You are highly recommended to make your vocabSize*vectorSize, " +
        "which is " + vocabSize + "*" + vectorSize + " for now, less than `Int.MaxValue/8`.")
    }

    val syn0Global = Array.fill[Float](vocabSize * vectorSize)((initRandom.nextFloat() - 0.5f) / vectorSize)
    val syn1Global = new Array[Float](vocabSize * vectorSize)//Array对象本身就是实现了序列化接口，因此不需要广播
    var alpha = learningRate
    for (k <- 1 to numIterations) {
      //发生变化的向量集合
      val partial = newSentences.mapPartitionsWithIndex { case (idx, iter) => //并发任务的序号、1000个词--每一个词在词典的位置
        val random = new XORShiftRandom(seed ^ ((idx + 1) << 16) ^ ((-k - 1) << 8))
        val syn0Modify = new Array[Int](vocabSize)
        val syn1Modify = new Array[Int](vocabSize)
        val model = iter.foldLeft((syn0Global, syn1Global, 0, 0)) { //循环一批次词
          //lastWordCount表示上一次处理学习速度前,处理了多少个词,wordCount表示一共处理了多少个词了
          case ((syn0, syn1, lastWordCount, wordCount), sentence) => //sentence记录该批次内,每一个词在字典的序号
            var lwc = lastWordCount
            var wc = wordCount
            if (wordCount - lastWordCount > 10000) {
              lwc = wordCount
              // TODO: discount by iteration?
              alpha =
                learningRate * (1 - numPartitions * wordCount.toDouble / (trainWordsCount + 1))
              if (alpha < learningRate * 0.0001) alpha = learningRate * 0.0001
              logInfo("wordCount = " + wordCount + ", alpha = " + alpha)
            }
            wc += sentence.size //设置处理了多少个词
            var pos = 0
            while (pos < sentence.size) { //循环每一个词的字典位置
              val word = sentence(pos) //某一个词在字典的位置
              val b = random.nextInt(window)
              // Train Skip-gram
              var a = b
              while (a < window * 2 + 1 - b) {
                if (a != window) {
                  val c = pos - window + a
                  if (c >= 0 && c < sentence.size) {
                    val lastWord = sentence(c)
                    val l1 = lastWord * vectorSize
                    val neu1e = new Array[Float](vectorSize)
                    // Hierarchical softmax
                    var d = 0
                    while (d < bcVocab.value(word).codeLen) {//获取该词的codeLen--即循环词的每一个节点
                      val inner = bcVocab.value(word).point(d)
                      val l2 = inner * vectorSize
                      // Propagate hidden -> output
                      var f = blas.sdot(vectorSize, syn0, l1, 1, syn1, l2, 1)
                      if (f > -MAX_EXP && f < MAX_EXP) {
                        val ind = ((f + MAX_EXP) * (EXP_TABLE_SIZE / MAX_EXP / 2.0)).toInt //sigmoid(x)的x定义域理论上是无穷，但因为x在[-6，6)时，y已极度接近(0,1)，故简化计算，用MAX_EXP取近似
                        f = expTable.value(ind)
                        val g = ((1 - bcVocab.value(word).code(d) - f) * alpha).toFloat
                        blas.saxpy(vectorSize, g, syn1, l2, 1, neu1e, 0, 1)
                        blas.saxpy(vectorSize, g, syn0, l1, 1, syn1, l2, 1)
                        syn1Modify(inner) += 1 //向量有变化
                      }
                      d += 1
                    }
                    blas.saxpy(vectorSize, 1.0f, neu1e, 0, 1, syn0, l1, 1)
                    syn0Modify(lastWord) += 1
                  }
                }
                a += 1
              }
              pos += 1
            }
            (syn0, syn1, lwc, wc)
        }
        val syn0Local = model._1
        val syn1Local = model._2
        // Only output modified vectors.仅仅输出修改过的向量
        Iterator.tabulate(vocabSize) { index => //循环每一个单词对应的向量
          if (syn0Modify(index) > 0) { //说明向量有变化
            Some((index, syn0Local.slice(index * vectorSize, (index + 1) * vectorSize))) //返回有变化的向量
          } else {
            None
          }
        }.flatten ++ Iterator.tabulate(vocabSize) { index =>
          if (syn1Modify(index) > 0) {//同样是发生变化的向量
            Some((index + vocabSize, syn1Local.slice(index * vectorSize, (index + 1) * vectorSize)))
          } else {
            None
          }
        }.flatten
      }
      val synAgg = partial.reduceByKey { case (v1, v2) =>
          blas.saxpy(vectorSize, 1.0f, v2, 1, v1, 1)
          v1
      }.collect()
      var i = 0
      while (i < synAgg.length) {
        val index = synAgg(i)._1
        if (index < vocabSize) {
          Array.copy(synAgg(i)._2, 0, syn0Global, index * vectorSize, vectorSize)
        } else {
          Array.copy(synAgg(i)._2, 0, syn1Global, (index - vocabSize) * vectorSize, vectorSize)
        }
        i += 1
      }
    }
    newSentences.unpersist()

    val wordArray = vocab.map(_.word)
    new Word2VecModel(wordArray.zipWithIndex.toMap, syn0Global)
  }

  /**
   * Computes the vector representation of each word in vocabulary (Java version).
   * @param dataset a JavaRDD of words
   * @return a Word2VecModel
   */
  @Since("1.1.0")
  def fit[S <: JavaIterable[String]](dataset: JavaRDD[S]): Word2VecModel = {
    fit(dataset.rdd.map(_.asScala))
  }
}

/**
 * :: Experimental ::
 * Word2Vec model
 * @param wordIndex maps each word to an index, which can retrieve the corresponding
 *                  vector from wordVectors
 * @param wordVectors array of length numWords * vectorSize, vector corresponding
 *                    to the word mapped with index i can be retrieved by the slice
 *                    (i * vectorSize, i * vectorSize + vectorSize)
 */
@Experimental
@Since("1.1.0")
class Word2VecModel private[mllib] (
    private val wordIndex: Map[String, Int],//字典的词与下标的映射
    private val wordVectors: Array[Float]) extends Serializable with Saveable { //字典所有词组成的向量,即数组长度 = 词典长度*每一个词使用多少位字节数组

  private val numWords = wordIndex.size //包含多少个词
  // vectorSize: Dimension of each word's vector.
  private val vectorSize = wordVectors.length / numWords //每一个词使用多少个字节

  // wordList: Ordered list of words obtained from wordIndex.对字典排序,按照词频组成的序号排序
  private val wordList: Array[String] = {
    val (wl, _) = wordIndex.toSeq.sortBy(_._2).unzip
    wl.toArray
  }

  // wordVecNorms: Array of length numWords, each value being the Euclidean norm
  //               of the wordVector.
  //存储每一个词的归一化值
  private val wordVecNorms: Array[Double] = {
    val wordVecNorms = new Array[Double](numWords)
    var i = 0
    while (i < numWords) { //循环每一个词
      val vec = wordVectors.slice(i * vectorSize, i * vectorSize + vectorSize) //获取属于该词的向量
      wordVecNorms(i) = blas.snrm2(vectorSize, vec, 1) //对词向量进行归一化
      i += 1
    }
    wordVecNorms
  }

  //参数存储每一个词与该词向量本身
  @Since("1.5.0")
  def this(model: Map[String, Array[Float]]) = {
    this(Word2VecModel.buildWordIndex(model), Word2VecModel.buildWordVectors(model))
  }

  //向量词之间的相似性
  private def cosineSimilarity(v1: Array[Float], v2: Array[Float]): Double = {
    require(v1.length == v2.length, "Vectors should have the same length")
    val n = v1.length
    val norm1 = blas.snrm2(n, v1, 1) //归一化
    val norm2 = blas.snrm2(n, v2, 1)
    if (norm1 == 0 || norm2 == 0) return 0.0
    blas.sdot(n, v1, 1, v2, 1) / norm1 / norm2
  }

  override protected def formatVersion = "1.0"

  @Since("1.4.0")
  def save(sc: SparkContext, path: String): Unit = {
    Word2VecModel.SaveLoadV1_0.save(sc, path, getVectors)
  }

  /**
   * Transforms a word to its vector representation
   * @param word a word
   * @return vector representation of word
    * 将一个词转换成向量
   */
  @Since("1.1.0")
  def transform(word: String): Vector = {
    wordIndex.get(word) match { //获取词的下标
      case Some(ind) =>
        val vec = wordVectors.slice(ind * vectorSize, ind * vectorSize + vectorSize) //获取到该词的向量
        Vectors.dense(vec.map(_.toDouble))
      case None =>
        throw new IllegalStateException(s"$word not in vocabulary") //没有找到该词
    }
  }

  /**
   * Find synonyms of a word
   * @param word a word
   * @param num number of synonyms to find 找多少个同义词
   * @return array of (word, cosineSimilarity)
    * 找同义词
   */
  @Since("1.1.0")
  def findSynonyms(word: String, num: Int): Array[(String, Double)] = {
    val vector = transform(word) //先找到词向量
    findSynonyms(vector, num)
  }

  /**
   * Find synonyms of the vector representation of a word
   * @param vector vector representation of a word
   * @param num number of synonyms to find
   * @return array of (word, cosineSimilarity)
    * 找同义词
   */
  @Since("1.1.0")
  def findSynonyms(vector: Vector, num: Int): Array[(String, Double)] = {
    require(num > 0, "Number of similar words should > 0")
    // TODO: optimize top-k
    val fVector = vector.toArray.map(_.toFloat)
    val cosineVec = Array.fill[Float](numWords)(0) //所有的词
    val alpha: Float = 1
    val beta: Float = 0

    blas.sgemv("T", vectorSize, numWords, alpha, wordVectors, vectorSize, fVector, 1, beta, cosineVec, 1)

    // Need not divide with the norm of the given vector since it is constant.
    val cosVec = cosineVec.map(_.toDouble)
    var ind = 0
    while (ind < numWords) { //循环每一个词
      cosVec(ind) /= wordVecNorms(ind)
      ind += 1
    }
    wordList.zip(cosVec)
      .toSeq
      .sortBy(- _._2)
      .take(num + 1)
      .tail
      .toArray
  }

  /**
   * Returns a map of words to their vector representations.
    * 返回每一个词 与该词对应的向量
   */
  @Since("1.2.0")
  def getVectors: Map[String, Array[Float]] = {
    wordIndex.map { case (word, ind) => //循环每一个词与词的下标位置
      (word, wordVectors.slice(vectorSize * ind, vectorSize * ind + vectorSize))
    }
  }
}

@Since("1.4.0")
@Experimental
object Word2VecModel extends Loader[Word2VecModel] {

  //参数存储每一个词与该词向量本身
  private def buildWordIndex(model: Map[String, Array[Float]]): Map[String, Int] = {
    model.keys.zipWithIndex.toMap //获取每一个词与该词对应的下标
  }

  private def buildWordVectors(model: Map[String, Array[Float]]): Array[Float] = {
    require(model.nonEmpty, "Word2VecMap should be non-empty")
    //每一个词的向量长度、有多少个词
    val (vectorSize, numWords) = (model.head._2.size, model.size)
    val wordList = model.keys.toArray
    val wordVectors = new Array[Float](vectorSize * numWords) //所有词需要的向量总长度
    var i = 0
    while (i < numWords) { //循环每一个词
      Array.copy(model(wordList(i)), 0, wordVectors, i * vectorSize, vectorSize)
      i += 1
    }
    wordVectors
  }

  private object SaveLoadV1_0 {

    val formatVersionV1_0 = "1.0"

    val classNameV1_0 = "org.apache.spark.mllib.feature.Word2VecModel"

    case class Data(word: String, vector: Array[Float]) //表示每一个词 与 该词的向量形式

    def load(sc: SparkContext, path: String): Word2VecModel = {
      val dataPath = Loader.dataPath(path)
      val sqlContext = new SQLContext(sc)
      val dataFrame = sqlContext.read.parquet(dataPath)

      val dataArray = dataFrame.select("word", "vector").collect()

      // Check schema explicitly since erasure makes it hard to use match-case for checking.
      Loader.checkSchema[Data](dataFrame.schema)

      val word2VecMap = dataArray.map(i => (i.getString(0), i.getSeq[Float](1).toArray)).toMap
      new Word2VecModel(word2VecMap)
    }

    //存储到path中
    //model内容是每一个词与该词的向量形式
    def save(sc: SparkContext, path: String, model: Map[String, Array[Float]]): Unit = {

      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      val vectorSize = model.values.head.size //获取第一个词的向量长度,即每一个词对应的向量长度
      val numWords = model.size //词数量
      //设置元数据:有多少词、每一个词的向量长度
      val metadata = compact(render
        (("class" -> classNameV1_0) ~ ("version" -> formatVersionV1_0) ~
         ("vectorSize" -> vectorSize) ~ ("numWords" -> numWords)))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path)) //保存元数据

      val dataArray = model.toSeq.map { case (w, v) => Data(w, v) } //存储每一个词与该词的向量内容
      sc.parallelize(dataArray.toSeq, 1).toDF().write.parquet(Loader.dataPath(path))
    }
  }

  //加载词向量
  @Since("1.4.0")
  override def load(sc: SparkContext, path: String): Word2VecModel = {

    val (loadedClassName, loadedVersion, metadata) = Loader.loadMetadata(sc, path)
    implicit val formats = DefaultFormats
    val expectedVectorSize = (metadata \ "vectorSize").extract[Int]
    val expectedNumWords = (metadata \ "numWords").extract[Int]
    val classNameV1_0 = SaveLoadV1_0.classNameV1_0
    (loadedClassName, loadedVersion) match {
      case (classNameV1_0, "1.0") =>
        val model = SaveLoadV1_0.load(sc, path)
        val vectorSize = model.getVectors.values.head.size
        val numWords = model.getVectors.size
        require(expectedVectorSize == vectorSize,
          s"Word2VecModel requires each word to be mapped to a vector of size " +
          s"$expectedVectorSize, got vector of size $vectorSize")
        require(expectedNumWords == numWords,
          s"Word2VecModel requires $expectedNumWords words, but got $numWords")
        model
      case _ => throw new Exception(
        s"Word2VecModel.load did not recognize model with (className, format version):" +
        s"($loadedClassName, $loadedVersion).  Supported:\n" +
        s"  ($classNameV1_0, 1.0)")
    }
  }
}
