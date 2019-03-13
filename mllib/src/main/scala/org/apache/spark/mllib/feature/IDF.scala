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

import breeze.linalg.{DenseVector => BDV}

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.rdd.RDD

/**
 * :: Experimental ::
 * Inverse document frequency (IDF).
 * The standard formulation is used: `idf = log((m + 1) / (d(t) + 1))`, where `m` is the total
 * number of documents and `d(t)` is the number of documents that contain term `t`.
 *
 * This implementation supports filtering out terms which do not appear in a minimum number
 * of documents (controlled by the variable `minDocFreq`). For terms that are not in
 * at least `minDocFreq` documents, the IDF is found as 0, resulting in TF-IDFs of 0.
 *
 * @param minDocFreq minimum of documents in which a term
 *                   should appear for filtering
 */
@Since("1.1.0")
@Experimental
class IDF @Since("1.2.0") (@Since("1.2.0") val minDocFreq: Int) {

  @Since("1.1.0")
  def this() = this(0)

  // TODO: Allow different IDF formulations.

  /**
   * Computes the inverse document frequency.
    * 计算每一个term出现在多少个doc中,（有多少个词，数组--每一个出现的词，数组--每一个词出现在多少个doc中）
   * @param dataset an RDD of term frequency vectors
    * 参数是词频向量,即使用HashingTF处理后的向量,总共都少词、词是否出现，出现都少次
   */
  @Since("1.1.0")
  def fit(dataset: RDD[Vector]): IDFModel = {
    val idf = dataset.treeAggregate(new IDF.DocumentFrequencyAggregator(
          minDocFreq = minDocFreq))(
      seqOp = (df, v) => df.add(v),
      combOp = (df1, df2) => df1.merge(df2)
    ).idf()
    new IDFModel(idf)
  }

  /**
   * Computes the inverse document frequency.
   * @param dataset a JavaRDD of term frequency vectors
   */
  @Since("1.1.0")
  def fit(dataset: JavaRDD[Vector]): IDFModel = {
    fit(dataset.rdd)
  }
}

private object IDF {

  /** Document frequency aggregator. 表示每一个term出现在都少个doc中*/
  class DocumentFrequencyAggregator(val minDocFreq: Int) extends Serializable {

    /** number of documents */
    private var m = 0L //map阶段包含了都少个文档
    /** document frequency vector */
    private var df: BDV[Long] = _ //词频对象---向量一个位置表示一个词，具体的值表示词出现在多少document中


    def this() = this(0)

    /** Adds a new document. map阶段添加一个文档*/
    def add(doc: Vector): this.type = {
      if (isEmpty) { //创建词频集合
        df = BDV.zeros(doc.size)
      }
      doc match {
        case SparseVector(size, indices, values) => //稀疏向量
          val nnz = indices.size //该文档有多少个词
          var k = 0
          while (k < nnz) {//依次迭代每一个term
            if (values(k) > 0) {//该term对应在该文档出现次数
              df(indices(k)) += 1L //说明出现在一个doc中
            }
            k += 1
          }
        case DenseVector(values) => //密集向量
          val n = values.size
          var j = 0
          while (j < n) {
            if (values(j) > 0.0) {
              df(j) += 1L
            }
            j += 1
          }
        case other =>
          throw new UnsupportedOperationException(
            s"Only sparse and dense vectors are supported but got ${other.getClass}.")
      }
      m += 1L
      this
    }

    /** Merges another. reduce阶段*/
    def merge(other: DocumentFrequencyAggregator): this.type = {
      if (!other.isEmpty) {
        m += other.m
        if (df == null) {
          df = other.df.copy
        } else {
          df += other.df
        }
      }
      this
    }

    private def isEmpty: Boolean = m == 0L

    /** Returns the current IDF vector.
      * 返回最终汇总后的向量
      **/
    def idf(): Vector = {
      if (isEmpty) {
        throw new IllegalStateException("Haven't seen any document yet.")
      }
      val n = df.length
      val inv = new Array[Double](n)
      var j = 0
      while (j < n) {
        /*
         * If the term is not present in the minimum
         * number of documents, set IDF to 0. This
         * will cause multiplication in IDFModel to
         * set TF-IDF to 0.
         * 如果该term所在文档数量小于minDocFreq,我们设置为0
         * Since arrays are initialized to 0 by default,
         * we just omit changing those entries.
         */
        if (df(j) >= minDocFreq) {
          inv(j) = math.log((m + 1.0) / (df(j) + 1.0))
        }
        j += 1
      }
      Vectors.dense(inv)
    }
  }
}

/**
 * :: Experimental ::
 * Represents an IDF model that can transform term frequency vectors.
  * 具体模型
 */
@Experimental
@Since("1.1.0")
class IDFModel private[spark] (@Since("1.1.0") val idf: Vector) extends Serializable {

  /**
   * Transforms term frequency (TF) vectors to TF-IDF vectors.
   *
   * If `minDocFreq` was set for the IDF calculation,
   * the terms which occur in fewer than `minDocFreq`
   * documents will have an entry of 0.
   *
   * @param dataset an RDD of term frequency vectors 词频向量
   * @return an RDD of TF-IDF vectors
   */
  @Since("1.1.0")
  def transform(dataset: RDD[Vector]): RDD[Vector] = {
    val bcIdf = dataset.context.broadcast(idf) //广播模型
    dataset.mapPartitions(iter => iter.map(v => IDFModel.transform(bcIdf.value, v))) //每一个向量与模型进行分析
  }

  /**
   * Transforms a term frequency (TF) vector to a TF-IDF vector
   *
   * @param v a term frequency vector
   * @return a TF-IDF vector
   */
  @Since("1.3.0")
  def transform(v: Vector): Vector = IDFModel.transform(idf, v)

  /**
   * Transforms term frequency (TF) vectors to TF-IDF vectors (Java version).
   * @param dataset a JavaRDD of term frequency vectors
   * @return a JavaRDD of TF-IDF vectors
   */
  @Since("1.1.0")
  def transform(dataset: JavaRDD[Vector]): JavaRDD[Vector] = {
    transform(dataset.rdd).toJavaRDD()
  }
}

private object IDFModel {

  /**
   * Transforms a term frequency (TF) vector to a TF-IDF vector with a IDF vector
   *
   * @param idf an IDF vector 模型
   * @param v a term frequence vector 要预测的向量
   * @return a TF-IDF vector
   */
  def transform(idf: Vector, v: Vector): Vector = {
    val n = v.size
    v match {
      case SparseVector(size, indices, values) =>
        val nnz = indices.size //该doc包含多少个term
        val newValues = new Array[Double](nnz)
        var k = 0
        while (k < nnz) {//循环每一个term
          newValues(k) = values(k) * idf(indices(k)) //该文档词频 * 该文档中该词的权重
          k += 1
        }
        Vectors.sparse(n, indices, newValues)
      case DenseVector(values) =>
        val newValues = new Array[Double](n)
        var j = 0
        while (j < n) {
          newValues(j) = values(j) * idf(j)
          j += 1
        }
        Vectors.dense(newValues)
      case other =>
        throw new UnsupportedOperationException(
          s"Only sparse and dense vectors are supported but got ${other.getClass}.")
    }
  }
}
