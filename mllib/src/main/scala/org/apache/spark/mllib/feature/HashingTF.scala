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

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

/**
 * :: Experimental ::
 * Maps a sequence of terms to their term frequencies using the hashing trick.
 *
 * @param numFeatures number of features (default: 2^20^)  默认保证词在2^20以内
  * 用于分析每一个文档出现了哪些词，以及对应的词频，一个文档的所有term词转换成一个向量
 */
@Since("1.1.0")
@Experimental
class HashingTF(val numFeatures: Int) extends Serializable {

  /**
   */
  @Since("1.1.0")
  def this() = this(1 << 20) //默认值2^20

  /**
   * Returns the index of the input term.
   */
  @Since("1.1.0")
  def indexOf(term: Any): Int = Utils.nonNegativeMod(term.##, numFeatures) //Any.##表示hash值--对hash值计算摩

  /**
   * Transforms the input document into a sparse term frequency vector.
    * 输入是文档对应的所有term词----内容是term的id,因为已经确保了term的id是唯一的,因此hash是可以工作的
    * 如果term是具体的词,而不是id,那么就有可能冲突,即不同的term返回的hash值是相同的。但是基本上该term说明是ascii码
    * 返回向量,即稀疏向量,描述哪些词有词频 以及 词频大小是多少
    * 输出向量三部分(向量总大小,数组--描述词序号,数组--描述每一个词对应的出现次数)
   */
  @Since("1.1.0")
  def transform(document: Iterable[_]): Vector = {
    val termFrequencies = mutable.HashMap.empty[Int, Double]//term词所在位置,value是出现的词频
    document.foreach { term =>
      val i = indexOf(term) //计算该term的hash值所在位置
      termFrequencies.put(i, termFrequencies.getOrElse(i, 0.0) + 1.0)
    }
    Vectors.sparse(numFeatures, termFrequencies.toSeq)
  }

  /**
   * Transforms the input document into a sparse term frequency vector (Java version).
   */
  @Since("1.1.0")
  def transform(document: JavaIterable[_]): Vector = {
    transform(document.asScala)
  }

  /**
   * Transforms the input document to term frequency vectors.
    * 参数RDD中的D是一个迭代器类型,表示一个document中所有的term词
   */
  @Since("1.1.0")
  def transform[D <: Iterable[_]](dataset: RDD[D]): RDD[Vector] = {
    dataset.map(this.transform)
  }

  /**
   * Transforms the input document to term frequency vectors (Java version).
   */
  @Since("1.1.0")
  def transform[D <: JavaIterable[_]](dataset: JavaRDD[D]): JavaRDD[Vector] = {
    dataset.rdd.map(this.transform).toJavaRDD()
  }
}
