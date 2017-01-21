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

package org.apache.spark.streaming.util

import org.apache.spark.SparkContext
import org.apache.spark.util.collection.OpenHashMap

private[streaming]
object RawTextHelper {

  /**
   * Splits lines and counts the words.
   * 一行一行读取信息
   * 然后拆分一行的内容,按照空格拆分,计算每一个单词出现的次数
   */
  def splitAndCountPartitions(iter: Iterator[String]): Iterator[(String, Long)] = {
    val map = new OpenHashMap[String, Long]
    var i = 0
    var j = 0
    while (iter.hasNext) {
      val s = iter.next() //读取一行信息
      i = 0
      while (i < s.length) {
        j = i
        while (j < s.length && s.charAt(j) != ' ') {//找到下一个空格位置
          j += 1
        }
        if (j > i) {//说明找到下一个空格了
          val w = s.substring(i, j) //i和j之间就是这个单词
          map.changeValue(w, 1L, _ + 1L) //第一次出现该key,则设置1,如果非第一次出现,则累加+1
        }
        i = j
        while (i < s.length && s.charAt(i) == ' ') {
          i += 1
        }
      }
      map.toIterator.map {
        case (k, v) => (k, v)
      }
    }
    map.toIterator.map{case (k, v) => (k, v)}
  }

  /**
   * Gets the top k words in terms of word counts. Assumes that each word exists only once
   * in the `data` iterator (that is, the counts have been reduced).
   * 按照data中第二个参数排序,获取前k个元素
   */
  def topK(data: Iterator[(String, Long)], k: Int): Iterator[(String, Long)] = {
    val taken = new Array[(String, Long)](k) //存储top k的集合  taken数组里面的内容是按照第二个值排序的

    var i = 0
    var len = 0 //taken中已经存储多少个元素
    var done = false
    var value: (String, Long) = null
    var swap: (String, Long) = null
    var count = 0 //数量

    while(data.hasNext) {
      value = data.next()//循环每一个元素
      if (value != null) {
        count += 1
        if (len == 0) {
          taken(0) = value
          len = 1
        } else if (len < k || value._2 > taken(len - 1)._2) { //taken中还有内容  或者 value的long值 比 taken的最后一个都大
          if (len < k) {//taken中还有内容
            len += 1 //元素个数追加1
          }
          taken(len - 1) = value //最后一个元素
          i = len - 1
          while(i > 0 && taken(i - 1)._2 < taken(i)._2) {
            swap = taken(i)
            taken(i) = taken(i-1)
            taken(i - 1) = swap
            i -= 1
          }
        }
      }
    }
    taken.toIterator
  }

  /**
   * Warms up the SparkContext in master and slave by running tasks to force JIT kick in
   * before real workload starts.
   * 暖身项目
   */
  def warmUp(sc: SparkContext) {
    for (i <- 0 to 1) {
      sc.parallelize(1 to 200000, 1000) //并发1000去计算
        .map(_ % 1331).map(_.toString)
        .mapPartitions(splitAndCountPartitions).reduceByKey(_ + _, 10) //最终产生10个partiton
        .count()
    }
  }

  def add(v1: Long, v2: Long): Long = {
    v1 + v2
  }

  def subtract(v1: Long, v2: Long): Long = {
    v1 - v2
  }

  def max(v1: Long, v2: Long): Long = math.max(v1, v2)
}
