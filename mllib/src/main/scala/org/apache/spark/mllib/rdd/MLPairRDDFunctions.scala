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

package org.apache.spark.mllib.rdd

import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.util.BoundedPriorityQueue

/**
 * Machine learning specific Pair RDD functions.
  * 其实就是kv键值对形式的RDD---对其进行封装,获取topByKey方法
 */
@DeveloperApi
class MLPairRDDFunctions[K: ClassTag, V: ClassTag](self: RDD[(K, V)]) extends Serializable {
  /**
   * Returns the top k (largest) elements for each key from this RDD as defined by the specified
   * implicit Ordering[T].
    * 对每一个key返回top num个value--注意 value是可以排序比较的对象
   * If the number of elements for a certain key is less than k, all of them will be returned.
   *
   * @param num k, the number of top elements to return
   * @param ord the implicit ordering for T
   * @return an RDD that contains the top k values for each key
   */
  def topByKey(num: Int)(implicit ord: Ordering[V]): RDD[(K, Array[V])] = {//将kv 转换成 k --top的value集合
    self.aggregateByKey(new BoundedPriorityQueue[V](num)(ord))(//优先队列，排序按照ord类型排序，优先队列大小为num
      seqOp = (queue, item) => {
        queue += item
      },
      combOp = (queue1, queue2) => {
        queue1 ++= queue2
      }
    ).mapValues(_.toArray.sorted(ord.reverse))  // This is an min-heap, so we reverse the order.
  }
}

@DeveloperApi
object MLPairRDDFunctions {
  /** Implicit conversion from a pair RDD to MLPairRDDFunctions.
    * kv键值对RDD隐式转换成MLPairRDDFunctions对象
    **/
  implicit def fromPairRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): MLPairRDDFunctions[K, V] =
    new MLPairRDDFunctions[K, V](rdd)
}
