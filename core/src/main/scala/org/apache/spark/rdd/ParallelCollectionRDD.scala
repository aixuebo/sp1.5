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

package org.apache.spark.rdd

import java.io._

import scala.Serializable
import scala.collection.Map
import scala.collection.immutable.NumericRange
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.Utils

//表示一个partition部分
private[spark] class ParallelCollectionPartition[T: ClassTag](
    var rddId: Long,//RDD的唯一ID
    var slice: Int,//是第几个partition部分
    var values: Seq[T])//该partition包含的数组内容
    extends Partition with Serializable {

  def iterator: Iterator[T] = values.iterator //迭代该partition对应的数据

  override def hashCode(): Int = (41 * (41 + rddId) + slice).toInt //通过RDD的ID以及partition部分确定hash

  override def equals(other: Any): Boolean = other match {
    case that: ParallelCollectionPartition[_] =>
      this.rddId == that.rddId && this.slice == that.slice
    case _ => false
  }

  override def index: Int = slice //返回第几个partition

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {

    val sfactory = SparkEnv.get.serializer

    // Treat java serializer with default action rather than going thru serialization, to avoid a
    // separate serialization header.

    sfactory match {
      case js: JavaSerializer => out.defaultWriteObject()
      case _ =>
        out.writeLong(rddId)//序列化RDD的ID
        out.writeInt(slice)//序列化partitionId

        //序列化partition内的每一个内容
        val ser = sfactory.newInstance()
        Utils.serializeViaNestedStream(out, ser)(_.writeObject(values))
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {

    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => in.defaultReadObject()
      case _ =>
        rddId = in.readLong()
        slice = in.readInt()

        val ser = sfactory.newInstance()
        Utils.deserializeViaNestedStream(in, ser)(ds => values = ds.readObject[Seq[T]]())
    }
  }
}

private[spark] class ParallelCollectionRDD[T: ClassTag](
    @transient sc: SparkContext,
    @transient data: Seq[T],//等待拆分的数据集合
    numSlices: Int,//等待将要被拆分成多少组
    locationPrefs: Map[Int, Seq[String]]) //key是partitionId,value是该partitionId对应的存储路径集合,即可以从这些集合内的路径获取该partitionId的信息
    extends RDD[T](sc, Nil) {//该RDD是根RDD,因为父RDD是Nil
  // TODO: Right now, each split sends along its full data, even if later down the RDD chain it gets
  // cached. It might be worthwhile to write the data to a file in the DFS and read it in the split
  // instead.
  // UPDATE: A parallel collection can be checkpointed to HDFS, which achieves this goal.

  //返回ParallelCollectionPartition数组
  override def getPartitions: Array[Partition] = {
    val slices = ParallelCollectionRDD.slice(data, numSlices).toArray //返回 Array[Seq[T]],将data数据拆分成numSlices个数组
    //将每一个元素组成ParallelCollectionPartition,最后返回ParallelCollectionPartition数组
    slices.indices.map(i => new ParallelCollectionPartition(id, i, slices(i))).toArray
  }

  //计算每一个ParallelCollectionPartition对象
  override def compute(s: Partition, context: TaskContext): Iterator[T] = {
    new InterruptibleIterator(context, s.asInstanceOf[ParallelCollectionPartition[T]].iterator)
  }

  //获取partitionId对应的存储路径集合
  override def getPreferredLocations(s: Partition): Seq[String] = {
    locationPrefs.getOrElse(s.index, Nil)
  }
}

private object ParallelCollectionRDD {
  /**
   * Slice a collection into numSlices sub-collections. One extra thing we do here is to treat Range
   * collections specially, encoding the slices as other Ranges to minimize memory cost. This makes
   * it efficient to run Spark over RDDs representing large sets of numbers. And if the collection
   * is an inclusive Range, we use inclusive range for the last slice.
   * 将集合seq拆分成numSlices个部分,返回值就是Seq[Seq[T]],第一维度数组长度是numSlices,每一个元素都是Seq[T]
   */
  def slice[T: ClassTag](seq: Seq[T], numSlices: Int): Seq[Seq[T]] = {
    if (numSlices < 1) {//不能拆分成小于1的partition
      throw new IllegalArgumentException("Positive number of slices required")
    }
    // Sequences need to be sliced at the same set of index positions for operations
    // like RDD.zip() to behave as expected
    /**
例如:参数是20,3
结果是三个元组:
0==6
6==13
13==20
      参数length表示一共数组有多少个元素
      参数numSlices 表示分多少个partition
      返回每一个分组在array中的开始位置和结束位置,这样就把数组分成若干个组了
     */
    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
      (0 until numSlices).iterator.map(i => {
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt
        (start, end)
      })
    }
    
    seq match {
      case r: Range => {//参见com.scala.collection.RangeTest的testRange方法
        positions(r.length, numSlices).zipWithIndex.map({ case ((start, end), index) =>
          // If the range is inclusive, use inclusive range for the last slice
          if (r.isInclusive && index == numSlices - 1) {
            new Range.Inclusive(r.start + start * r.step, r.end, r.step)
          }
          else {
            new Range(r.start + start * r.step, r.start + end * r.step, r.step)
          }
        }).toSeq.asInstanceOf[Seq[Seq[T]]] //返回N个Range对象,每一个Range对象又是T集合,因此返回值是Seq[Seq[Int]]
      }
      case nr: NumericRange[_] => { //不仅仅提供了Integer的区间操作,也实现了Range.Long、Range.BigInt、Range.BigDecimal等区间操作
        // For ranges of Long, Double, BigInteger, etc
        val slices = new ArrayBuffer[Seq[T]](numSlices) //最终结果是一个ArrayBuffer,数组的大小就是numSlices个,每一个元素都是一个NumericRange[_]类型的元素,NumericRange[_]仅仅是原始nr的一部分而已
        var r = nr
        for ((start, end) <- positions(nr.length, numSlices)) {
          val sliceSize = end - start //获取一部分原始数据
          slices += r.take(sliceSize).asInstanceOf[Seq[T]] //将获取的一部分原始数据填充到slices数组中
          r = r.drop(sliceSize) //从原始的数据中取消这一部分数据
        }
        slices
      }
      case _ => {//返回的Array[Array[T]]每一个元素都是Array[T]
        val array = seq.toArray // To prevent O(n^2) operations for List etc
        positions(array.length, numSlices).map({
          case (start, end) =>
            array.slice(start, end).toSeq //获取array的子集数据集,
        }).toSeq
      }
    }
  }
}
