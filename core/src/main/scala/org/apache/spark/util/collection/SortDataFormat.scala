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

package org.apache.spark.util.collection

import scala.reflect.ClassTag

/**
 * Abstraction for sorting an arbitrary input buffer of data. This interface requires determining
 * the sort key for a given element index, as well as swapping elements and moving data from one
 * buffer to another.
 *
 * Example format: an array of numbers, where each element is also the key.
 * See [[KVArraySortDataFormat]] for a more exciting format.
 *
 * Note: Declaring and instantiating multiple subclasses of this class would prevent JIT inlining
 * overridden methods and hence decrease the shuffle performance.
 *
 * @tparam K Type of the sort key of each element
 * @tparam Buffer Internal data structure used by a particular format (e.g., Array[Int]).
 */
// TODO: Making Buffer a real trait would be a better abstraction, but adds some complexity.
//按照key的类型排序,排序后存储在Buffer数组中
private[spark]
abstract class SortDataFormat[K, Buffer] {

  /**
   * Creates a new mutable key for reuse. This should be implemented if you want to override
   * [[getKey(Buffer, Int, K)]].
   * 产生新的key
   */
  def newKey(): K = null.asInstanceOf[K]

  /** Return the sort key for the element at the given index. 
   *  获取pos位置的key
   **/
  protected def getKey(data: Buffer, pos: Int): K

  /**
   * Returns the sort key for the element at the given index and reuse the input key if possible.
   * The default implementation ignores the reuse parameter and invokes [[getKey(Buffer, Int]].
   * If you want to override this method, you must implement [[newKey()]].
   * 获取pos位置的key
   */
  def getKey(data: Buffer, pos: Int, reuse: K): K = {
    getKey(data, pos)
  }

  /** Swap two elements. 
   * 交换pos和pos1两个位置  
   **/
  def swap(data: Buffer, pos0: Int, pos1: Int): Unit

  /** Copy a single element from src(srcPos) to dst(dstPos). 
   * 将src的  srcPos位置,copy到dst的dstPos位置
   **/
  def copyElement(src: Buffer, srcPos: Int, dst: Buffer, dstPos: Int): Unit

  /**
   * Copy a range of elements starting at src(srcPos) to dst, starting at dstPos.
   * Overlapping ranges are allowed.
   * 将src数组上的一组信息,copy到dst数组上
   */
  def copyRange(src: Buffer, srcPos: Int, dst: Buffer, dstPos: Int, length: Int): Unit

  /**
   * Allocates a Buffer that can hold up to 'length' elements.
   * All elements of the buffer should be considered invalid until data is explicitly copied in.
   * 分配内存空间
   */
  def allocate(length: Int): Buffer
}

/**
 * Supports sorting an array of key-value pairs where the elements of the array alternate between
 * keys and values, as used in [[AppendOnlyMap]].
 *
 * @tparam K Type of the sort key of each element 每一个元素按照key进行排序的,是一个类型
 * @tparam T Type of the Array we're sorting. Typically this must extend AnyRef, to support cases
 *           when the keys and values are not the same type.存储key-value排序后的数组
 * 该对象是一个排序后的key-value键值对,被使用于AppendOnlyMap
 */
private[spark]
class KVArraySortDataFormat[K, T <: AnyRef : ClassTag] extends SortDataFormat[K, Array[T]] {

  //在data数组中获取第pos位置的key对象
  override def getKey(data: Array[T], pos: Int): K = data(2 * pos).asInstanceOf[K]

  //交换key在pos0和pos1这两个位置
  override def swap(data: Array[T], pos0: Int, pos1: Int) {
    val tmpKey = data(2 * pos0)
    val tmpVal = data(2 * pos0 + 1)
    data(2 * pos0) = data(2 * pos1)
    data(2 * pos0 + 1) = data(2 * pos1 + 1)
    data(2 * pos1) = tmpKey
    data(2 * pos1 + 1) = tmpVal
  }

  //使目标数组的dstPos位置对应的key-value,设置为从src的srcPos位置的key-value
  override def copyElement(src: Array[T], srcPos: Int, dst: Array[T], dstPos: Int) {
    dst(2 * dstPos) = src(2 * srcPos)
    dst(2 * dstPos + 1) = src(2 * srcPos + 1)
  }

  //将src的数组copy到dst中
  override def copyRange(src: Array[T], srcPos: Int, dst: Array[T], dstPos: Int, length: Int) {
    System.arraycopy(src, 2 * srcPos, dst, 2 * dstPos, 2 * length)
  }

  //分配一个长度2倍的数组,用于存储key-value,因此是2倍
  override def allocate(length: Int): Array[T] = {
    new Array[T](2 * length)
  }
}
