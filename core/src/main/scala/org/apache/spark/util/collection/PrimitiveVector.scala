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
 * An append-only, non-threadsafe, array-backed vector that is optimized for primitive types.
 * 非线程安全的,只能追加操作的vertor,该vertor是基于数组操作的
 * 存储的元素是一个元组(Long, Int, Double)
 */
private[spark]
class PrimitiveVector[@specialized(Long, Int, Double) V: ClassTag](initialSize: Int = 64) {
  private var _numElements = 0 //元素数量
  private var _array: Array[V] = _ //V是一个元组

  // NB: This must be separate from the declaration, otherwise the specialized parent class
  // will get its own array with the same initial size.
  _array = new Array[V](initialSize)

  //获取第index个位置的元素
  def apply(index: Int): V = {
    require(index < _numElements)
    _array(index)
  }

  //向数组中添加一个V
  def +=(value: V): Unit = {
    if (_numElements == _array.length) { //扩容
      resize(_array.length * 2)
    }
    _array(_numElements) = value
    _numElements += 1
  }

  //获取数组总容量
  def capacity: Int = _array.length

  //获取数组中元素数量
  def length: Int = _numElements
  //获取数组中元素数量
  def size: Int = _numElements

  //迭代器,可以获取每一个元素
  def iterator: Iterator[V] = new Iterator[V] {
    var index = 0
    override def hasNext: Boolean = index < _numElements
    override def next(): V = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val value = _array(index)
      index += 1
      value
    }
  }

  /** Gets the underlying array backing this vector.
   * 返回整个数组  
   **/
  def array: Array[V] = _array

  /** Trims this vector so that the capacity is equal to the size.
   * 返回新的  PrimitiveVector,该新的对象中没有空白地方,数组中所有的元素都有内容
   **/
  def trim(): PrimitiveVector[V] = resize(size)

  /** Resizes the array, dropping elements if the total length decreases.
   * 扩容到新的newLength大小元素
   **/
  def resize(newLength: Int): PrimitiveVector[V] = {
    _array = copyArrayWithLength(newLength) //复制
    if (newLength < _numElements) {
      _numElements = newLength
    }
    this
  }

  /** Return a trimmed version of the underlying array.
   * 重新返回一个新的PrimitiveVector对象
   **/
  def toArray: Array[V] = {
    copyArrayWithLength(size)
  }

  //复制数据到新的数组中
  private def copyArrayWithLength(length: Int): Array[V] = {
    val copy = new Array[V](length)
    _array.copyToArray(copy)
    copy
  }
}
