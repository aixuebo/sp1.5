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

package org.apache.spark.graphx.util.collection

import org.apache.spark.util.collection.OpenHashSet

import scala.reflect._

/**
 * A fast hash map implementation for primitive, non-null keys. This hash map supports
 * insertions and updates, but not deletions. This map is about an order of magnitude
 * faster than java.util.HashMap, while using much less space overhead.
 *
 * Under the hood, it uses our OpenHashSet implementation.
 * 其实就是Map
 * key存储在OpenHashSet中,因为OpenHashSet当你给他key的时候,他会返回给你一个int,该int就表示该key存储的具体值.通过该int可以在_values中找到对应的map的value值
 */
private[graphx]
class GraphXPrimitiveKeyOpenHashMap[@specialized(Long, Int) K: ClassTag,
                              @specialized(Long, Int, Double) V: ClassTag](
    val keySet: OpenHashSet[K], var _values: Array[V]) //参数是一个key的set集合,还有存储value的数组
  extends Iterable[(K, V)]
  with Serializable {

  /**
   * Allocate an OpenHashMap with a fixed initial capacity
   */
  def this(initialCapacity: Int) =
    this(new OpenHashSet[K](initialCapacity), new Array[V](initialCapacity))

  /**
   * Allocate an OpenHashMap with a default initial capacity, providing a true
   * no-argument constructor.
   */
  def this() = this(64)

  /**
   * Allocate an OpenHashMap with a fixed initial capacity
   */
  def this(keySet: OpenHashSet[K]) = this(keySet, new Array[V](keySet.capacity))

  require(classTag[K] == classTag[Long] || classTag[K] == classTag[Int])

  private var _oldValues: Array[V] = null

  override def size: Int = keySet.size

  /** Get the value for a given key
    * 相当于map的get方法
    **/
  def apply(k: K): V = {
    val pos = keySet.getPos(k) //获取key对应的存储index
    _values(pos) //从数组中获取index的值
  }

  /** Get the value for a given key, or returns elseValue if it doesn't exist. */
  def getOrElse(k: K, elseValue: V): V = {
    val pos = keySet.getPos(k)
    if (pos >= 0) _values(pos) else elseValue
  }

  /** Set the value for a key */
  def update(k: K, v: V) { //相当于map.put方法
    val pos = keySet.addWithoutResize(k) & OpenHashSet.POSITION_MASK
    _values(pos) = v
    keySet.rehashIfNeeded(k, grow, move)
    _oldValues = null
  }


  /** Set the value for a key
    * 对相同的key如果有值存在，要进行合并
    *
    * 没有返回值
    **/
  def setMerge(k: K, v: V, mergeF: (V, V) => V) {
    val pos = keySet.addWithoutResize(k)
    val ind = pos & OpenHashSet.POSITION_MASK
    if ((pos & OpenHashSet.NONEXISTENCE_MASK) != 0) { // if first add
      _values(ind) = v
    } else {
      _values(ind) = mergeF(_values(ind), v)
    }
    keySet.rehashIfNeeded(k, grow, move)
    _oldValues = null
  }


  /**
   * If the key doesn't exist yet in the hash map, set its value to defaultValue; otherwise,
   * set its value to mergeValue(oldValue).
   *
   * @return the newly updated value.
   * 返回值是最新的值
   */
  def changeValue(k: K, defaultValue: => V, mergeValue: (V) => V): V = {
    val pos = keySet.addWithoutResize(k)
    if ((pos & OpenHashSet.NONEXISTENCE_MASK) != 0) {
      val newValue = defaultValue
      _values(pos & OpenHashSet.POSITION_MASK) = newValue
      keySet.rehashIfNeeded(k, grow, move)
      newValue
    } else {
      _values(pos) = mergeValue(_values(pos))
      _values(pos)
    }
  }

  override def iterator: Iterator[(K, V)] = new Iterator[(K, V)] {
    var pos = 0
    var nextPair: (K, V) = computeNextPair()

    /** Get the next value we should return from next(), or null if we're finished iterating
      * 寻找下一个key=value的键值对
      **/
    def computeNextPair(): (K, V) = {
      pos = keySet.nextPos(pos)
      if (pos >= 0) {
        val ret = (keySet.getValue(pos), _values(pos))
        pos += 1
        ret
      } else {
        null
      }
    }

    def hasNext: Boolean = nextPair != null

    def next(): (K, V) = {
      val pair = nextPair
      nextPair = computeNextPair()
      pair
    }
  }

  // The following member variables are declared as protected instead of private for the
  // specialization to work (specialized class extends the unspecialized one and needs access
  // to the "private" variables).
  // They also should have been val's. We use var's because there is a Scala compiler bug that
  // would throw illegal access error at runtime if they are declared as val's.
  protected var grow = (newCapacity: Int) => {
    _oldValues = _values
    _values = new Array[V](newCapacity)
  }

  protected var move = (oldPos: Int, newPos: Int) => {
    _values(newPos) = _oldValues(oldPos)
  }
}
