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

import java.util.{Arrays, Comparator}

import com.google.common.hash.Hashing

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * A simple open hash table optimized for the append-only use case, where keys
 * are never removed, but the value for each key may be changed.
 *
 * This implementation uses quadratic probing with a power-of-2 hash table
 * size, which is guaranteed to explore all spaces for each key (see
 * http://en.wikipedia.org/wiki/Quadratic_probing).
 *
 * The map can support up to `375809638 (0.7 * 2 ^ 29)` elements.
 *
 * TODO: Cache the hash values of each key? java.util.HashMap does that.
 * 类似hash table的实现
 */
@DeveloperApi
class AppendOnlyMap[K, V](initialCapacity: Int = 64)
  extends Iterable[(K, V)] with Serializable {

  import AppendOnlyMap._

  require(initialCapacity <= MAXIMUM_CAPACITY,
    s"Can't make capacity bigger than ${MAXIMUM_CAPACITY} elements")
  require(initialCapacity >= 1, "Invalid initial capacity")

  private val LOAD_FACTOR = 0.7

  private var capacity = nextPowerOf2(initialCapacity) //当前容量
  private var mask = capacity - 1
  private var curSize = 0 //当前存储多少个元素
  private var growThreshold = (LOAD_FACTOR * capacity).toInt //扩容数据临界点

  // Holds keys and values in the same array for memory locality; specifically, the order of
  // elements is key0, value0, key1, value1, key2, value2, etc.
  //存储key和value在同一个数组中
  private var data = new Array[AnyRef](2 * capacity)

  // Treat the null key differently so we can use nulls in "data" to represent empty items.
  private var haveNullValue = false //true说明有key=null的被存储进来了
  private var nullValue: V = null.asInstanceOf[V] //返回key是null对应的value

  // Triggered by destructiveSortedIterator; the underlying data array may no longer be used
  private var destroyed = false //true表示不能再被使用
  private val destructionMessage = "Map state is invalid from destructive sorting!" //错误信息

  /** Get the value for a given key 
   * 给定key,获取对应的value返回  
   **/
  def apply(key: K): V = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]
    if (k.eq(null)) {
      return nullValue
    }
    var pos = rehash(k.hashCode) & mask //返回k应该存储在哪个数组的位置上
    var i = 1
    while (true) {
      val curKey = data(2 * pos) //获取当前key所在位置
      if (k.eq(curKey) || k.equals(curKey)) {//获取对应的value返回
        return data(2 * pos + 1).asInstanceOf[V]
      } else if (curKey.eq(null)) {//如果key是null,说明不存在,则返回null
        return null.asInstanceOf[V]
      } else {//如果key与参数不一致,则说明有冲突,则继续寻找
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
    null.asInstanceOf[V]
  }

  /** Set the value for a key 
   * 更新key-value的映射关系  
   **/
  def update(key: K, value: V): Unit = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]
    if (k.eq(null)) { //如果key是null,则设置默认null的value值
      if (!haveNullValue) {
        incrementSize()
      }
      nullValue = value
      haveNullValue = true
      return
    }
    var pos = rehash(key.hashCode) & mask //找到该key对应的位置
    var i = 1
    while (true) {
      val curKey = data(2 * pos) //查看该key位置对应的key
      if (curKey.eq(null)) {//说明该key位置空着
        data(2 * pos) = k //设置该key
        data(2 * pos + 1) = value.asInstanceOf[AnyRef] //设置该key对应的value
        incrementSize()  // Since we added a new key 增加一个数据
        return
      } else if (k.eq(curKey) || k.equals(curKey)) { //如果该key位置已经存在,则更改改value
        data(2 * pos + 1) = value.asInstanceOf[AnyRef]
        return
      } else {//说明冲突,则继续寻找该key的位置
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
  }

  /**
   * Set the value for key to updateFunc(hadValue, oldValue), where oldValue will be the old value
   * for key, if any, or null otherwise. Returns the newly updated value.
   * @updateFunc 参数是元组,1表示该key是否存在value,2参数表示存在的old的value值,返回值就是新的value
   * 返回最新的value
   */
  def changeValue(key: K, updateFunc: (Boolean, V) => V): V = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]
    if (k.eq(null)) {
      if (!haveNullValue) {
        incrementSize()
      }
      nullValue = updateFunc(haveNullValue, nullValue)
      haveNullValue = true
      return nullValue
    }
    var pos = rehash(k.hashCode) & mask //找到该key存储的位置
    var i = 1
    while (true) {
      val curKey = data(2 * pos) //获取该key的值
      if (k.eq(curKey) || k.equals(curKey)) { //如果就是参数对应的key,传入true,和老的value值
        val newValue = updateFunc(true, data(2 * pos + 1).asInstanceOf[V])
        data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]
        return newValue
      } else if (curKey.eq(null)) {//说明该key不存在
        val newValue = updateFunc(false, null.asInstanceOf[V]) //传入false,null来更新最新值
        data(2 * pos) = k
        data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]
        incrementSize()
        return newValue
      } else { //解决冲突
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
    null.asInstanceOf[V] // Never reached but needed to keep compiler happy
  }

  /** Iterator method from Iterable 
   *  循环每一个元组,即key-value组成的的元组
   **/
  override def iterator: Iterator[(K, V)] = {
    assert(!destroyed, destructionMessage)
    new Iterator[(K, V)] {
      var pos = -1 //下一次读取的位置

      /** Get the next value we should return from next(), or null if we're finished iterating
       *  获取下一个元组key-value
       **/
      def nextValue(): (K, V) = {
        if (pos == -1) {    // Treat position -1 as looking at the null value 先从key=null开始读取
          if (haveNullValue) {//存在默认的value,则返回key=null,默认的value
            return (null.asInstanceOf[K], nullValue)
          }
          pos += 1
        }
        
        while (pos < capacity) {//一直循环到容量范围外结束
          if (!data(2 * pos).eq(null)) {//key不是null,就返回key-value元组
            return (data(2 * pos).asInstanceOf[K], data(2 * pos + 1).asInstanceOf[V])
          }
          pos += 1
        }
        null
      }

      override def hasNext: Boolean = nextValue() != null

      //返回下一个key-value键值对
      override def next(): (K, V) = {
        val value = nextValue()
        if (value == null) {
          throw new NoSuchElementException("End of iterator")
        }
        pos += 1
        value
      }
    }
  }

  override def size: Int = curSize //当前容器存储了多少个元素

  /** Increase table size by 1, rehashing if necessary 
   * 扩容  
   **/
  private def incrementSize() {
    curSize += 1
    if (curSize > growThreshold) {
      growTable()
    }
  }

  /**
   * Re-hash a value to deal better with hash functions that don't differ in the lower bits.
   */
  private def rehash(h: Int): Int = Hashing.murmur3_32().hashInt(h).asInt()

  /** Double the table's size and re-hash everything 
   *  扩容
   **/
  protected def growTable() {
    // capacity < MAXIMUM_CAPACITY (2 ^ 29) so capacity * 2 won't overflow
    val newCapacity = capacity * 2
    require(newCapacity <= MAXIMUM_CAPACITY, s"Can't contain more than ${growThreshold} elements")
    val newData = new Array[AnyRef](2 * newCapacity)
    val newMask = newCapacity - 1
    // Insert all our old values into the new array. Note that because our old keys are
    // unique, there's no need to check for equality here when we insert.
    var oldPos = 0
    while (oldPos < capacity) {
      if (!data(2 * oldPos).eq(null)) {
        val key = data(2 * oldPos)
        val value = data(2 * oldPos + 1)
        var newPos = rehash(key.hashCode) & newMask
        var i = 1
        var keepGoing = true
        while (keepGoing) {
          val curKey = newData(2 * newPos)
          if (curKey.eq(null)) {
            newData(2 * newPos) = key
            newData(2 * newPos + 1) = value
            keepGoing = false
          } else {
            val delta = i
            newPos = (newPos + delta) & newMask
            i += 1
          }
        }
      }
      oldPos += 1
    }
    data = newData
    capacity = newCapacity
    mask = newMask
    growThreshold = (LOAD_FACTOR * newCapacity).toInt
  }

  private def nextPowerOf2(n: Int): Int = {
    val highBit = Integer.highestOneBit(n)
    if (highBit == n) n else highBit << 1
  }

  /**
   * Return an iterator of the map in sorted order. This provides a way to sort the map without
   * using additional memory, at the expense of destroying the validity of the map.
   * 对data中数据进行排序,返回排序后的迭代器
   * 
   * 因为对data原始数据进行了排序.因此该数组数据已经被损坏了,因为不能按照原来的方式对key进行hash后找到该key对应的value了,因此该方法之后,原始的data就作废了
   */
  def destructiveSortedIterator(keyComparator: Comparator[K]): Iterator[(K, V)] = {
    destroyed = true //将原始data设置为过期作废状态
    // Pack KV pairs into the front of the underlying array
    //以下的while循环,使data数据集更紧凑,将中间的null的位置给过滤掉,例如data的capacity=100,但是其中60个是有数据的,40个没有数据,因此经过while之后,前60个都是有数据的,后40个都是null
    var keyIndex, newIndex = 0
    while (keyIndex < capacity) {
      if (data(2 * keyIndex) != null) {//如果该keyIndex位置有key
        data(2 * newIndex) = data(2 * keyIndex) //则移动该key和value移动到数组的下一个空白位置
        data(2 * newIndex + 1) = data(2 * keyIndex + 1)
        newIndex += 1
      }
      keyIndex += 1
    }
    assert(curSize == newIndex + (if (haveNullValue) 1 else 0)) //确保数据存储的总数量curSize = 最后一个有值的位置newIndex + value的默认值

    //对data中从0到newIndex位置按照keyComparator进行排序,即对data中所有的数据进行排序
    new Sorter(new KVArraySortDataFormat[K, AnyRef]).sort(data, 0, newIndex, keyComparator)

    //返回排序后的迭代器,先迭代key=null的情况
    new Iterator[(K, V)] {
      var i = 0 //已经迭代到第几个下标位置了
      var nullValueReady = haveNullValue
      def hasNext: Boolean = (i < newIndex || nullValueReady)
      def next(): (K, V) = {
        if (nullValueReady) {//先迭代key=null的情况
          nullValueReady = false
          (null.asInstanceOf[K], nullValue)
        } else {//返回下一个key-value元组
          val item = (data(2 * i).asInstanceOf[K], data(2 * i + 1).asInstanceOf[V])
          i += 1
          item
        }
      }
    }
  }

  /**
   * Return whether the next insert will cause the map to grow
   * 是否马上要进行扩容了
   */
  def atGrowThreshold: Boolean = curSize == growThreshold
}

private object AppendOnlyMap {
  val MAXIMUM_CAPACITY = (1 << 29) //最大容量
}
