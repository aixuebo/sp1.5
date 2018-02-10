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

import scala.reflect._
import com.google.common.hash.Hashing

import org.apache.spark.annotation.Private

/**
 * A simple, fast hash set optimized for non-null insertion-only use case, where keys are never
 * removed.
 *
 * The underlying implementation uses Scala compiler's specialization to generate optimized
 * storage for two primitive types (Long and Int). It is much faster than Java's standard HashSet
 * while incurring much less memory overhead. This can serve as building blocks for higher level
 * data structures such as an optimized HashMap.
 *
 * This OpenHashSet is designed to serve as building blocks for higher level data structures
 * such as an optimized hash map. Compared with standard hash set implementations, this class
 * provides its various callbacks interfaces (e.g. allocateFunc, moveFunc) and interfaces to
 * retrieve the position of a key in the underlying array.
 *
 * It uses quadratic probing with a power-of-2 hash table size, which is guaranteed
 * to explore all spaces for each key (see http://en.wikipedia.org/wiki/Quadratic_probing).
 * 就是一个set集合.只是set的内容都存储在数组中,元素内容做一个hash,可以知道数组的位置
 */
@Private
class OpenHashSet[@specialized(Long, Int) T: ClassTag](
    initialCapacity: Int,
    loadFactor: Double)
  extends Serializable {

  require(initialCapacity <= OpenHashSet.MAX_CAPACITY,
    s"Can't make capacity bigger than ${OpenHashSet.MAX_CAPACITY} elements")
  require(initialCapacity >= 1, "Invalid initial capacity")
  require(loadFactor < 1.0, "Load factor must be less than 1.0")
  require(loadFactor > 0.0, "Load factor must be greater than 0.0")

  import OpenHashSet._

  def this(initialCapacity: Int) = this(initialCapacity, 0.7)

  def this() = this(64)

  // The following member variables are declared as protected instead of private for the
  // specialization to work (specialized class extends the non-specialized one and needs access
  // to the "private" variables).

  //根据T的类型转换成不同的hash函数
  protected val hasher: Hasher[T] = {
    // It would've been more natural to write the following using pattern matching. But Scala 2.9.x
    // compiler has a bug when specialization is used together with this pattern matching, and
    // throws:
    // scala.tools.nsc.symtab.Types$TypeError: type mismatch;
    //  found   : scala.reflect.AnyValManifest[Long]
    //  required: scala.reflect.ClassTag[Int]
    //         at scala.tools.nsc.typechecker.Contexts$Context.error(Contexts.scala:298)
    //         at scala.tools.nsc.typechecker.Infer$Inferencer.error(Infer.scala:207)
    //         ...
    val mt = classTag[T]
    if (mt == ClassTag.Long) {
      (new LongHasher).asInstanceOf[Hasher[T]]
    } else if (mt == ClassTag.Int) {
      (new IntHasher).asInstanceOf[Hasher[T]]
    } else {
      new Hasher[T]
    }
  }

  protected var _capacity = nextPowerOf2(initialCapacity)
  protected var _mask = _capacity - 1
  protected var _size = 0
  protected var _growThreshold = (loadFactor * _capacity).toInt //达到该size后就要扩容

  protected var _bitset = new BitSet(_capacity) //数组的位置被占用了,则设置为1

  def getBitSet: BitSet = _bitset

  // Init of the array in constructor (instead of in declaration) to work around a Scala compiler
  // specialization bug that would generate two arrays (one for Object and one for specialized T).
  protected var _data: Array[T] = _
  _data = new Array[T](_capacity) //使用数组存储数据

  /** Number of elements in the set.集合的元素数量 */
  def size: Int = _size

  /** The capacity of the set (i.e. size of the underlying array).元素的容量 */
  def capacity: Int = _capacity

  /** Return true if this set contains the specified element. */
  def contains(k: T): Boolean = getPos(k) != INVALID_POS

  /**
   * Add an element to the set. If the set is over capacity after the insertion, grow the set
   * and rehash all elements.
   */
  def add(k: T) {
    addWithoutResize(k) //将K存储到数组中
    rehashIfNeeded(k, grow, move) //是否扩容
  }

  def union(other: OpenHashSet[T]): OpenHashSet[T] = {
    val iterator = other.iterator
    while (iterator.hasNext) {
      add(iterator.next())
    }
    this
  }

  /**
   * Add an element to the set. This one differs from add in that it doesn't trigger rehashing.
   * The caller is responsible for calling rehashIfNeeded.
   *
   * Use (retval & POSITION_MASK) to get the actual position, and
   * (retval & NONEXISTENCE_MASK) == 0 for prior existence.
   *
   * @return The position where the key is placed, plus the highest order bit is set if the key
   *         does not exists previously.
   * 不扩容的前提下,找到K应该存储在哪个数组的位置,并且将key存储在数组该位置中
   */
  def addWithoutResize(k: T): Int = {
    var pos = hashcode(hasher.hash(k)) & _mask //获取该元素对应数组的哪个位置
    var delta = 1
    while (true) {
      if (!_bitset.get(pos)) { //说明该位置没有被存储,即该位置是空位置
        // This is a new key.
        _data(pos) = k //数组的内容存储为key
        _bitset.set(pos)
        _size += 1
        return pos | NONEXISTENCE_MASK //返回该K存储在数组的位置
      } else if (_data(pos) == k) {//说明该位置已经存在值了,那么对比是否是K,
        // Found an existing key. 说明该值已经存储了,则直接返回该Key存储的位置
        return pos
      } else {//说明该位置被存储了,但不是K
        // quadratic probing with values increase by 1, 2, 3, ...
        pos = (pos + delta) & _mask
        delta += 1
      }
    }
    throw new RuntimeException("Should never reach here.") //应该不会到达这个函数位置,因为while循环没有break代码
  }

  /**
   * Rehash the set if it is overloaded.
   * @param k A parameter unused in the function, but to force the Scala compiler to specialize
   *          this method.
   * @param allocateFunc Callback invoked when we are allocating a new, larger array.
   * @param moveFunc Callback invoked when we move the key from one position (in the old data array)
   *                 to a new position (in the new data array).
   * 如果要扩容,则扩容
   */
  def rehashIfNeeded(k: T, allocateFunc: (Int) => Unit, moveFunc: (Int, Int) => Unit) {
    if (_size > _growThreshold) { //说明需要扩容了
      rehash(k, allocateFunc, moveFunc)
    }
  }

  /**
   * Return the position of the element in the underlying array, or INVALID_POS if it is not found.
   * 返回K所在的位置,如果不存在,则返回-1
   */
  def getPos(k: T): Int = {
    var pos = hashcode(hasher.hash(k)) & _mask
    var delta = 1
    while (true) {
      if (!_bitset.get(pos)) {//说明不存在,则返回-1
        return INVALID_POS
      } else if (k == _data(pos)) {//说明该位置有,并且是K,则返回该位置
        return pos
      } else {//说明位置有值,但不是key,则寻找下一个位置
        // quadratic probing with values increase by 1, 2, 3, ...
        pos = (pos + delta) & _mask
        delta += 1
      }
    }
    throw new RuntimeException("Should never reach here.") //永远不会到达该位置
  }

  /** Return the value at the specified position.
    * 获取该位置的值
    **/
  def getValue(pos: Int): T = _data(pos)

  def iterator: Iterator[T] = new Iterator[T] {
    var pos = nextPos(0)
    override def hasNext: Boolean = pos != INVALID_POS
    override def next(): T = {
      val tmp = getValue(pos)
      pos = nextPos(pos + 1)
      tmp
    }
  }

  /** Return the value at the specified position
    *  安全的获取值,即确保该位置一定是有值的
    **/
  def getValueSafe(pos: Int): T = {
    assert(_bitset.get(pos))
    _data(pos)
  }

  /**
   * Return the next position with an element stored, starting from the given position inclusively.
   * 下一个数组有值的位置
   */
  def nextPos(fromPos: Int): Int = _bitset.nextSetBit(fromPos)

  /**
   * Double the table's size and re-hash everything. We are not really using k, but it is declared
   * so Scala compiler can specialize this method (which leads to calling the specialized version
   * of putInto).
   *
   * @param k A parameter unused in the function, but to force the Scala compiler to specialize
   *          this method.
   * @param allocateFunc Callback invoked when we are allocating a new, larger array.
   * @param moveFunc Callback invoked when we move the key from one position (in the old data array)
   *                 to a new position (in the new data array).
   * 扩容
   */
  private def rehash(k: T, allocateFunc: (Int) => Unit, moveFunc: (Int, Int) => Unit) {
    val newCapacity = _capacity * 2
    require(newCapacity > 0 && newCapacity <= OpenHashSet.MAX_CAPACITY,
      s"Can't contain more than ${(loadFactor * OpenHashSet.MAX_CAPACITY).toInt} elements")
    allocateFunc(newCapacity) //将新的容量进行通知

    //获取新的容量bitset对象和新的数组
    val newBitset = new BitSet(newCapacity)
    val newData = new Array[T](newCapacity)
    val newMask = newCapacity - 1

    var oldPos = 0
    while (oldPos < capacity) {//循环老的数组
      if (_bitset.get(oldPos)) {//如果该位置有值,则进行处理
        val key = _data(oldPos) //获取值
        var newPos = hashcode(hasher.hash(key)) & newMask //将老的值存储到新的值
        var i = 1
        var keepGoing = true
        // No need to check for equality here when we insert so this has one less if branch than
        // the similar code path in addWithoutResize.
        while (keepGoing) {
          if (!newBitset.get(newPos)) {
            // Inserting the key at newPos
            newData(newPos) = key
            newBitset.set(newPos)
            moveFunc(oldPos, newPos)
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

    _bitset = newBitset
    _data = newData
    _capacity = newCapacity
    _mask = newMask
    _growThreshold = (loadFactor * newCapacity).toInt
  }

  /**
   * Re-hash a value to deal better with hash functions that don't differ in the lower bits.
   */
  private def hashcode(h: Int): Int = Hashing.murmur3_32().hashInt(h).asInt()

  private def nextPowerOf2(n: Int): Int = {
    val highBit = Integer.highestOneBit(n)
    if (highBit == n) n else highBit << 1
  }
}


private[spark]
object OpenHashSet {

  val MAX_CAPACITY = 1 << 30
  val INVALID_POS = -1 //空数组的位置存储的是-1
  val NONEXISTENCE_MASK = 1 << 31
  val POSITION_MASK = (1 << 31) - 1

  /**
   * A set of specialized hash function implementation to avoid boxing hash code computation
   * in the specialized implementation of OpenHashSet.
   */
  sealed class Hasher[@specialized(Long, Int) T] extends Serializable {
    def hash(o: T): Int = o.hashCode()
  }

  class LongHasher extends Hasher[Long] {
    override def hash(o: Long): Int = (o ^ (o >>> 32)).toInt
  }

  class IntHasher extends Hasher[Int] {
    override def hash(o: Int): Int = o
  }

  private def grow1(newSize: Int) {} //当扩容时,产生新的容量大小的时候,要进行回调通知,即参数就是新的容量
  private def move1(oldPos: Int, newPos: Int) { } //当产生扩容的时候,将老的位置的数据 添加到新的数组中,新的数组的位置是newPos,该函数也是一个回调函数

  private val grow = grow1 _
  private val move = move1 _
}
