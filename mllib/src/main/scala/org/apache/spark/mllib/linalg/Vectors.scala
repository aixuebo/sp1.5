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

package org.apache.spark.mllib.linalg

import java.util
import java.lang.{Double => JavaDouble, Integer => JavaInteger, Iterable => JavaIterable}

import scala.annotation.varargs
import scala.collection.JavaConverters._

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}

import org.apache.spark.SparkException
import org.apache.spark.annotation.{AlphaComponent, Since}
import org.apache.spark.mllib.util.NumericParser
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._

/**
 * Represents a numeric vector, whose index type is Int and value type is Double.
 *
 * Note: Users should not implement this interface.
 */
@SQLUserDefinedType(udt = classOf[VectorUDT])
@Since("1.0.0")
sealed trait Vector extends Serializable {

  /**
   * Size of the vector.
   */
  @Since("1.0.0")
  def size: Int

  /**
   * Converts the instance to a double array.
   */
  @Since("1.0.0")
  def toArray: Array[Double]

  override def equals(other: Any): Boolean = {
    other match {
      case v2: Vector =>
        if (this.size != v2.size) return false
        (this, v2) match {
          case (s1: SparseVector, s2: SparseVector) =>
            Vectors.equals(s1.indices, s1.values, s2.indices, s2.values)
          case (s1: SparseVector, d1: DenseVector) =>
            Vectors.equals(s1.indices, s1.values, 0 until d1.size, d1.values)
          case (d1: DenseVector, s1: SparseVector) =>
            Vectors.equals(0 until d1.size, d1.values, s1.indices, s1.values)
          case (_, _) => util.Arrays.equals(this.toArray, v2.toArray)
        }
      case _ => false
    }
  }

  /**
   * Returns a hash code value for the vector. The hash code is based on its size and its nonzeros
   * in the first 16 entries, using a hash algorithm similar to [[java.util.Arrays.hashCode]].
   */
  override def hashCode(): Int = {
    // This is a reference implementation. It calls return in foreachActive, which is slow.
    // Subclasses should override it with optimized implementation.
    var result: Int = 31 + size
    this.foreachActive { (index, value) =>
      if (index < 16) {
        // ignore explicit 0 for comparison between sparse and dense
        if (value != 0) {
          result = 31 * result + index
          val bits = java.lang.Double.doubleToLongBits(value)
          result = 31 * result + (bits ^ (bits >>> 32)).toInt
        }
      } else {
        return result
      }
    }
    result
  }

  /**
   * Converts the instance to a breeze vector.
   */
  private[spark] def toBreeze: BV[Double]

  /**
   * Gets the value of the ith element.
   * @param i index 获取向量的某一个维度值
   */
  @Since("1.1.0")
  def apply(i: Int): Double = toBreeze(i)

  /**
   * Makes a deep copy of this vector.
   */
  @Since("1.1.0")
  def copy: Vector = {
    throw new NotImplementedError(s"copy is not implemented for ${this.getClass}.")
  }

  /**
   * Applies a function `f` to all the active elements of dense and sparse vector.
   *
   * @param f the function takes two parameters where the first parameter is the index of
   *          the vector with type `Int`, and the second parameter is the corresponding value
   *          with type `Double`.
    * 循环向量的每一个元素.参数是元素位置和元素内容
   */
  private[spark] def foreachActive(f: (Int, Double) => Unit)

  /**
   * Number of active entries.  An "active entry" is an element which is explicitly stored,
   * regardless of its value.  Note that inactive entries have value 0.
    * value的个数
    * 密集向量，该值就是 size
    * 稀疏向量, 该值是 value的size
   */
  @Since("1.4.0")
  def numActives: Int

  /**
   * Number of nonzero elements. This scans all active values and count nonzeros.
    * 非0的value的个数
    * 密集向量，该值就是向量中元素不是0的个数
    * 稀疏向量, 该值是value的中非0的元素个数
   */
  @Since("1.4.0")
  def numNonzeros: Int

  /**
   * Converts this vector to a sparse vector with all explicit zeros removed.
    * 移除所有0的元素,转换成稀松向量
   */
  @Since("1.4.0")
  def toSparse: SparseVector

  /**
   * Converts this vector to a dense vector.
    * 转换成密集向量
   */
  @Since("1.4.0")
  def toDense: DenseVector = new DenseVector(this.toArray)

  /**
   * Returns a vector in either dense or sparse format, whichever uses less storage.
    * 对向量进行压缩
    * 如果非0向量很少,那还是做密集向量,如果非0向量很多,则做稀松向量
   */
  @Since("1.4.0")
  def compressed: Vector = {
    val nnz = numNonzeros
    // A dense vector needs 8 * size + 8 bytes, while a sparse vector needs 12 * nnz + 20 bytes.
    if (1.5 * (nnz + 1.0) < size) { //非0元素的1.5倍 不足向量空间，因此说明0还是很多，用稀疏向量
      toSparse
    } else {
      toDense
    }
  }

  /**
   * Find the index of a maximal element.  Returns the first maximal element in case of a tie.
   * Returns -1 if vector has length 0.
    * 最大元素值对应的向量索引位置
   */
  @Since("1.5.0")
  def argmax: Int
}

/**
 * :: AlphaComponent ::
 *
 * User-defined type for [[Vector]] which allows easy interaction with SQL
 * via [[org.apache.spark.sql.DataFrame]].
  * 自定义一个向量类型
 */
@AlphaComponent
class VectorUDT extends UserDefinedType[Vector] {

  override def sqlType: StructType = {
    // type: 0 = sparse, 1 = dense
    // We only use "values" for dense vectors, and "size", "indices", and "values" for sparse
    // vectors. The "values" field is nullable because we might want to add binary vectors later,
    // which uses "size" and "indices", but not "values".
    //秘籍向量时，仅仅使用value属性即可
    StructType(Seq(
      StructField("type", ByteType, nullable = false),//向量类型 0表示稀松向量  1表示密集向量
      StructField("size", IntegerType, nullable = true),//向量总大小
      StructField("indices", ArrayType(IntegerType, containsNull = false), nullable = true),//数组--表示哪些位置非0
      StructField("values", ArrayType(DoubleType, containsNull = false), nullable = true)))//数组--表示非0位置具体的值
  }

  override def serialize(obj: Any): InternalRow = {
    obj match {
      case SparseVector(size, indices, values) =>
        val row = new GenericMutableRow(4)
        row.setByte(0, 0)
        row.setInt(1, size)
        row.update(2, new GenericArrayData(indices.map(_.asInstanceOf[Any])))
        row.update(3, new GenericArrayData(values.map(_.asInstanceOf[Any])))
        row
      case DenseVector(values) =>
        val row = new GenericMutableRow(4)
        row.setByte(0, 1)
        row.setNullAt(1)//设置为null
        row.setNullAt(2)//设置为null
        row.update(3, new GenericArrayData(values.map(_.asInstanceOf[Any])))
        row
    }
  }

  override def deserialize(datum: Any): Vector = {
    datum match {
      case row: InternalRow =>
        require(row.numFields == 4,
          s"VectorUDT.deserialize given row with length ${row.numFields} but requires length == 4")
        val tpe = row.getByte(0) //判断向量类型
        tpe match {
          case 0 => //稀松
            val size = row.getInt(1)
            val indices = row.getArray(2).toIntArray()
            val values = row.getArray(3).toDoubleArray()
            new SparseVector(size, indices, values)
          case 1 => //密集
            val values = row.getArray(3).toDoubleArray()
            new DenseVector(values)
        }
    }
  }

  override def pyUDT: String = "pyspark.mllib.linalg.VectorUDT"

  override def userClass: Class[Vector] = classOf[Vector]

  override def equals(o: Any): Boolean = {
    o match {
      case v: VectorUDT => true
      case _ => false
    }
  }

  // see [SPARK-8647], this achieves the needed constant hash code without constant no.
  override def hashCode(): Int = classOf[VectorUDT].getName.hashCode()

  override def typeName: String = "vector"

  private[spark] override def asNullable: VectorUDT = this
}

/**
 * Factory methods for [[org.apache.spark.mllib.linalg.Vector]].
 * We don't use the name `Vector` because Scala imports
 * [[scala.collection.immutable.Vector]] by default.
 */
@Since("1.0.0")
object Vectors {

  /**
   * Creates a dense vector from its values.
    * 密集向量
   */
  @Since("1.0.0")
  @varargs
  def dense(firstValue: Double, otherValues: Double*): Vector =
    new DenseVector((firstValue +: otherValues).toArray)

  // A dummy implicit is used to avoid signature collision with the one generated by @varargs.
  /**
   * Creates a dense vector from a double array.
   */
  @Since("1.0.0")
  def dense(values: Array[Double]): Vector = new DenseVector(values)

  /**
   * Creates a sparse vector providing its index array and value array.
   *
   * @param size vector size.
   * @param indices index array, must be strictly increasing.
   * @param values value array, must have the same length as indices.
    * 稀疏向量
   */
  @Since("1.0.0")
  def sparse(size: Int, indices: Array[Int], values: Array[Double]): Vector =
    new SparseVector(size, indices, values)

  /**
   * Creates a sparse vector using unordered (index, value) pairs.
   *
   * @param size vector size.
   * @param elements vector elements in (index, value) pairs. 向量的第几个元素是非0,添加此集合
   */
  @Since("1.0.0")
  def sparse(size: Int, elements: Seq[(Int, Double)]): Vector = {
    require(size > 0, "The size of the requested sparse vector must be greater than 0.")

    val (indices, values) = elements.sortBy(_._1).unzip //先按照序号排序,拆分成2部分
    var prev = -1
    indices.foreach { i =>  //确保顺序
      require(prev < i, s"Found duplicate indices: $i.")
      prev = i
    }
    require(prev < size, s"You may not write an element to index $prev because the declared " +
      s"size of your vector is $size")

    new SparseVector(size, indices.toArray, values.toArray)
  }

  /**
   * Creates a sparse vector using unordered (index, value) pairs in a Java friendly way.
   *
   * @param size vector size.
   * @param elements vector elements in (index, value) pairs.
   */
  @Since("1.0.0")
  def sparse(size: Int, elements: JavaIterable[(JavaInteger, JavaDouble)]): Vector = {
    sparse(size, elements.asScala.map { case (i, x) =>
      (i.intValue(), x.doubleValue())
    }.toSeq)
  }

  /**
   * Creates a vector of all zeros.
   *
   * @param size vector size
   * @return a zero vector
   */
  @Since("1.1.0")
  def zeros(size: Int): Vector = {
    new DenseVector(new Array[Double](size))
  }

  /**
   * Parses a string resulted from [[Vector.toString]] into a [[Vector]].
   */
  @Since("1.1.0")
  def parse(s: String): Vector = {
    parseNumeric(NumericParser.parse(s))
  }

  private[mllib] def parseNumeric(any: Any): Vector = {
    any match {
      case values: Array[Double] =>
        Vectors.dense(values)
      case Seq(size: Double, indices: Array[Double], values: Array[Double]) =>
        Vectors.sparse(size.toInt, indices.map(_.toInt), values)
      case other =>
        throw new SparkException(s"Cannot parse $other.")
    }
  }

  /**
   * Creates a vector instance from a breeze vector.
   * 转换成向量
   */
  private[spark] def fromBreeze(breezeVector: BV[Double]): Vector = {
    breezeVector match {
      case v: BDV[Double] => //D说明是密集向量
        if (v.offset == 0 && v.stride == 1 && v.length == v.data.length) {
          new DenseVector(v.data)
        } else {
          new DenseVector(v.toArray)  // Can't use underlying array directly, so make a new one
        }
      case v: BSV[Double] => //S说明是稀松向量
        if (v.index.length == v.used) {
          new SparseVector(v.length, v.index, v.data)
        } else {
          new SparseVector(v.length, v.index.slice(0, v.used), v.data.slice(0, v.used))
        }
      case v: BV[_] =>
        sys.error("Unsupported Breeze vector type: " + v.getClass.getName)
    }
  }

  /**
   * Returns the p-norm of this vector.
   * @param vector input vector.
   * @param p norm.
   * @return norm in L^p^ space.
   */
  @Since("1.3.0")
  def norm(vector: Vector, p: Double): Double = {
    require(p >= 1.0, "To compute the p-norm of the vector, we require that you specify a p>=1. " +
      s"You specified p=$p.")
    val values = vector match {
      case DenseVector(vs) => vs
      case SparseVector(n, ids, vs) => vs
      case v => throw new IllegalArgumentException("Do not support vector type " + v.getClass)
    }
    val size = values.length //向量元素数量

    if (p == 1) {//向量元素绝对值之和
      var sum = 0.0
      var i = 0
      while (i < size) {//迭代每一个向量元素
        sum += math.abs(values(i))
        i += 1
      }
      sum
    } else if (p == 2) {//向量元素平方之和--开根号
      var sum = 0.0
      var i = 0
      while (i < size) {
        sum += values(i) * values(i)
        i += 1
      }
      math.sqrt(sum) //开根号
    } else if (p == Double.PositiveInfinity) {//向量元素绝对值最大的元素---正无穷
      var max = 0.0
      var i = 0
      while (i < size) {
        val value = math.abs(values(i))
        if (value > max) max = value
        i += 1
      }
      max
    } else {//向量元素^n之和
      var sum = 0.0
      var i = 0
      while (i < size) {
        sum += math.pow(math.abs(values(i)), p) //向量元素^n之和
        i += 1
      }
      math.pow(sum, 1.0 / p) //向量元素绝对值的p次方和的1/p次幂
    }
  }

  /**
   * Returns the squared distance between two Vectors.
    * (向量1-向量2)^2和
   * @param v1 first Vector.
   * @param v2 second Vector.
   * @return squared distance between two Vectors.
   */
  @Since("1.3.0")
  def sqdist(v1: Vector, v2: Vector): Double = {
    require(v1.size == v2.size, s"Vector dimensions do not match: Dim(v1)=${v1.size} and Dim(v2)" +
      s"=${v2.size}.")
    var squaredDistance = 0.0
    (v1, v2) match {
      case (v1: SparseVector, v2: SparseVector) =>
        val v1Values = v1.values
        val v1Indices = v1.indices
        val v2Values = v2.values
        val v2Indices = v2.indices
        val nnzv1 = v1Indices.length
        val nnzv2 = v2Indices.length

        var kv1 = 0 //第一个向量指针
        var kv2 = 0 //第二个向量指针
        while (kv1 < nnzv1 || kv2 < nnzv2) {
          var score = 0.0

          if (kv2 >= nnzv2 || (kv1 < nnzv1 && v1Indices(kv1) < v2Indices(kv2))) {
            score = v1Values(kv1)
            kv1 += 1
          } else if (kv1 >= nnzv1 || (kv2 < nnzv2 && v2Indices(kv2) < v1Indices(kv1))) {
            score = v2Values(kv2)
            kv2 += 1
          } else {
            score = v1Values(kv1) - v2Values(kv2)
            kv1 += 1
            kv2 += 1
          }
          squaredDistance += score * score
        }

      case (v1: SparseVector, v2: DenseVector) =>
        squaredDistance = sqdist(v1, v2)

      case (v1: DenseVector, v2: SparseVector) =>
        squaredDistance = sqdist(v2, v1)

      case (DenseVector(vv1), DenseVector(vv2)) =>
        var kv = 0
        val sz = vv1.length
        while (kv < sz) {
          val score = vv1(kv) - vv2(kv)
          squaredDistance += score * score
          kv += 1
        }
      case _ =>
        throw new IllegalArgumentException("Do not support vector type " + v1.getClass +
          " and " + v2.getClass)
    }
    squaredDistance
  }

  /**
   * Returns the squared distance between DenseVector and SparseVector.
    * (向量1-向量2)^2和
   */
  private[mllib] def sqdist(v1: SparseVector, v2: DenseVector): Double = {
    var kv1 = 0 //向量1的下标
    var kv2 = 0 //向量2的下标
    val indices = v1.indices //向量1稀疏向量的非0点下标
    var squaredDistance = 0.0
    val nnzv1 = indices.length //向量1有多少个非0点
    val nnzv2 = v2.size //向量2的全部点
    var iv1 = if (nnzv1 > 0) indices(kv1) else -1 //向量1的第一个非0点下标序号，如果没有则-1

    while (kv2 < nnzv2) { //循环向量2
      var score = 0.0
      if (kv2 != iv1) { //说明不是同一个位置
        score = v2(kv2) //只获取向量2的
      } else {//说明两个向量位置是相同的
        score = v1.values(kv1) - v2(kv2) //减法
        if (kv1 < nnzv1 - 1) { //更新第一个向量的下一个非0的位置
          kv1 += 1
          iv1 = indices(kv1)
        }
      }
      squaredDistance += score * score
      kv2 += 1
    }
    squaredDistance
  }

  /**
   * Check equality between sparse/dense vectors
   */
  private[mllib] def equals(
      v1Indices: IndexedSeq[Int],
      v1Values: Array[Double],
      v2Indices: IndexedSeq[Int],
      v2Values: Array[Double]): Boolean = {
    val v1Size = v1Values.length
    val v2Size = v2Values.length
    var k1 = 0
    var k2 = 0
    var allEqual = true
    while (allEqual) {
      while (k1 < v1Size && v1Values(k1) == 0) k1 += 1
      while (k2 < v2Size && v2Values(k2) == 0) k2 += 1

      if (k1 >= v1Size || k2 >= v2Size) {
        return k1 >= v1Size && k2 >= v2Size // check end alignment
      }
      allEqual = v1Indices(k1) == v2Indices(k2) && v1Values(k1) == v2Values(k2)
      k1 += 1
      k2 += 1
    }
    allEqual
  }
}

/**
 * A dense vector represented by a value array.
  * 密集向量
 */
@Since("1.0.0")
@SQLUserDefinedType(udt = classOf[VectorUDT])
class DenseVector @Since("1.0.0") (
    @Since("1.0.0") val values: Array[Double]) extends Vector {

  @Since("1.0.0")
  override def size: Int = values.length

  override def toString: String = values.mkString("[", ",", "]")

  @Since("1.0.0")
  override def toArray: Array[Double] = values

  private[spark] override def toBreeze: BV[Double] = new BDV[Double](values)

  @Since("1.0.0")
  override def apply(i: Int): Double = values(i)

  @Since("1.1.0")
  override def copy: DenseVector = {
    new DenseVector(values.clone())
  }

  private[spark] override def foreachActive(f: (Int, Double) => Unit) = {
    var i = 0
    val localValuesSize = values.length
    val localValues = values

    while (i < localValuesSize) {
      f(i, localValues(i))
      i += 1
    }
  }

  override def hashCode(): Int = {
    var result: Int = 31 + size
    var i = 0
    val end = math.min(values.length, 16)
    while (i < end) {
      val v = values(i)
      if (v != 0.0) {
        result = 31 * result + i
        val bits = java.lang.Double.doubleToLongBits(values(i))
        result = 31 * result + (bits ^ (bits >>> 32)).toInt
      }
      i += 1
    }
    result
  }

  @Since("1.4.0")
  override def numActives: Int = size

  @Since("1.4.0")
  override def numNonzeros: Int = {
    // same as values.count(_ != 0.0) but faster
    var nnz = 0
    values.foreach { v =>
      if (v != 0.0) {
        nnz += 1
      }
    }
    nnz
  }

  @Since("1.4.0")
  override def toSparse: SparseVector = {
    val nnz = numNonzeros
    val ii = new Array[Int](nnz)
    val vv = new Array[Double](nnz)
    var k = 0
    foreachActive { (i, v) =>
      if (v != 0) {
        ii(k) = i
        vv(k) = v
        k += 1
      }
    }
    new SparseVector(size, ii, vv)
  }

  //最大元素值对应的向量索引位置
  @Since("1.5.0")
  override def argmax: Int = {
    if (size == 0) {
      -1
    } else {
      var maxIdx = 0
      var maxValue = values(0)
      var i = 1
      while (i < size) {
        if (values(i) > maxValue) {
          maxIdx = i
          maxValue = values(i)
        }
        i += 1
      }
      maxIdx
    }
  }
}

@Since("1.3.0")
object DenseVector {

  /** Extracts the value array from a dense vector.
    * 返回密度向量中的元素数组集合
    **/
  @Since("1.3.0")
  def unapply(dv: DenseVector): Option[Array[Double]] = Some(dv.values)
}

/**
 * A sparse vector represented by an index array and an value array.
 *
 * @param size size of the vector.
 * @param indices index array, assume to be strictly increasing.
 * @param values value array, must have the same length as the index array.
  * 稀疏向量
 */
@Since("1.0.0")
@SQLUserDefinedType(udt = classOf[VectorUDT])
class SparseVector @Since("1.0.0") (
    @Since("1.0.0") override val size: Int,//向量包含多少个元素,包含0的元素
    @Since("1.0.0") val indices: Array[Int], //非0元素所在的索引位置
    @Since("1.0.0") val values: Array[Double]) //真正非0的元素值
   extends Vector {

  require(indices.length == values.length, "Sparse vectors require that the dimension of the" +
    s" indices match the dimension of the values. You provided ${indices.length} indices and " +
    s" ${values.length} values.")
  require(indices.length <= size, s"You provided ${indices.length} indices and values, " +
    s"which exceeds the specified vector size ${size}.")

  override def toString: String =
    s"($size,${indices.mkString("[", ",", "]")},${values.mkString("[", ",", "]")})"

  //返回全部向量内容,包括0的元素
  @Since("1.0.0")
  override def toArray: Array[Double] = {
    val data = new Array[Double](size)
    var i = 0
    val nnz = indices.length
    while (i < nnz) {
      data(indices(i)) = values(i)
      i += 1
    }
    data
  }

  @Since("1.1.0")
  override def copy: SparseVector = {
    new SparseVector(size, indices.clone(), values.clone())
  }

  private[spark] override def toBreeze: BV[Double] = new BSV[Double](indices, values, size)

  //真对有value的内容进行f函数处理
  private[spark] override def foreachActive(f: (Int, Double) => Unit) = {
    var i = 0
    val localValuesSize = values.length
    val localIndices = indices
    val localValues = values

    while (i < localValuesSize) {
      f(localIndices(i), localValues(i))
      i += 1
    }
  }

  override def hashCode(): Int = {
    var result: Int = 31 + size
    val end = values.length
    var continue = true
    var k = 0
    while ((k < end) & continue) {
      val i = indices(k)
      if (i < 16) {
        val v = values(k)
        if (v != 0.0) {
          result = 31 * result + i
          val bits = java.lang.Double.doubleToLongBits(v)
          result = 31 * result + (bits ^ (bits >>> 32)).toInt
        }
      } else {
        continue = false
      }
      k += 1
    }
    result
  }

  //有value的数据长度
  @Since("1.4.0")
  override def numActives: Int = values.length

  //非0的元素个数
  @Since("1.4.0")
  override def numNonzeros: Int = {
    var nnz = 0
    values.foreach { v =>
      if (v != 0.0) {
        nnz += 1
      }
    }
    nnz
  }

  @Since("1.4.0")
  override def toSparse: SparseVector = {
    val nnz = numNonzeros //非0的元素数量
    if (nnz == numActives) {//说明所有有效的元素都是非0的
      this
    } else {
      val ii = new Array[Int](nnz)
      val vv = new Array[Double](nnz)
      var k = 0
      foreachActive { (i, v) =>
        if (v != 0.0) {
          ii(k) = i
          vv(k) = v
          k += 1
        }
      }
      new SparseVector(size, ii, vv)
    }
  }

  @Since("1.5.0")
  override def argmax: Int = {
    if (size == 0) {
      -1
    } else {
      // Find the max active entry.
      //初始化最大的元素就是第一个元素
      var maxIdx = indices(0)
      var maxValue = values(0)
      var maxJ = 0 //最大值在value集合中的索引位置

      var j = 1//从1开始循环,循环所有有效的value
      val na = numActives //所有活跃的value元素
      while (j < na) {
        val v = values(j)//获取该有效值
        if (v > maxValue) {//说明该有效值此时最大
          maxValue = v //设置最大值
          maxIdx = indices(j) //设置最大值下标
          maxJ = j //设置最大值在value集合中的索引位置
        }
        j += 1
      }

      // If the max active entry is nonpositive and there exists inactive ones, find the first zero.
      if (maxValue <= 0.0 && na < size) {//说明最大的value是负数或者0,但是还有0的元素
        if (maxValue == 0.0) {//说明最大值是0
          // If there exists an inactive entry before maxIdx, find it and return its index.
          if (maxJ < maxIdx) {
            var k = 0
            while (k < maxJ && indices(k) == k) {
              k += 1
            }
            maxIdx = k
          }
        } else {
          // If the max active value is negative, find and return the first inactive index.
          var k = 0
          while (k < na && indices(k) == k) {
            k += 1
          }
          maxIdx = k
        }
      }

      maxIdx
    }
  }

  /**
   * Create a slice of this vector based on the given indices.
   * @param selectedIndices Unsorted list of indices into the vector.
   *                        This does NOT do bound checking.
   * @return  New SparseVector with values in the order specified by the given indices.
   *
   * NOTE: The API needs to be discussed before making this public.
   *       Also, if we have a version assuming indices are sorted, we should optimize it.
    * 对向量进行抽取，只要向量的某些位置的值，组成一个新向量
   */
  private[spark] def slice(selectedIndices: Array[Int]): SparseVector = {
    var currentIdx = 0 //新向量有值的节点下标就是0 1 2 累加的连续值
    val (sliceInds, sliceVals) = selectedIndices.flatMap { origIdx =>
      val iIdx = java.util.Arrays.binarySearch(this.indices, origIdx)
      val i_v = if (iIdx >= 0) {
        Iterator((currentIdx, this.values(iIdx)))
      } else {
        Iterator()
      }
      currentIdx += 1
      i_v
    }.unzip
    new SparseVector(selectedIndices.length, sliceInds.toArray, sliceVals.toArray)
  }
}

//返回元素个数、有值得元素位置索引,有值得元素数组
@Since("1.3.0")
object SparseVector {//稀疏向量
  @Since("1.3.0")
  def unapply(sv: SparseVector): Option[(Int, Array[Int], Array[Double])] =
    Some((sv.size, sv.indices, sv.values))
}
