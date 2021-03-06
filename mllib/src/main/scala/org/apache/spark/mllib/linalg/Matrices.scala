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

import java.util.{Arrays, Random}

import scala.collection.mutable.{ArrayBuilder => MArrayBuilder, HashSet => MHashSet, ArrayBuffer}

import breeze.linalg.{CSCMatrix => BSM, DenseMatrix => BDM, Matrix => BM}

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

/**
 * Trait for a local matrix.
  * 本地的一个矩阵
 */
@SQLUserDefinedType(udt = classOf[MatrixUDT])
@Since("1.0.0")
sealed trait Matrix extends Serializable {

  /** Number of rows. */
  @Since("1.0.0")
  def numRows: Int

  /** Number of columns. */
  @Since("1.0.0")
  def numCols: Int

  /** Flag that keeps track whether the matrix is transposed or not. False by default. 是否支持转置*/
  @Since("1.3.0")
  val isTransposed: Boolean = false

  /** Converts to a dense array in column major. 按照列向量的方式存储矩阵*/
  @Since("1.0.0")
  def toArray: Array[Double] = {
    val newArray = new Array[Double](numRows * numCols)
    foreachActive { (i, j, v) =>
      newArray(j * numRows + i) = v
    }
    newArray
  }

  /** Converts to a breeze matrix. */
  private[mllib] def toBreeze: BM[Double]

  /** Gets the (i, j)-th element. 获取矩阵中的值*/
  @Since("1.3.0")
  def apply(i: Int, j: Int): Double

  /** Return the index for the (i, j)-th element in the backing array. */
  private[mllib] def index(i: Int, j: Int): Int

  /** Update element at (i, j) */
  private[mllib] def update(i: Int, j: Int, v: Double): Unit

  /** Get a deep copy of the matrix. */
  @Since("1.2.0")
  def copy: Matrix

  /** Transpose the Matrix. Returns a new `Matrix` instance sharing the same underlying data.
    * 转置
    **/
  @Since("1.3.0")
  def transpose: Matrix

  /** Convenience method for `Matrix`-`DenseMatrix` multiplication. 矩阵乘法*/
  @Since("1.2.0")
  def multiply(y: DenseMatrix): DenseMatrix = {
    val C: DenseMatrix = DenseMatrix.zeros(numRows, y.numCols)
    BLAS.gemm(1.0, this, y, 0.0, C)
    C
  }

  /** Convenience method for `Matrix`-`DenseVector` multiplication. For binary compatibility. */
  @Since("1.2.0")
  def multiply(y: DenseVector): DenseVector = {
    multiply(y.asInstanceOf[Vector])
  }

  /** Convenience method for `Matrix`-`Vector` multiplication. */
  @Since("1.4.0")
  def multiply(y: Vector): DenseVector = {
    val output = new DenseVector(new Array[Double](numRows)) //多少行,说明多少个lable,返回一个所有label组成的向量
    BLAS.gemv(1.0, this, y, 0.0, output)
    output
  }

  /** A human readable representation of the matrix */
  override def toString: String = toBreeze.toString()

  /** A human readable representation of the matrix with maximum lines and width */
  @Since("1.4.0")
  def toString(maxLines: Int, maxLineWidth: Int): String = toBreeze.toString(maxLines, maxLineWidth)

  /** Map the values of this matrix using a function. Generates a new matrix. Performs the
    * function on only the backing array. For example, an operation such as addition or
    * subtraction will only be performed on the non-zero values in a `SparseMatrix`.
    * 对每一个元素值进行更新
    **/
  private[spark] def map(f: Double => Double): Matrix

  /** Update all the values of this matrix using the function f. Performed in-place on the
    * backing array. For example, an operation such as addition or subtraction will only be
    * performed on the non-zero values in a `SparseMatrix`. */
  private[mllib] def update(f: Double => Double): Matrix

  /**
   * Applies a function `f` to all the active elements of dense and sparse matrix. The ordering
   * of the elements are not defined.
   *
   * @param f the function takes three parameters where the first two parameters are the row
   *          and column indices respectively with the type `Int`, and the final parameter is the
   *          corresponding value in the matrix with type `Double`.
   */
  private[spark] def foreachActive(f: (Int, Int, Double) => Unit)

  /**
   * Find the number of non-zero active values.
   */
  @Since("1.5.0")
  def numNonzeros: Int

  /**
   * Find the number of values stored explicitly. These values can be zero as well.
   */
  @Since("1.5.0")
  def numActives: Int
}

//自定义sql的数据类型--矩阵类型
@DeveloperApi
private[spark] class MatrixUDT extends UserDefinedType[Matrix] {

  override def sqlType: StructType = {
    // type: 0 = sparse, 1 = dense
    // the dense matrix is built by numRows, numCols, values and isTransposed, all of which are
    // set as not nullable, except values since in the future, support for binary matrices might
    // be added for which values are not needed.
    // the sparse matrix needs colPtrs and rowIndices, which are set as
    // null, while building the dense matrix.
    StructType(Seq(
      StructField("type", ByteType, nullable = false),//0表示稀松 1表示密集
      StructField("numRows", IntegerType, nullable = false),//行数
      StructField("numCols", IntegerType, nullable = false),//列数
      StructField("colPtrs", ArrayType(IntegerType, containsNull = false), nullable = true),//用于稀松矩阵
      StructField("rowIndices", ArrayType(IntegerType, containsNull = false), nullable = true),//用于稀松矩阵
      StructField("values", ArrayType(DoubleType, containsNull = false), nullable = true),//存储所有的矩阵的值
      StructField("isTransposed", BooleanType, nullable = false) //矩阵是行存储还是列存储
      ))
  }

  //序列化
  override def serialize(obj: Any): InternalRow = {
    val row = new GenericMutableRow(7)
    obj match {
      case sm: SparseMatrix =>
        row.setByte(0, 0)
        row.setInt(1, sm.numRows)
        row.setInt(2, sm.numCols)
        row.update(3, new GenericArrayData(sm.colPtrs.map(_.asInstanceOf[Any])))
        row.update(4, new GenericArrayData(sm.rowIndices.map(_.asInstanceOf[Any])))
        row.update(5, new GenericArrayData(sm.values.map(_.asInstanceOf[Any])))
        row.setBoolean(6, sm.isTransposed)

      case dm: DenseMatrix => //密集矩阵
        row.setByte(0, 1)
        row.setInt(1, dm.numRows)
        row.setInt(2, dm.numCols)
        row.setNullAt(3) //稀疏矩阵的特征,因此设置为null
        row.setNullAt(4) //稀疏矩阵的特征,因此设置为null
        row.update(5, new GenericArrayData(dm.values.map(_.asInstanceOf[Any])))//存储value值
        row.setBoolean(6, dm.isTransposed)
    }
    row
  }

  override def deserialize(datum: Any): Matrix = {
    datum match {
      case row: InternalRow =>
        require(row.numFields == 7,
          s"MatrixUDT.deserialize given row with length ${row.numFields} but requires length == 7")
        val tpe = row.getByte(0)
        val numRows = row.getInt(1)
        val numCols = row.getInt(2)
        val values = row.getArray(5).toDoubleArray()
        val isTransposed = row.getBoolean(6)
        tpe match {
          case 0 =>
            val colPtrs = row.getArray(3).toIntArray()
            val rowIndices = row.getArray(4).toIntArray()
            new SparseMatrix(numRows, numCols, colPtrs, rowIndices, values, isTransposed)
          case 1 =>
            new DenseMatrix(numRows, numCols, values, isTransposed)
        }
    }
  }

  override def userClass: Class[Matrix] = classOf[Matrix]

  override def equals(o: Any): Boolean = {
    o match {
      case v: MatrixUDT => true
      case _ => false
    }
  }

  // see [SPARK-8647], this achieves the needed constant hash code without constant no.
  override def hashCode(): Int = classOf[MatrixUDT].getName.hashCode()

  override def typeName: String = "matrix"

  override def pyUDT: String = "pyspark.mllib.linalg.MatrixUDT"

  private[spark] override def asNullable: MatrixUDT = this
}

/**
 * Column-major dense matrix.
 * The entry values are stored in a single array of doubles with columns listed in sequence.
 * For example, the following matrix
 * {{{
 *   1.0 2.0
 *   3.0 4.0
 *   5.0 6.0
 * }}}
 * is stored as `[1.0, 3.0, 5.0, 2.0, 4.0, 6.0]`.
 *
 * @param numRows number of rows
 * @param numCols number of columns
 * @param values matrix entries in column major if not transposed or in row major otherwise
 * @param isTransposed whether the matrix is transposed. If true, `values` stores the matrix in
 *                     row major.
  *  密集矩阵
 */
@Since("1.0.0")
@SQLUserDefinedType(udt = classOf[MatrixUDT])
class DenseMatrix @Since("1.3.0") (
    @Since("1.0.0") val numRows: Int,
    @Since("1.0.0") val numCols: Int,
    @Since("1.0.0") val values: Array[Double],//列的方式存储该矩阵
    @Since("1.3.0") override val isTransposed: Boolean) extends Matrix {//true表示value的数据是以行的方式录入的

  require(values.length == numRows * numCols, "The number of values supplied doesn't match the " +
    s"size of the matrix! values.length: ${values.length}, numRows * numCols: ${numRows * numCols}")

  /**
   * Column-major dense matrix.
   * The entry values are stored in a single array of doubles with columns listed in sequence.
   * For example, the following matrix
   * {{{
   *   1.0 2.0
   *   3.0 4.0
   *   5.0 6.0
   * }}}
   * is stored as `[1.0, 3.0, 5.0, 2.0, 4.0, 6.0]`.
   *
   * @param numRows number of rows
   * @param numCols number of columns
   * @param values matrix entries in column major
   */
  @Since("1.0.0")
  def this(numRows: Int, numCols: Int, values: Array[Double]) =
    this(numRows, numCols, values, false)

  override def equals(o: Any): Boolean = o match {
    case m: Matrix => toBreeze == m.toBreeze
    case _ => false
  }

  override def hashCode: Int = {
    com.google.common.base.Objects.hashCode(numRows : Integer, numCols: Integer, toArray)
  }

  //BM对象会有更丰富的矩阵操作
  private[mllib] def toBreeze: BM[Double] = {
    if (!isTransposed) {
      new BDM[Double](numRows, numCols, values)
    } else {
      val breezeMatrix = new BDM[Double](numCols, numRows, values) //先转置
      breezeMatrix.t
    }
  }

  private[mllib] def apply(i: Int): Double = values(i)

  //获取i行 j列的值
  @Since("1.3.0")
  override def apply(i: Int, j: Int): Double = values(index(i, j))

  //i表示row j表示列
  private[mllib] def index(i: Int, j: Int): Int = {
    if (!isTransposed) i + numRows * j else j + numCols * i //i从0开始计算
  }

  private[mllib] def update(i: Int, j: Int, v: Double): Unit = {
    values(index(i, j)) = v
  }

  @Since("1.4.0")
  override def copy: DenseMatrix = new DenseMatrix(numRows, numCols, values.clone())

  //对每一个value进行处理
  private[spark] def map(f: Double => Double) = new DenseMatrix(numRows, numCols, values.map(f),
    isTransposed)

  //更新value每一个元素值
  private[mllib] def update(f: Double => Double): DenseMatrix = {
    val len = values.length
    var i = 0
    while (i < len) {
      values(i) = f(values(i))
      i += 1
    }
    this
  }

  @Since("1.3.0")
  override def transpose: DenseMatrix = new DenseMatrix(numCols, numRows, values, !isTransposed) //isTransposed 进行转置

  //对每一个元素进行处理
  private[spark] override def foreachActive(f: (Int, Int, Double) => Unit): Unit = {
    if (!isTransposed) {
      // outer loop over columns
      var j = 0
      while (j < numCols) {
        var i = 0
        val indStart = j * numRows
        while (i < numRows) {
          f(i, j, values(indStart + i))
          i += 1
        }
        j += 1
      }
    } else {
      // outer loop over rows
      var i = 0
      while (i < numRows) {
        var j = 0
        val indStart = i * numCols
        while (j < numCols) {
          f(i, j, values(indStart + j))
          j += 1
        }
        i += 1
      }
    }
  }

  //非0的元素数量
  @Since("1.5.0")
  override def numNonzeros: Int = values.count(_ != 0)

  //所有元素值数量
  @Since("1.5.0")
  override def numActives: Int = values.length

  /**
   * Generate a `SparseMatrix` from the given `DenseMatrix`. The new matrix will have isTransposed
   * set to false.
    * 转换成稀松矩阵
   */
  @Since("1.3.0")
  def toSparse: SparseMatrix = {
    val spVals: MArrayBuilder[Double] = new MArrayBuilder.ofDouble
    val colPtrs: Array[Int] = new Array[Int](numCols + 1)
    val rowIndices: MArrayBuilder[Int] = new MArrayBuilder.ofInt
    var nnz = 0
    var j = 0
    while (j < numCols) {
      var i = 0
      while (i < numRows) {
        val v = values(index(i, j))
        if (v != 0.0) {
          rowIndices += i
          spVals += v
          nnz += 1
        }
        i += 1
      }
      j += 1
      colPtrs(j) = nnz
    }
    new SparseMatrix(numRows, numCols, colPtrs, rowIndices.result(), spVals.result())
  }
}

/**
 * Factory methods for [[org.apache.spark.mllib.linalg.DenseMatrix]].
 */
@Since("1.3.0")
object DenseMatrix {

  /**
   * Generate a `DenseMatrix` consisting of zeros.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @return `DenseMatrix` with size `numRows` x `numCols` and values of zeros
    *        创建都为0的矩阵
   */
  @Since("1.3.0")
  def zeros(numRows: Int, numCols: Int): DenseMatrix = {
    require(numRows.toLong * numCols <= Int.MaxValue,
            s"$numRows x $numCols dense matrix is too large to allocate")
    new DenseMatrix(numRows, numCols, new Array[Double](numRows * numCols))
  }

  /**
   * Generate a `DenseMatrix` consisting of ones.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @return `DenseMatrix` with size `numRows` x `numCols` and values of ones
    *     创建都为1的矩阵
   */
  @Since("1.3.0")
  def ones(numRows: Int, numCols: Int): DenseMatrix = {
    require(numRows.toLong * numCols <= Int.MaxValue,
            s"$numRows x $numCols dense matrix is too large to allocate")
    new DenseMatrix(numRows, numCols, Array.fill(numRows * numCols)(1.0))
  }

  /**
   * Generate an Identity Matrix in `DenseMatrix` format.
   * @param n number of rows and columns of the matrix
   * @return `DenseMatrix` with size `n` x `n` and values of ones on the diagonal
    * 创建方阵,并且对角线元素都是1,其他元素都是0的方阵
   */
  @Since("1.3.0")
  def eye(n: Int): DenseMatrix = {
    val identity = DenseMatrix.zeros(n, n)
    var i = 0
    while (i < n) {
      identity.update(i, i, 1.0)
      i += 1
    }
    identity
  }

  /**
   * Generate a `DenseMatrix` consisting of `i.i.d.` uniform random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param rng a random number generator
   * @return `DenseMatrix` with size `numRows` x `numCols` and values in U(0, 1)
    *        产生0-1的随机矩阵,当然随机函数可以自己定义,默认是0-1的随机数
   */
  @Since("1.3.0")
  def rand(numRows: Int, numCols: Int, rng: Random): DenseMatrix = {
    require(numRows.toLong * numCols <= Int.MaxValue,
            s"$numRows x $numCols dense matrix is too large to allocate")
    new DenseMatrix(numRows, numCols, Array.fill(numRows * numCols)(rng.nextDouble()))
  }

  /**
   * Generate a `DenseMatrix` consisting of `i.i.d.` gaussian random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param rng a random number generator
   * @return `DenseMatrix` with size `numRows` x `numCols` and values in N(0, 1)
    *        产生0-1的随机矩阵,当然随机函数可以自己定义,默认是0-1的随机数
    *        注意随机数符合高斯分布
   */
  @Since("1.3.0")
  def randn(numRows: Int, numCols: Int, rng: Random): DenseMatrix = {
    require(numRows.toLong * numCols <= Int.MaxValue,
            s"$numRows x $numCols dense matrix is too large to allocate")
    new DenseMatrix(numRows, numCols, Array.fill(numRows * numCols)(rng.nextGaussian()))
  }

  /**
   * Generate a diagonal matrix in `DenseMatrix` format from the supplied values.
   * @param vector a `Vector` that will form the values on the diagonal of the matrix
   * @return Square `DenseMatrix` with size `values.length` x `values.length` and `values`
   *         on the diagonal
    *         产生一个方阵，方阵的其他元素都是0，对角线元素为参数向量
   */
  @Since("1.3.0")
  def diag(vector: Vector): DenseMatrix = {
    val n = vector.size
    val matrix = DenseMatrix.zeros(n, n)
    val values = vector.toArray
    var i = 0
    while (i < n) {
      matrix.update(i, i, values(i))
      i += 1
    }
    matrix
  }
}

/**
 * Column-major sparse matrix.
 * The entry values are stored in Compressed Sparse Column (CSC) format.
 * For example, the following matrix
 * {{{
 *   1.0 0.0 4.0
 *   0.0 3.0 5.0
 *   2.0 0.0 6.0
 * }}}
 * is stored as `values: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]`,
 * `rowIndices=[0, 2, 1, 0, 1, 2]`, `colPointers=[0, 2, 3, 6]`.
 *
 * @param numRows number of rows
 * @param numCols number of columns
 * @param colPtrs the index corresponding to the start of a new column (if not transposed)
 * @param rowIndices the row index of the entry (if not transposed). They must be in strictly
 *                   increasing order for each column
 * @param values nonzero matrix entries in column major (if not transposed)
 * @param isTransposed whether the matrix is transposed. If true, the matrix can be considered
 *                     Compressed Sparse Row (CSR) format, where `colPtrs` behaves as rowPtrs,
 *                     and `rowIndices` behave as colIndices, and `values` are stored in row major.
  * 稀疏矩阵
  * 只存储非0的元素
 */
@Since("1.2.0")
@SQLUserDefinedType(udt = classOf[MatrixUDT])
class SparseMatrix @Since("1.3.0") (
    @Since("1.2.0") val numRows: Int,
    @Since("1.2.0") val numCols: Int,
    @Since("1.2.0") val colPtrs: Array[Int],
    @Since("1.2.0") val rowIndices: Array[Int],
    @Since("1.2.0") val values: Array[Double],//存储非0的值
    @Since("1.3.0") override val isTransposed: Boolean) extends Matrix {

  //每一个数据对应一个元素
  require(values.length == rowIndices.length, "The number of row indices and values don't match! " +
    s"values.length: ${values.length}, rowIndices.length: ${rowIndices.length}")
  // The Or statement is for the case when the matrix is transposed
  //colPtrs为列数+1 或者 行数+1,取决于转置
  require(colPtrs.length == numCols + 1 || colPtrs.length == numRows + 1, "The length of the " +
    "column indices should be the number of columns + 1. Currently, colPointers.length: " +
    s"${colPtrs.length}, numCols: $numCols")
  //value的元素数量 = colPtrs的最后一个数字
  require(values.length == colPtrs.last, "The last value of colPtrs must equal the number of " +
    s"elements. values.length: ${values.length}, colPtrs.last: ${colPtrs.last}")

  /**
   * Column-major sparse matrix.
   * The entry values are stored in Compressed Sparse Column (CSC) format.
   * For example, the following matrix
   * {{{
   *   1.0 0.0 4.0
   *   0.0 3.0 5.0
   *   2.0 0.0 6.0
   * }}}
   * is stored as `values: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]`,
   * `rowIndices=[0, 2, 1, 0, 1, 2]`, `colPointers=[0, 2, 3, 6]`.
   *
   * @param numRows number of rows
   * @param numCols number of columns
   * @param colPtrs the index corresponding to the start of a new column
   * @param rowIndices the row index of the entry. They must be in strictly increasing
   *                   order for each column
   * @param values non-zero matrix entries in column major
   */
  @Since("1.2.0")
  def this(
      numRows: Int,
      numCols: Int,
      colPtrs: Array[Int],//表示以列为单位,截止到该列时，存储了多少个有效的值
      rowIndices: Array[Int],//以列为单位，计算存储该列的位置
      values: Array[Double]) = this(numRows, numCols, colPtrs, rowIndices, values, false)

  override def equals(o: Any): Boolean = o match {
    case m: Matrix => toBreeze == m.toBreeze
    case _ => false
  }

  private[mllib] def toBreeze: BM[Double] = {
     if (!isTransposed) {
       new BSM[Double](values, numRows, numCols, colPtrs, rowIndices)
     } else {
       val breezeMatrix = new BSM[Double](values, numCols, numRows, colPtrs, rowIndices)
       breezeMatrix.t
     }
  }

  @Since("1.3.0")
  override def apply(i: Int, j: Int): Double = {
    val ind = index(i, j)
    if (ind < 0) 0.0 else values(ind)
  }

  //找不到元素则返回-1
  private[mllib] def index(i: Int, j: Int): Int = {
    if (!isTransposed) {
      Arrays.binarySearch(rowIndices, colPtrs(j), colPtrs(j + 1), i)
    } else {
      Arrays.binarySearch(rowIndices, colPtrs(i), colPtrs(i + 1), j)
    }
  }

  //只能更改非0的值,如果该位置无有效的值,因此index返回是-1,会报错
  private[mllib] def update(i: Int, j: Int, v: Double): Unit = {
    val ind = index(i, j)
    if (ind == -1) {
      throw new NoSuchElementException("The given row and column indices correspond to a zero " +
        "value. Only non-zero elements in Sparse Matrices can be updated.")
    } else {
      values(ind) = v
    }
  }

  @Since("1.4.0")
  override def copy: SparseMatrix = {
    new SparseMatrix(numRows, numCols, colPtrs, rowIndices, values.clone())
  }

  private[spark] def map(f: Double => Double) =
    new SparseMatrix(numRows, numCols, colPtrs, rowIndices, values.map(f), isTransposed)

  private[mllib] def update(f: Double => Double): SparseMatrix = {
    val len = values.length
    var i = 0
    while (i < len) {
      values(i) = f(values(i))
      i += 1
    }
    this
  }

  @Since("1.3.0")
  override def transpose: SparseMatrix =
    new SparseMatrix(numCols, numRows, colPtrs, rowIndices, values, !isTransposed)

  private[spark] override def foreachActive(f: (Int, Int, Double) => Unit): Unit = {
    if (!isTransposed) {
      var j = 0
      while (j < numCols) {
        var idx = colPtrs(j) //找到该列的开始元素位置
        val idxEnd = colPtrs(j + 1) //该列的咱以后元素位置
        while (idx < idxEnd) { //循环输出一列数据
          f(rowIndices(idx), j, values(idx))
          idx += 1
        }
        j += 1
      }
    } else {
      var i = 0
      while (i < numRows) {
        var idx = colPtrs(i)
        val idxEnd = colPtrs(i + 1)
        while (idx < idxEnd) {
          val j = rowIndices(idx)
          f(i, j, values(idx))
          idx += 1
        }
        i += 1
      }
    }
  }

  /**
   * Generate a `DenseMatrix` from the given `SparseMatrix`. The new matrix will have isTransposed
   * set to false.
   */
  @Since("1.3.0")
  def toDense: DenseMatrix = {
    new DenseMatrix(numRows, numCols, toArray)
  }

  @Since("1.5.0")
  override def numNonzeros: Int = values.count(_ != 0)

  @Since("1.5.0")
  override def numActives: Int = values.length

}

/**
 * Factory methods for [[org.apache.spark.mllib.linalg.SparseMatrix]].
 */
@Since("1.3.0")
object SparseMatrix {

  /**
   * Generate a `SparseMatrix` from Coordinate List (COO) format. Input must be an array of
   * (i, j, value) tuples. Entries that have duplicate values of i and j are
   * added together. Tuples where value is equal to zero will be omitted.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param entries Array of (i, j, value) tuples
   * @return The corresponding `SparseMatrix`
   */
  @Since("1.3.0")
  def fromCOO(numRows: Int, numCols: Int, entries: Iterable[(Int, Int, Double)]): SparseMatrix = {
    val sortedEntries = entries.toSeq.sortBy(v => (v._2, v._1)) //按照列 行排序
    val numEntries = sortedEntries.size
    if (sortedEntries.nonEmpty) {
      // Since the entries are sorted by column index, we only need to check the first and the last.
      for (col <- Seq(sortedEntries.head._2, sortedEntries.last._2)) {
        require(col >= 0 && col < numCols, s"Column index out of range [0, $numCols): $col.")
      }
    }
    val colPtrs = new Array[Int](numCols + 1)
    val rowIndices = MArrayBuilder.make[Int]
    rowIndices.sizeHint(numEntries)
    val values = MArrayBuilder.make[Double]
    values.sizeHint(numEntries)
    var nnz = 0
    var prevCol = 0
    var prevRow = -1
    var prevVal = 0.0
    // Append a dummy entry to include the last one at the end of the loop.
    (sortedEntries.view :+ (numRows, numCols, 1.0)).foreach { case (i, j, v) =>
      if (v != 0) {
        if (i == prevRow && j == prevCol) {
          prevVal += v
        } else {
          if (prevVal != 0) {
            require(prevRow >= 0 && prevRow < numRows,
              s"Row index out of range [0, $numRows): $prevRow.")
            nnz += 1
            rowIndices += prevRow
            values += prevVal
          }
          prevRow = i
          prevVal = v
          while (prevCol < j) {
            colPtrs(prevCol + 1) = nnz
            prevCol += 1
          }
        }
      }
    }
    new SparseMatrix(numRows, numCols, colPtrs, rowIndices.result(), values.result())
  }

  /**
   * Generate an Identity Matrix in `SparseMatrix` format.
   * @param n number of rows and columns of the matrix
   * @return `SparseMatrix` with size `n` x `n` and values of ones on the diagonal
    *        方阵,并且对角线都是1,其他都是0
   */
  @Since("1.3.0")
  def speye(n: Int): SparseMatrix = {
    new SparseMatrix(n, n, (0 to n).toArray, (0 until n).toArray, Array.fill(n)(1.0))
  }

  /**
   * Generates the skeleton of a random `SparseMatrix` with a given random number generator.
   * The values of the matrix returned are undefined.
   */
  private def genRandMatrix(
      numRows: Int,
      numCols: Int,
      density: Double,
      rng: Random): SparseMatrix = {
    require(numRows > 0, s"numRows must be greater than 0 but got $numRows")
    require(numCols > 0, s"numCols must be greater than 0 but got $numCols")
    require(density >= 0.0 && density <= 1.0,
      s"density must be a double in the range 0.0 <= d <= 1.0. Currently, density: $density")
    val size = numRows.toLong * numCols
    val expected = size * density
    assert(expected < Int.MaxValue,
      "The expected number of nonzeros cannot be greater than Int.MaxValue.")
    val nnz = math.ceil(expected).toInt
    if (density == 0.0) {
      new SparseMatrix(numRows, numCols, new Array[Int](numCols + 1), Array[Int](), Array[Double]())
    } else if (density == 1.0) {
      val colPtrs = Array.tabulate(numCols + 1)(j => j * numRows)
      val rowIndices = Array.tabulate(size.toInt)(idx => idx % numRows)
      new SparseMatrix(numRows, numCols, colPtrs, rowIndices, new Array[Double](numRows * numCols))
    } else if (density < 0.34) {
      // draw-by-draw, expected number of iterations is less than 1.5 * nnz
      val entries = MHashSet[(Int, Int)]()
      while (entries.size < nnz) {
        entries += ((rng.nextInt(numRows), rng.nextInt(numCols)))
      }
      SparseMatrix.fromCOO(numRows, numCols, entries.map(v => (v._1, v._2, 1.0)))
    } else {
      // selection-rejection method
      var idx = 0L
      var numSelected = 0
      var j = 0
      val colPtrs = new Array[Int](numCols + 1)
      val rowIndices = new Array[Int](nnz)
      while (j < numCols && numSelected < nnz) {
        var i = 0
        while (i < numRows && numSelected < nnz) {
          if (rng.nextDouble() < 1.0 * (nnz - numSelected) / (size - idx)) {
            rowIndices(numSelected) = i
            numSelected += 1
          }
          i += 1
          idx += 1
        }
        colPtrs(j + 1) = numSelected
        j += 1
      }
      new SparseMatrix(numRows, numCols, colPtrs, rowIndices, new Array[Double](nnz))
    }
  }

  /**
   * Generate a `SparseMatrix` consisting of `i.i.d`. uniform random numbers. The number of non-zero
   * elements equal the ceiling of `numRows` x `numCols` x `density`
   *
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param density the desired density for the matrix
   * @param rng a random number generator
   * @return `SparseMatrix` with size `numRows` x `numCols` and values in U(0, 1)
   */
  @Since("1.3.0")
  def sprand(numRows: Int, numCols: Int, density: Double, rng: Random): SparseMatrix = {
    val mat = genRandMatrix(numRows, numCols, density, rng)
    mat.update(i => rng.nextDouble())
  }

  /**
   * Generate a `SparseMatrix` consisting of `i.i.d`. gaussian random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param density the desired density for the matrix
   * @param rng a random number generator
   * @return `SparseMatrix` with size `numRows` x `numCols` and values in N(0, 1)
   */
  @Since("1.3.0")
  def sprandn(numRows: Int, numCols: Int, density: Double, rng: Random): SparseMatrix = {
    val mat = genRandMatrix(numRows, numCols, density, rng)
    mat.update(i => rng.nextGaussian())
  }

  /**
   * Generate a diagonal matrix in `SparseMatrix` format from the supplied values.
   * @param vector a `Vector` that will form the values on the diagonal of the matrix
   * @return Square `SparseMatrix` with size `values.length` x `values.length` and non-zero
   *         `values` on the diagonal
    *         方阵,对角线是参数向量的值,但是刨除0的参数值
   */
  @Since("1.3.0")
  def spdiag(vector: Vector): SparseMatrix = {
    val n = vector.size
    vector match {
      case sVec: SparseVector => //稀松向量
        SparseMatrix.fromCOO(n, n, sVec.indices.zip(sVec.values).map(v => (v._1, v._1, v._2)))
      case dVec: DenseVector => //密集向量
        val entries = dVec.values.zipWithIndex
        val nnzVals = entries.filter(v => v._1 != 0.0)
        SparseMatrix.fromCOO(n, n, nnzVals.map(v => (v._2, v._2, v._1))) //对角线就是11 22 33,因此是v._2,v._1是具体的值
    }
  }
}

/**
 * Factory methods for [[org.apache.spark.mllib.linalg.Matrix]].
 */
@Since("1.0.0")
object Matrices {

  /**
   * Creates a column-major dense matrix.
   *
   * @param numRows number of rows
   * @param numCols number of columns
   * @param values matrix entries in column major
   */
  @Since("1.0.0")
  def dense(numRows: Int, numCols: Int, values: Array[Double]): Matrix = {
    new DenseMatrix(numRows, numCols, values)
  }

  /**
   * Creates a column-major sparse matrix in Compressed Sparse Column (CSC) format.
   *
   * @param numRows number of rows
   * @param numCols number of columns
   * @param colPtrs the index corresponding to the start of a new column
   * @param rowIndices the row index of the entry
   * @param values non-zero matrix entries in column major
   */
  @Since("1.2.0")
  def sparse(
     numRows: Int,
     numCols: Int,
     colPtrs: Array[Int],
     rowIndices: Array[Int],
     values: Array[Double]): Matrix = {
    new SparseMatrix(numRows, numCols, colPtrs, rowIndices, values)
  }

  /**
   * Creates a Matrix instance from a breeze matrix.
   * @param breeze a breeze matrix
   * @return a Matrix instance
   */
  private[mllib] def fromBreeze(breeze: BM[Double]): Matrix = {
    breeze match {
      case dm: BDM[Double] =>
        new DenseMatrix(dm.rows, dm.cols, dm.data, dm.isTranspose)
      case sm: BSM[Double] =>
        // There is no isTranspose flag for sparse matrices in Breeze
        new SparseMatrix(sm.rows, sm.cols, sm.colPtrs, sm.rowIndices, sm.data)
      case _ =>
        throw new UnsupportedOperationException(
          s"Do not support conversion from type ${breeze.getClass.getName}.")
    }
  }

  /**
   * Generate a `Matrix` consisting of zeros.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @return `Matrix` with size `numRows` x `numCols` and values of zeros
   */
  @Since("1.2.0")
  def zeros(numRows: Int, numCols: Int): Matrix = DenseMatrix.zeros(numRows, numCols)

  /**
   * Generate a `DenseMatrix` consisting of ones.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @return `Matrix` with size `numRows` x `numCols` and values of ones
   */
  @Since("1.2.0")
  def ones(numRows: Int, numCols: Int): Matrix = DenseMatrix.ones(numRows, numCols)

  /**
   * Generate a dense Identity Matrix in `Matrix` format.
   * @param n number of rows and columns of the matrix
   * @return `Matrix` with size `n` x `n` and values of ones on the diagonal
   */
  @Since("1.2.0")
  def eye(n: Int): Matrix = DenseMatrix.eye(n)

  /**
   * Generate a sparse Identity Matrix in `Matrix` format.
   * @param n number of rows and columns of the matrix
   * @return `Matrix` with size `n` x `n` and values of ones on the diagonal
   */
  @Since("1.3.0")
  def speye(n: Int): Matrix = SparseMatrix.speye(n)

  /**
   * Generate a `DenseMatrix` consisting of `i.i.d.` uniform random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param rng a random number generator
   * @return `Matrix` with size `numRows` x `numCols` and values in U(0, 1)
   */
  @Since("1.2.0")
  def rand(numRows: Int, numCols: Int, rng: Random): Matrix =
    DenseMatrix.rand(numRows, numCols, rng)

  /**
   * Generate a `SparseMatrix` consisting of `i.i.d.` gaussian random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param density the desired density for the matrix
   * @param rng a random number generator
   * @return `Matrix` with size `numRows` x `numCols` and values in U(0, 1)
   */
  @Since("1.3.0")
  def sprand(numRows: Int, numCols: Int, density: Double, rng: Random): Matrix =
    SparseMatrix.sprand(numRows, numCols, density, rng)

  /**
   * Generate a `DenseMatrix` consisting of `i.i.d.` gaussian random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param rng a random number generator
   * @return `Matrix` with size `numRows` x `numCols` and values in N(0, 1)
   */
  @Since("1.2.0")
  def randn(numRows: Int, numCols: Int, rng: Random): Matrix =
    DenseMatrix.randn(numRows, numCols, rng)

  /**
   * Generate a `SparseMatrix` consisting of `i.i.d.` gaussian random numbers.
   * @param numRows number of rows of the matrix
   * @param numCols number of columns of the matrix
   * @param density the desired density for the matrix
   * @param rng a random number generator
   * @return `Matrix` with size `numRows` x `numCols` and values in N(0, 1)
   */
  @Since("1.3.0")
  def sprandn(numRows: Int, numCols: Int, density: Double, rng: Random): Matrix =
    SparseMatrix.sprandn(numRows, numCols, density, rng)

  /**
   * Generate a diagonal matrix in `Matrix` format from the supplied values.
   * @param vector a `Vector` that will form the values on the diagonal of the matrix
   * @return Square `Matrix` with size `values.length` x `values.length` and `values`
   *         on the diagonal
   */
  @Since("1.2.0")
  def diag(vector: Vector): Matrix = DenseMatrix.diag(vector)

  /**
   * Horizontally concatenate a sequence of matrices. The returned matrix will be in the format
   * the matrices are supplied in. Supplying a mix of dense and sparse matrices will result in
   * a sparse matrix. If the Array is empty, an empty `DenseMatrix` will be returned.
   * @param matrices array of matrices
   * @return a single `Matrix` composed of the matrices that were horizontally concatenated
    * 所有参数的矩阵的行数都相同，拼接起来
    * 比如 参数是 4*3 4*6 4* 8 三个矩阵，结果是4*(3+6+8)的矩阵
   */
  @Since("1.3.0")
  def horzcat(matrices: Array[Matrix]): Matrix = {
    if (matrices.isEmpty) {
      return new DenseMatrix(0, 0, Array[Double]())
    } else if (matrices.size == 1) {
      return matrices(0)
    }
    val numRows = matrices(0).numRows //多少行
    var hasSparse = false //true表示有稀松矩阵
    var numCols = 0 //总列数
    //确保行数都相同
    matrices.foreach { mat =>
      require(numRows == mat.numRows, "The number of rows of the matrices in this sequence, " +
        "don't match!")
      mat match {
        case sparse: SparseMatrix => hasSparse = true
        case dense: DenseMatrix => // empty on purpose
        case _ => throw new IllegalArgumentException("Unsupported matrix format. Expected " +
          s"SparseMatrix or DenseMatrix. Instead got: ${mat.getClass}")
      }
      numCols += mat.numCols
    }
    if (!hasSparse) {
      new DenseMatrix(numRows, numCols, matrices.flatMap(_.toArray))
    } else {
      var startCol = 0 //每一个矩阵迭代后 都会累加该矩阵的列数
      val entries: Array[(Int, Int, Double)] = matrices.flatMap { mat =>
        val nCols = mat.numCols
        mat match {
          case spMat: SparseMatrix =>
            val data = new Array[(Int, Int, Double)](spMat.values.length)
            var cnt = 0
            spMat.foreachActive { (i, j, v) =>
              data(cnt) = (i, j + startCol, v)
              cnt += 1
            }
            startCol += nCols
            data //返回数组可以迭代的
          case dnMat: DenseMatrix =>
            val data = new ArrayBuffer[(Int, Int, Double)]()
            dnMat.foreachActive { (i, j, v) =>
              if (v != 0.0) {
                data.append((i, j + startCol, v))
              }
            }
            startCol += nCols
            data
        }
      }
      SparseMatrix.fromCOO(numRows, numCols, entries)
    }
  }

  /**
   * Vertically concatenate a sequence of matrices. The returned matrix will be in the format
   * the matrices are supplied in. Supplying a mix of dense and sparse matrices will result in
   * a sparse matrix. If the Array is empty, an empty `DenseMatrix` will be returned.
   * @param matrices array of matrices
   * @return a single `Matrix` composed of the matrices that were vertically concatenated
    *  列相同的矩阵进行拼接
    *  比如 3*4 4*4 5*4 的矩阵，返回值是(3+4+5) * 4 的矩阵
   */
  @Since("1.3.0")
  def vertcat(matrices: Array[Matrix]): Matrix = {
    if (matrices.isEmpty) {
      return new DenseMatrix(0, 0, Array[Double]())
    } else if (matrices.size == 1) {
      return matrices(0)
    }
    val numCols = matrices(0).numCols
    var hasSparse = false
    var numRows = 0
    matrices.foreach { mat =>
      require(numCols == mat.numCols, "The number of rows of the matrices in this sequence, " +
        "don't match!")
      mat match {
        case sparse: SparseMatrix => hasSparse = true
        case dense: DenseMatrix => // empty on purpose
        case _ => throw new IllegalArgumentException("Unsupported matrix format. Expected " +
          s"SparseMatrix or DenseMatrix. Instead got: ${mat.getClass}")
      }
      numRows += mat.numRows
    }
    if (!hasSparse) {
      val allValues = new Array[Double](numRows * numCols)
      var startRow = 0
      matrices.foreach { mat =>
        var j = 0
        val nRows = mat.numRows
        mat.foreachActive { (i, j, v) =>
          val indStart = j * numRows + startRow
          allValues(indStart + i) = v
        }
        startRow += nRows
      }
      new DenseMatrix(numRows, numCols, allValues)
    } else {
      var startRow = 0
      val entries: Array[(Int, Int, Double)] = matrices.flatMap { mat =>
        val nRows = mat.numRows
        mat match {
          case spMat: SparseMatrix =>
            val data = new Array[(Int, Int, Double)](spMat.values.length)
            var cnt = 0
            spMat.foreachActive { (i, j, v) =>
              data(cnt) = (i + startRow, j, v)
              cnt += 1
            }
            startRow += nRows
            data
          case dnMat: DenseMatrix =>
            val data = new ArrayBuffer[(Int, Int, Double)]()
            dnMat.foreachActive { (i, j, v) =>
              if (v != 0.0) {
                data.append((i + startRow, j, v))
              }
            }
            startRow += nRows
            data
        }
      }
      SparseMatrix.fromCOO(numRows, numCols, entries)
    }
  }
}
