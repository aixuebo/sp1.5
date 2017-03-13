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

package org.apache.spark.sql.columnar

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.columnar.ColumnBuilder._
import org.apache.spark.sql.columnar.compression.{AllCompressionSchemes, CompressibleColumnBuilder}
import org.apache.spark.sql.types._

//为每一个列设置一个builder
private[sql] trait ColumnBuilder {
  /**
   * Initializes with an approximate lower bound on the expected number of elements in this column.
   * 列的名字namem,以及是否对列进行压缩
   * initialSize表示默认让该列存储多少个元素,即有多少行数据,默认是1024*1024行数据,即100多万条数据,还是很大的量,尽量不要使用默认值
   */
  def initialize(initialSize: Int, columnName: String = "", useCompression: Boolean = false)

  /**
   * Appends `row(ordinal)` to the column builder.
   */
  def appendFrom(row: InternalRow, ordinal: Int)

  /**
   * Column statistics information
   */
  def columnStats: ColumnStats

  /**
   * Returns the final columnar byte buffer.
   * 真正产生一个存储该列的ByteBuffer对象
   */
  def build(): ByteBuffer
}

private[sql] class BasicColumnBuilder[JvmType](
    val columnStats: ColumnStats,
    val columnType: ColumnType[JvmType])//该列存储的数据类型
  extends ColumnBuilder {

  protected var columnName: String = _ //列属性name

  protected var buffer: ByteBuffer = _ //存储列数据的buffer

  //创建一个缓冲区
  override def initialize(
      initialSize: Int,
      columnName: String = "",
      useCompression: Boolean = false): Unit = {

    val size = if (initialSize == 0) DEFAULT_INITIAL_BUFFER_SIZE else initialSize //初始化元素数量
    this.columnName = columnName

    // Reserves 4 bytes for column type ID
    buffer = ByteBuffer.allocate(4 + size * columnType.defaultSize) //创建一个缓冲区,缓冲区大小是: 4(数据类型)+元素数量*列的每一个列的默认大小
    buffer.order(ByteOrder.nativeOrder()).putInt(columnType.typeId)
  }

  //将row中第ordinal列的属性值获取到,追加到buffer中
  override def appendFrom(row: InternalRow, ordinal: Int): Unit = {
    //确保容纳row的ordinal位置的字节数,可能涉及到字节数组的扩容
    buffer = ensureFreeSpace(buffer, columnType.actualSize(row, ordinal)) //columnType.actualSize(row, ordinal) 表示获取row的ordinal位置的真实存储的字节数量
    columnType.append(row, ordinal, buffer) //将row中第ordinal列的属性值获取到,追加到buffer中
  }

  //返回完整的buffer字节数组
  override def build(): ByteBuffer = {
    buffer.flip().asInstanceOf[ByteBuffer]
  }
}

//存储复杂的数据类型
private[sql] abstract class ComplexColumnBuilder[JvmType](
    columnStats: ColumnStats,
    columnType: ColumnType[JvmType])
  extends BasicColumnBuilder[JvmType](columnStats, columnType)
  with NullableColumnBuilder

private[sql] abstract class NativeColumnBuilder[T <: AtomicType](
    override val columnStats: ColumnStats,
    override val columnType: NativeColumnType[T])
  extends BasicColumnBuilder[T#InternalType](columnStats, columnType)
  with NullableColumnBuilder
  with AllCompressionSchemes
  with CompressibleColumnBuilder[T]

private[sql] class BooleanColumnBuilder extends NativeColumnBuilder(new BooleanColumnStats, BOOLEAN)

private[sql] class ByteColumnBuilder extends NativeColumnBuilder(new ByteColumnStats, BYTE)

private[sql] class ShortColumnBuilder extends NativeColumnBuilder(new ShortColumnStats, SHORT)

private[sql] class IntColumnBuilder extends NativeColumnBuilder(new IntColumnStats, INT)

private[sql] class LongColumnBuilder extends NativeColumnBuilder(new LongColumnStats, LONG)

private[sql] class FloatColumnBuilder extends NativeColumnBuilder(new FloatColumnStats, FLOAT)

private[sql] class DoubleColumnBuilder extends NativeColumnBuilder(new DoubleColumnStats, DOUBLE)

private[sql] class StringColumnBuilder extends NativeColumnBuilder(new StringColumnStats, STRING)

private[sql] class DateColumnBuilder extends NativeColumnBuilder(new DateColumnStats, DATE)

private[sql] class TimestampColumnBuilder
  extends NativeColumnBuilder(new TimestampColumnStats, TIMESTAMP)

private[sql] class FixedDecimalColumnBuilder(
    precision: Int,
    scale: Int)
  extends NativeColumnBuilder(
    new FixedDecimalColumnStats(precision, scale),
    FIXED_DECIMAL(precision, scale))

private[sql] class BinaryColumnBuilder extends ComplexColumnBuilder(new BinaryColumnStats, BINARY)

//存储复杂类型,该类型是dataType,但是存储的时候还是存储的字节数组,因为将复杂类型进行序列化与反序列化后进行存储的
// TODO (lian) Add support for array, struct and map
private[sql] class GenericColumnBuilder(dataType: DataType)
  extends ComplexColumnBuilder(new GenericColumnStats(dataType), GENERIC(dataType))

private[sql] object ColumnBuilder {

  val DEFAULT_INITIAL_BUFFER_SIZE = 1024 * 1024 //默认buffer中存储多少个元素,不是字节,而是元素个数

  //确保buffer中有足够size的字节
  private[columnar] def ensureFreeSpace(orig: ByteBuffer, size: Int) = {
    if (orig.remaining >= size) {
      orig
    } else {//扩容
      // grow in steps of initial size
      val capacity = orig.capacity()
      val newSize = capacity + size.max(capacity / 8 + 1)
      val pos = orig.position()

      ByteBuffer
        .allocate(newSize)
        .order(ByteOrder.nativeOrder())
        .put(orig.array(), 0, pos)
    }
  }

  //为每一个列设置一个builder
  def apply(
      dataType: DataType,//属性类型
      initialSize: Int = 0,
      columnName: String = "",//属性名字
      useCompression: Boolean = false) //是否压缩,因为存储的是二进制在buffer中,因此压缩是一种很好的方式
    : ColumnBuilder = {
    val builder: ColumnBuilder = dataType match {
      case BooleanType => new BooleanColumnBuilder
      case ByteType => new ByteColumnBuilder
      case ShortType => new ShortColumnBuilder
      case IntegerType => new IntColumnBuilder
      case DateType => new DateColumnBuilder
      case LongType => new LongColumnBuilder
      case TimestampType => new TimestampColumnBuilder
      case FloatType => new FloatColumnBuilder
      case DoubleType => new DoubleColumnBuilder
      case StringType => new StringColumnBuilder
      case BinaryType => new BinaryColumnBuilder
      case DecimalType.Fixed(precision, scale) if precision < 19 =>
        new FixedDecimalColumnBuilder(precision, scale)
      case other => new GenericColumnBuilder(other)
    }

    builder.initialize(initialSize, columnName, useCompression)
    builder
  }
}
