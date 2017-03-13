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

import org.apache.spark.sql.catalyst.expressions.MutableRow
import org.apache.spark.sql.columnar.compression.CompressibleColumnAccessor
import org.apache.spark.sql.types._

/**
 * An `Iterator` like trait used to extract values from columnar byte buffer. When a value is
 * extracted from the buffer, instead of directly returning it, the value is set into some field of
 * a [[MutableRow]]. In this way, boxing cost can be avoided by leveraging the setter methods
 * for primitive values provided by [[MutableRow]].
 * 从ByteBuffer中抽取数据,类似迭代器一样,不断的从ByteBuffer中抽取数据
 *
 * 数据列的访问器
 */
private[sql] trait ColumnAccessor {
  initialize()

  protected def initialize()

  def hasNext: Boolean //是否还有数据要被访问

  //每一次抽取的值为该row的第ordinal个属性赋值
  def extractTo(row: MutableRow, ordinal: Int)

  protected def underlyingBuffer: ByteBuffer //底层的缓冲池
}

private[sql] abstract class BasicColumnAccessor[JvmType](
    protected val buffer: ByteBuffer,//存储数据的buffer
    protected val columnType: ColumnType[JvmType]) //buffer中存储的数据类型
  extends ColumnAccessor {

  protected def initialize() {}

  override def hasNext: Boolean = buffer.hasRemaining //buffer内还有数据,则true

  //每一次抽取的值为该row的第ordinal个属性赋值
  override def extractTo(row: MutableRow, ordinal: Int): Unit = {
    extractSingle(row, ordinal)
  }

  //每一次抽取的值为该row的第ordinal个属性赋值
  def extractSingle(row: MutableRow, ordinal: Int): Unit = {
    columnType.extract(buffer, row, ordinal)//从buffer中抽取数据,然后将结果为row的第ordinal个属性赋值
  }

  protected def underlyingBuffer = buffer //底层的缓冲池
}

private[sql] abstract class NativeColumnAccessor[T <: AtomicType](
    override protected val buffer: ByteBuffer,//存储数据的缓冲区
    override protected val columnType: NativeColumnType[T])//缓冲区里面存储的是什么类型的数据
  extends BasicColumnAccessor(buffer, columnType)
  with NullableColumnAccessor
  with CompressibleColumnAccessor[T]

//比如存储的是boolean类型的
private[sql] class BooleanColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, BOOLEAN)

private[sql] class ByteColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, BYTE)

private[sql] class ShortColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, SHORT)

private[sql] class IntColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, INT)

private[sql] class LongColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, LONG)

private[sql] class FloatColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, FLOAT)

private[sql] class DoubleColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, DOUBLE)

private[sql] class StringColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, STRING)

private[sql] class FixedDecimalColumnAccessor(buffer: ByteBuffer, precision: Int, scale: Int)
  extends NativeColumnAccessor(buffer, FIXED_DECIMAL(precision, scale))

private[sql] class DateColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, DATE)

private[sql] class TimestampColumnAccessor(buffer: ByteBuffer)
  extends NativeColumnAccessor(buffer, TIMESTAMP)

//每次访问的
private[sql] class BinaryColumnAccessor(buffer: ByteBuffer)
  extends BasicColumnAccessor[Array[Byte]](buffer, BINARY)
  with NullableColumnAccessor

//抽取一组字节数组,然后将其转反序列化成对象,存储到对应的row的index位置上
private[sql] class GenericColumnAccessor(buffer: ByteBuffer, dataType: DataType)
  extends BasicColumnAccessor[Array[Byte]](buffer, GENERIC(dataType))
  with NullableColumnAccessor

private[sql] object ColumnAccessor {
  def apply(dataType: DataType, buffer: ByteBuffer): ColumnAccessor = {
    val dup = buffer.duplicate().order(ByteOrder.nativeOrder)

    // The first 4 bytes in the buffer indicate the column type.  This field is not used now,
    // because we always know the data type of the column ahead of time.
    dup.getInt() //获取第一个int值,表示数据类型,但是因为我们传入参数已经知道了数据类型,因此不需要该数据了,但是需要从buffer中把该值抽取出来

    dataType match {
      case BooleanType => new BooleanColumnAccessor(dup)
      case ByteType => new ByteColumnAccessor(dup)
      case ShortType => new ShortColumnAccessor(dup)
      case IntegerType => new IntColumnAccessor(dup)
      case DateType => new DateColumnAccessor(dup)
      case LongType => new LongColumnAccessor(dup)
      case TimestampType => new TimestampColumnAccessor(dup)
      case FloatType => new FloatColumnAccessor(dup)
      case DoubleType => new DoubleColumnAccessor(dup)
      case StringType => new StringColumnAccessor(dup)
      case BinaryType => new BinaryColumnAccessor(dup)
      case DecimalType.Fixed(precision, scale) if precision < 19 =>
        new FixedDecimalColumnAccessor(dup, precision, scale)
      case other => new GenericColumnAccessor(dup, other)
    }
  }
}
