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

package org.apache.spark.sql.columnar.compression

import java.nio.{ByteBuffer, ByteOrder}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.MutableRow
import org.apache.spark.sql.columnar.{ColumnType, NativeColumnType}
import org.apache.spark.sql.types.AtomicType

//对一个数据类型进行编码
private[sql] trait Encoder[T <: AtomicType] {

  def gatherCompressibilityStats(row: InternalRow, ordinal: Int): Unit = {} //去给定一行的数据以及该属性列的序号,去收集压缩前后的字节大小

  def compressedSize: Int //压缩后的字节大小

  def uncompressedSize: Int //未压缩前的数据大小

  //压缩比例
  def compressionRatio: Double = {
    if (uncompressedSize > 0) compressedSize.toDouble / uncompressedSize else 1.0 //压缩后/压缩前 即压缩比例
  }

  //真正意义的去压缩,将from的原始数据,压缩后,存储到to中
  def compress(from: ByteBuffer, to: ByteBuffer): ByteBuffer
}

//对数据进行解码,解码成T类型
private[sql] trait Decoder[T <: AtomicType] {

  //为row的第ordinal属性设置值
  def next(row: MutableRow, ordinal: Int): Unit

  //是否还有值要去被设置
  def hasNext: Boolean
}

private[sql] trait CompressionScheme {
  def typeId: Int //压缩算法类型   全体通过PassThrough的类型是0   RunLengthEncoding类型是1   DictionaryEncoding类型是2  BooleanBitSet类型是3  IntDelta类型是4   LongDelta类型是5

  def supports(columnType: ColumnType[_]): Boolean //是否该压缩算法支持该数据类型

  def encoder[T <: AtomicType](columnType: NativeColumnType[T]): Encoder[T] //对该数据类型进行编码

  def decoder[T <: AtomicType](buffer: ByteBuffer, columnType: NativeColumnType[T]): Decoder[T] //对该数据类型进行解码,从buffer中获取数据
}

//表示该属性会持有若干种压缩方式的集合
private[sql] trait WithCompressionSchemes {
  def schemes: Seq[CompressionScheme]
}

//所有的压缩算法集合
private[sql] trait AllCompressionSchemes extends WithCompressionSchemes {
  override val schemes: Seq[CompressionScheme] = CompressionScheme.all
}

private[sql] object CompressionScheme {
  val all: Seq[CompressionScheme] =
    Seq(PassThrough, RunLengthEncoding, DictionaryEncoding, BooleanBitSet, IntDelta, LongDelta) //压缩算法集合

  private val typeIdToScheme = all.map(scheme => scheme.typeId -> scheme).toMap //每一个压缩算法id与压缩算法实例CompressionScheme的map映射

  //给一个压缩算法id,返回该压缩算法实例
  def apply(typeId: Int): CompressionScheme = {
    typeIdToScheme.getOrElse(typeId, throw new UnsupportedOperationException(
      s"Unrecognized compression scheme type ID: $typeId"))//抛异常,说明不支持该类型的压缩
  }

  //获取header头信息
  def columnHeaderSize(columnBuffer: ByteBuffer): Int = {
    val header = columnBuffer.duplicate().order(ByteOrder.nativeOrder)
    val nullCount = header.getInt(4) //获取null的数量
    // Column type ID + null count + null positions
    4 + 4 + 4 * nullCount //列的头信息需要的字节数,即该属性的数据类型+null的数量+null的位置字节和所占字节总数
  }
}
