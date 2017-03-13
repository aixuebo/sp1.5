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

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.columnar.{ColumnBuilder, NativeColumnBuilder}
import org.apache.spark.sql.types.AtomicType

/**
 * A stackable trait that builds optionally compressed byte buffer for a column.  Memory layout of
 * the final byte buffer is:
 * {{{
 *    .--------------------------- Column type ID (4 bytes) 属性字段对应的值类型
 *    |   .----------------------- Null count N (4 bytes)  有多少个null
 *    |   |   .------------------- Null positions (4 x N bytes, empty if null count is zero) 存储null的位置的字节数组
 *    |   |   |     .------------- Compression scheme ID (4 bytes) 压缩schemeId
 *    |   |   |     |   .--------- Compressed non-null elements 压缩非null的元素值
 *    V   V   V     V   V
 *   +---+---+-----+---+---------+
 *   |   |   | ... |   | ... ... |
 *   +---+---+-----+---+---------+
 *    \-----------/ \-----------/
 *       header         body
 * }}}
 */
private[sql] trait CompressibleColumnBuilder[T <: AtomicType]
  extends ColumnBuilder with Logging { //继承的ColumnBuilder是一个具体的类,而不是抽象方法

  this: NativeColumnBuilder[T] with WithCompressionSchemes =>

  var compressionEncoders: Seq[Encoder[T]] = _ //该属性可以允许的压缩编码方式集合

  abstract override def initialize(
      initialSize: Int,
      columnName: String,
      useCompression: Boolean): Unit = {

    compressionEncoders =
      if (useCompression) {
        schemes.filter(_.supports(columnType))//获取该类型的所有压缩方法
          .map(_.encoder[T](columnType))//得到压缩算法集合
      } else {
        Seq(PassThrough.encoder(columnType))//说明不需要压缩,因此只能有一个方式
      }
    super.initialize(initialSize, columnName, useCompression)
  }

  protected def isWorthCompressing(encoder: Encoder[T]) = { //true表示有价值的压缩,即压缩比很高,压缩效果好
    encoder.compressionRatio < 0.8
  }

  private def gatherCompressibilityStats(row: InternalRow, ordinal: Int): Unit = {
    var i = 0
    while (i < compressionEncoders.length) {//计算每一种压缩算法后,压缩剩余字节数
      compressionEncoders(i).gatherCompressibilityStats(row, ordinal)
      i += 1
    }
  }

  abstract override def appendFrom(row: InternalRow, ordinal: Int): Unit = {
    super.appendFrom(row, ordinal) //先进行追加元素
    if (!row.isNullAt(ordinal)) {
      gatherCompressibilityStats(row, ordinal)//不是null的元素进行计算压缩后的字节
    }
  }

  override def build(): ByteBuffer = {
    val nonNullBuffer = buildNonNulls()
    val typeId = nonNullBuffer.getInt() //数据类型
    //压缩方式
    val encoder: Encoder[T] = {
      val candidate = compressionEncoders.minBy(_.compressionRatio) //寻找压缩比例最小的
      if (isWorthCompressing(candidate)) candidate else PassThrough.encoder(columnType) //判断是否值得压缩,如果不值得压缩,使用PassThrough进行压缩
    }

    // Header = column type ID + null count + null positions
    val headerSize = 4 + 4 + nulls.limit() //前面两个4分别设置列的属性类型以及null的数量
    //压缩非null后的字节数
    val compressedSize = if (encoder.compressedSize == 0) {
      nonNullBuffer.remaining()
    } else {
      encoder.compressedSize
    }

    val compressedBuffer = ByteBuffer
      // Reserves 4 bytes for compression scheme ID
      .allocate(headerSize + 4 + compressedSize)
      .order(ByteOrder.nativeOrder)
      // Write the header
      .putInt(typeId)
      .putInt(nullCount)
      .put(nulls)

    logDebug(s"Compressor for [$columnName]: $encoder, ratio: ${encoder.compressionRatio}")
    encoder.compress(nonNullBuffer, compressedBuffer) //对数据进行压缩,encoder会向compressedBuffer中设置压缩的算法类型ID以及压缩内容
  }
}
