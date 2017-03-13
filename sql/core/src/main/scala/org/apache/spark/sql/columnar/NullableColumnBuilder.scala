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

/**
 * A stackable trait used for building byte buffer for a column containing null values.  Memory
 * layout of the final byte buffer is:
 * {{{
 *    .----------------------- Column type ID (4 bytes) //属性是什么类型的
 *    |   .------------------- Null count N (4 bytes) 存储多少个null
 *    |   |   .--------------- Null positions (4 x N bytes, empty if null count is zero) //每一个null对应的位置
 *    |   |   |     .--------- Non-null elements //非null的元素内容
 *    V   V   V     V
 *   +---+---+-----+---------+
 *   |   |   | ... | ... ... |
 *   +---+---+-----+---------+
 * }}}
 */
private[sql] trait NullableColumnBuilder extends ColumnBuilder {
  protected var nulls: ByteBuffer = _ //存储null的字节数组
  protected var nullCount: Int = _ //一共存储了多少个null数据
  private var pos: Int = _ //当前第几行数据

  abstract override def initialize(
      initialSize: Int,//初始化null元素的个数
      columnName: String,//属性的name
      useCompression: Boolean) //是否对字节数组进行压缩
      : Unit = {

    nulls = ByteBuffer.allocate(1024) //先分配一部分空间
    nulls.order(ByteOrder.nativeOrder()) //存储字节排序
    pos = 0
    nullCount = 0
    super.initialize(initialSize, columnName, useCompression) //初始化属性
  }

  //向buffer中添加null值
  abstract override def appendFrom(row: InternalRow, ordinal: Int): Unit = {
    columnStats.gatherStats(row, ordinal)
    if (row.isNullAt(ordinal)) {//判断row的ordinal属性值是否是null
      nulls = ColumnBuilder.ensureFreeSpace(nulls, 4) //确保有足够的数据字节位置
      nulls.putInt(pos) //向null的字节数组中设置该pos位置,即表示该位置的属性值是null
      nullCount += 1 //增加一个null
    } else {//不是null,则添加数据到buffer中
      super.appendFrom(row, ordinal)
    }
    pos += 1
  }

  //创建包含null和非null的数据结果buffer
  abstract override def build(): ByteBuffer = {
    val nonNulls = super.build() //非null的字节数组
    val typeId = nonNulls.getInt() //非null的字节数组类型
    val nullDataLen = nulls.position()//null的字节数组中一共多少个字节

    nulls.limit(nullDataLen) //null的字节数组截至到该位置
    nulls.rewind()

    val buffer = ByteBuffer
      .allocate(4 + 4 + nullDataLen + nonNulls.remaining()) //前两个4 分别表示 属性类型 和 null的元素数量
      .order(ByteOrder.nativeOrder()) //设置字节排序方式
      .putInt(typeId) //设置属性的类型
      .putInt(nullCount)//设置null的元素数量
      .put(nulls) //设置null的字节数组
      .put(nonNulls) //设置非null的字节数组

    buffer.rewind()
    buffer
  }

  protected def buildNonNulls(): ByteBuffer = {
    nulls.limit(nulls.position()).rewind()
    super.build()
  }
}
