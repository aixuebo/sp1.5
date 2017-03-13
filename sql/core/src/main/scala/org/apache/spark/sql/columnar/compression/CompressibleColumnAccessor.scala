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

import org.apache.spark.sql.catalyst.expressions.MutableRow
import org.apache.spark.sql.columnar.{ColumnAccessor, NativeColumnAccessor}
import org.apache.spark.sql.types.AtomicType

//可压缩的方式访问列属性值
private[sql] trait CompressibleColumnAccessor[T <: AtomicType] extends ColumnAccessor {
  this: NativeColumnAccessor[T] =>

  private var decoder: Decoder[T] = _ //解压方式

  abstract override protected def initialize(): Unit = {
    super.initialize()
    decoder = CompressionScheme(underlyingBuffer.getInt()).decoder(buffer, columnType) //通过数压缩类型,得到具体的解压方式.有了解压方式,就可以对buffer进行解压.每一个元素的值是columnType类型的
  }

  abstract override def hasNext: Boolean = super.hasNext || decoder.hasNext

  //不断抽取一个元素值,存储到row的第ordinal个元素位置上
  override def extractSingle(row: MutableRow, ordinal: Int): Unit = {
    decoder.next(row, ordinal)
  }
}
