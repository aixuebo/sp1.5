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

import java.nio.{ByteOrder, ByteBuffer}

import org.apache.spark.sql.catalyst.expressions.MutableRow

/**
 * 将列的属性值是null的单独处理,设置成第几个是null,将下标占用4个字节存储到字节数组即可
 */
private[sql] trait NullableColumnAccessor extends ColumnAccessor {
  private var nullsBuffer: ByteBuffer = _ //存储null值的字节数组
  private var nullCount: Int = _ //返回有多少个null被设置了
  private var seenNulls: Int = 0 //已经看到了多少个null了

  private var nextNullIndex: Int = _ //下一个null对应的index
  private var pos: Int = 0 //当前位置

  abstract override protected def initialize(): Unit = {
    nullsBuffer = underlyingBuffer.duplicate().order(ByteOrder.nativeOrder()) //获取全部字节数组
    nullCount = nullsBuffer.getInt()  //返回字节数组有多少个null
    nextNullIndex = if (nullCount > 0) nullsBuffer.getInt() else -1 //获取下一个null所在的index
    pos = 0 //当前位置

    underlyingBuffer.position(underlyingBuffer.position + 4 + nullCount * 4) //设置底层位置,刨除null后的位置,即当前位置+4(多少个null)+ nullCount*4(这么多null占用的字节数)
    super.initialize()
  }

  abstract override def extractTo(row: MutableRow, ordinal: Int): Unit = {
    if (pos == nextNullIndex) {//查看当前位置是否是下一个null的位置
      seenNulls += 1 //是则说明看到了一个null

      if (seenNulls < nullCount) {//说明还是有null的
        nextNullIndex = nullsBuffer.getInt()//返回下一个null的位置
      }

      row.setNullAt(ordinal)//为该row设置ordinal位置的属性为null
    } else {
      super.extractTo(row, ordinal)
    }

    pos += 1 //位置移动一个
  }

  abstract override def hasNext: Boolean = seenNulls < nullCount || super.hasNext //还有null  or 还有数据 则都说明还有数据可以被循环,因此返回true
}
