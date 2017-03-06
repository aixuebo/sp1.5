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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{GeneratedExpressionCode, CodeGenContext}
import org.apache.spark.sql.types.{LongType, DataType}

/**
 * Returns monotonically increasing 64-bit integers.
 * 返回单调增加的64为long整数
 *
 * The generated ID is guaranteed to be monotonically increasing and unique, but not consecutive.
 * 保证产生单调 且增加的唯一序号,但是该序号不是连续的
 * The current implementation puts the partition ID in the upper 31 bits, and the lower 33 bits
 * represent the record number within each partition.
 * 当前的实现是partitionId用于设置前31位,后33位设置当前partition内的序号
 * The assumption is that the data frame has less than 1 billion partitions, and each partition has less than 8 billion records.
 * 这个假设数据是少于10亿个分区,每一个分区内数据少于80亿条才有保证
 *
 * Since this expression is stateful, it cannot be a case object.
 * 该表达式会返回一个Long类型的数据
 */
private[sql] case class MonotonicallyIncreasingID() extends LeafExpression with Nondeterministic {//该表达式不确定的表达式,并且是叶子表达式,即不需要子表达式参与运算

  /**
   * Record ID within each partition. By being transient, count's value is reset to 0 every time
   * we serialize and deserialize and initialize it.
   */
  @transient private[this] var count: Long = _ //记录一个partition的序号

  @transient private[this] var partitionMask: Long = _ //记录该partition的前缀

  override protected def initInternal(): Unit = {
    count = 0L
    partitionMask = TaskContext.getPartitionId().toLong << 33 //设置partition为前31位
  }

  override def nullable: Boolean = false

  override def dataType: DataType = LongType

  override protected def evalInternal(input: InternalRow): Long = {
    val currentCount = count
    count += 1 //每次序号累加1
    partitionMask + currentCount //更改最终的唯一序号
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val countTerm = ctx.freshName("count")
    val partitionMaskTerm = ctx.freshName("partitionMask")
    ctx.addMutableState(ctx.JAVA_LONG, countTerm, s"$countTerm = 0L;")
    ctx.addMutableState(ctx.JAVA_LONG, partitionMaskTerm,
      s"$partitionMaskTerm = ((long) org.apache.spark.TaskContext.getPartitionId()) << 33;")

    ev.isNull = "false"
    s"""
      final ${ctx.javaType(dataType)} ${ev.primitive} = $partitionMaskTerm + $countTerm;
      $countTerm++;
    """
  }
}
