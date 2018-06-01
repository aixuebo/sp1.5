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

package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.LongSQLMetric


trait HashJoin {
  self: SparkPlan =>

  val leftKeys: Seq[Expression]
  val rightKeys: Seq[Expression]
  val buildSide: BuildSide
  val left: SparkPlan
  val right: SparkPlan

  //根据buildSide的类型,初始化buildPlan和streamedPlan
  protected lazy val (buildPlan, streamedPlan) = buildSide match {
    case BuildLeft => (left, right)
    case BuildRight => (right, left)
  }

  protected lazy val (buildKeys, streamedKeys) = buildSide match {
    case BuildLeft => (leftKeys, rightKeys)
    case BuildRight => (rightKeys, leftKeys)
  }

  override def output: Seq[Attribute] = left.output ++ right.output

  protected[this] def isUnsafeMode: Boolean = {
    (self.codegenEnabled && self.unsafeEnabled
      && UnsafeProjection.canSupport(buildKeys)
      && UnsafeProjection.canSupport(self.schema))
  }

  override def outputsUnsafeRows: Boolean = isUnsafeMode
  override def canProcessUnsafeRows: Boolean = isUnsafeMode
  override def canProcessSafeRows: Boolean = !isUnsafeMode

  protected def buildSideKeyGenerator: Projection =
    if (isUnsafeMode) {
      UnsafeProjection.create(buildKeys, buildPlan.output)
    } else {
      newMutableProjection(buildKeys, buildPlan.output)()
    }

  protected def streamSideKeyGenerator: Projection =
    if (isUnsafeMode) {
      UnsafeProjection.create(streamedKeys, streamedPlan.output)
    } else {
      newMutableProjection(streamedKeys, streamedPlan.output)()
    }

  protected def hashJoin(
      streamIter: Iterator[InternalRow],
      numStreamRows: LongSQLMetric,
      hashedRelation: HashedRelation,
      numOutputRows: LongSQLMetric): Iterator[InternalRow] =
  {
    new Iterator[InternalRow] {
      private[this] var currentStreamedRow: InternalRow = _ //streamIter中处理的当前行数据
      private[this] var currentHashMatches: Seq[InternalRow] = _ //与currentStreamedRow相同key的行集合
      private[this] var currentMatchPosition: Int = -1

      // Mutable per row objects.
      private[this] val joinRow = new JoinedRow
      //参数是两个表join后的schema集合对应的行,返回最终需要的行属性
      //该方法说明要一个InternalRow参数,返回一个InternalRow对象
      private[this] val resultProjection: (InternalRow) => InternalRow = {
        if (isUnsafeMode) {
          UnsafeProjection.create(self.schema)
        } else {
          identity[InternalRow] //返回两个行merge后的结果
        }
      }

      //对一个row对象如何产生key
      private[this] val joinKeys = streamSideKeyGenerator

      override final def hasNext: Boolean =
        (currentMatchPosition != -1 && currentMatchPosition < currentHashMatches.size) ||
          (streamIter.hasNext && fetchNext())

      //返回两个行merge后的结果
      override final def next(): InternalRow = {
        val ret = buildSide match { //拼装成一个大行,该行有两个表的数据组成的一个大的行,即两个表的schema拼装而成
          case BuildRight => joinRow(currentStreamedRow, currentHashMatches(currentMatchPosition))
          case BuildLeft => joinRow(currentHashMatches(currentMatchPosition), currentStreamedRow)
        }
        currentMatchPosition += 1
        numOutputRows += 1
        resultProjection(ret)
      }

      /**
       * Searches the streamed iterator for the next row that has at least one match in hashtable.
       *
       * @return true if the search is successful, and false if the streamed iterator runs out of
       *         tuples.
       */
      private final def fetchNext(): Boolean = {
        currentHashMatches = null
        currentMatchPosition = -1

        while (currentHashMatches == null && streamIter.hasNext) {
          currentStreamedRow = streamIter.next()
          numStreamRows += 1
          val key = joinKeys(currentStreamedRow)
          if (!key.anyNull) {
            currentHashMatches = hashedRelation.get(key)
          }
        }

        if (currentHashMatches == null) {
          false
        } else {
          currentMatchPosition = 0
          true
        }
      }
    }
  }
}
