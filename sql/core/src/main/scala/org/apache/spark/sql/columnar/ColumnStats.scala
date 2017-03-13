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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, Attribute, AttributeMap, AttributeReference}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

//每一个属性对应的统计状态
//对象数组组合成一个统计行
private[sql] class ColumnStatisticsSchema(a: Attribute) extends Serializable {
  val upperBound = AttributeReference(a.name + ".upperBound", a.dataType, nullable = true)() //统计该列的最大值,值就是属性本身的类型,因此类型是a.dataType
  val lowerBound = AttributeReference(a.name + ".lowerBound", a.dataType, nullable = true)() //统计该列的最小值,值就是属性本身的类型,因此类型是a.dataType
  val nullCount = AttributeReference(a.name + ".nullCount", IntegerType, nullable = false)()  //统计该列的null出现的次数,类型是IntegerType,不允许为null
  val count = AttributeReference(a.name + ".count", IntegerType, nullable = false)() //统计该列null和非null值出现的总数,即该列有多少行数据,类型是IntegerType
  val sizeInBytes = AttributeReference(a.name + ".sizeInBytes", LongType, nullable = false)() //返回该列null和非null对应的总字节大小,类型是LongType

  val schema = Seq(lowerBound, upperBound, nullCount, count, sizeInBytes)
}

//循环表中所有的属性
private[sql] class PartitionStatistics(tableSchema: Seq[Attribute]) extends Serializable {
  val (forAttribute, schema) = {
    val allStats = tableSchema.map(a => a -> new ColumnStatisticsSchema(a)) //每一个属性对应一个ColumnStatisticsSchema对象,用于对该属性进行统计值存储,返回值是一个Map,key就是Attribute,value就是该属性的对应统计对象
    (AttributeMap(allStats), allStats.map(_._2.schema).foldLeft(Seq.empty[Attribute])(_ ++ _))
  }
}

/**
 * Used to collect statistical information when building in-memory columns.
 *
 * NOTE: we intentionally avoid using `Ordering[T]` to compare values here because `Ordering[T]`
 * brings significant performance penalty.
 */
private[sql] sealed trait ColumnStats extends Serializable {
  protected var count = 0 //一共null和非null的元素总数
  protected var nullCount = 0 //该属性是null的次数
  protected var sizeInBytes = 0L //该列所有值(null和非null值)占用的总字节

  /**
   * Gathers statistics information from `row(ordinal)`.
   */
  def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    if (row.isNullAt(ordinal)) { //说明该属性是null
      nullCount += 1 //该属性是null的次数+1
      // 4 bytes for null position
      sizeInBytes += 4 //字节数+4,因为要存储nll的位置,因此是4个字节
    }
    count += 1
  }

  /**
   * Column statistics represented as a single row, currently including closed lower bound, closed
   * upper bound and null count.
   */
  def collectedStatistics: GenericInternalRow
}

/**
 * A no-op ColumnStats only used for testing purposes.
 */
private[sql] class NoopColumnStats extends ColumnStats {
  override def gatherStats(row: InternalRow, ordinal: Int): Unit = super.gatherStats(row, ordinal)

  override def collectedStatistics: GenericInternalRow =
    new GenericInternalRow(Array[Any](null, null, nullCount, count, 0L))
}

private[sql] class BooleanColumnStats extends ColumnStats {
  protected var upper = false
  protected var lower = true

  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      val value = row.getBoolean(ordinal)
      if (value > upper) upper = value
      if (value < lower) lower = value
      sizeInBytes += BOOLEAN.defaultSize
    }
  }

  override def collectedStatistics: GenericInternalRow =
    new GenericInternalRow(Array[Any](lower, upper, nullCount, count, sizeInBytes))
}

private[sql] class ByteColumnStats extends ColumnStats {
  protected var upper = Byte.MinValue
  protected var lower = Byte.MaxValue

  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      val value = row.getByte(ordinal)
      if (value > upper) upper = value
      if (value < lower) lower = value
      sizeInBytes += BYTE.defaultSize
    }
  }

  override def collectedStatistics: GenericInternalRow =
    new GenericInternalRow(Array[Any](lower, upper, nullCount, count, sizeInBytes))
}

private[sql] class ShortColumnStats extends ColumnStats {
  protected var upper = Short.MinValue
  protected var lower = Short.MaxValue

  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      val value = row.getShort(ordinal)
      if (value > upper) upper = value
      if (value < lower) lower = value
      sizeInBytes += SHORT.defaultSize
    }
  }

  override def collectedStatistics: GenericInternalRow =
    new GenericInternalRow(Array[Any](lower, upper, nullCount, count, sizeInBytes))
}

private[sql] class IntColumnStats extends ColumnStats {
  protected var upper = Int.MinValue
  protected var lower = Int.MaxValue

  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal) //对null的数据进行处理
    if (!row.isNullAt(ordinal)) {//说明该数据不是null
      val value = row.getInt(ordinal) //获取该数据值
      //设置最大值和最小值
      if (value > upper) upper = value
      if (value < lower) lower = value
      sizeInBytes += INT.defaultSize //追加总字节数
    }
  }

  override def collectedStatistics: GenericInternalRow =
    new GenericInternalRow(Array[Any](lower, upper, nullCount, count, sizeInBytes)) //对象数组组合成一个统计行
}

private[sql] class LongColumnStats extends ColumnStats {
  protected var upper = Long.MinValue
  protected var lower = Long.MaxValue

  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      val value = row.getLong(ordinal)
      if (value > upper) upper = value
      if (value < lower) lower = value
      sizeInBytes += LONG.defaultSize
    }
  }

  override def collectedStatistics: GenericInternalRow =
    new GenericInternalRow(Array[Any](lower, upper, nullCount, count, sizeInBytes))
}

private[sql] class FloatColumnStats extends ColumnStats {
  protected var upper = Float.MinValue
  protected var lower = Float.MaxValue

  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      val value = row.getFloat(ordinal)
      if (value > upper) upper = value
      if (value < lower) lower = value
      sizeInBytes += FLOAT.defaultSize
    }
  }

  override def collectedStatistics: GenericInternalRow =
    new GenericInternalRow(Array[Any](lower, upper, nullCount, count, sizeInBytes))
}

private[sql] class DoubleColumnStats extends ColumnStats {
  protected var upper = Double.MinValue
  protected var lower = Double.MaxValue

  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      val value = row.getDouble(ordinal)
      if (value > upper) upper = value
      if (value < lower) lower = value
      sizeInBytes += DOUBLE.defaultSize
    }
  }

  override def collectedStatistics: GenericInternalRow =
    new GenericInternalRow(Array[Any](lower, upper, nullCount, count, sizeInBytes))
}

private[sql] class StringColumnStats extends ColumnStats {
  protected var upper: UTF8String = null
  protected var lower: UTF8String = null

  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      val value = row.getUTF8String(ordinal)
      if (upper == null || value.compareTo(upper) > 0) upper = value
      if (lower == null || value.compareTo(lower) < 0) lower = value
      sizeInBytes += STRING.actualSize(row, ordinal)
    }
  }

  override def collectedStatistics: GenericInternalRow =
    new GenericInternalRow(Array[Any](lower, upper, nullCount, count, sizeInBytes))
}

private[sql] class BinaryColumnStats extends ColumnStats {
  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      sizeInBytes += BINARY.actualSize(row, ordinal)
    }
  }

  override def collectedStatistics: GenericInternalRow =
    new GenericInternalRow(Array[Any](null, null, nullCount, count, sizeInBytes))
}

private[sql] class FixedDecimalColumnStats(precision: Int, scale: Int) extends ColumnStats {
  protected var upper: Decimal = null
  protected var lower: Decimal = null

  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      val value = row.getDecimal(ordinal, precision, scale)
      if (upper == null || value.compareTo(upper) > 0) upper = value
      if (lower == null || value.compareTo(lower) < 0) lower = value
      sizeInBytes += FIXED_DECIMAL.defaultSize
    }
  }

  override def collectedStatistics: GenericInternalRow =
    new GenericInternalRow(Array[Any](lower, upper, nullCount, count, sizeInBytes))
}

private[sql] class GenericColumnStats(dataType: DataType) extends ColumnStats {
  val columnType = GENERIC(dataType)

  override def gatherStats(row: InternalRow, ordinal: Int): Unit = {
    super.gatherStats(row, ordinal)
    if (!row.isNullAt(ordinal)) {
      sizeInBytes += columnType.actualSize(row, ordinal)
    }
  }

  override def collectedStatistics: GenericInternalRow =
    new GenericInternalRow(Array[Any](null, null, nullCount, count, sizeInBytes))
}

private[sql] class DateColumnStats extends IntColumnStats

private[sql] class TimestampColumnStats extends LongColumnStats
