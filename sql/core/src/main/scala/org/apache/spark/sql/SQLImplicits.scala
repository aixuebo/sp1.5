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

package org.apache.spark.sql

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificMutableRow
import org.apache.spark.sql.types.StructField
import org.apache.spark.unsafe.types.UTF8String

/**
 * A collection of implicit methods for converting common Scala objects into [[DataFrame]]s.
 */
private[sql] abstract class SQLImplicits {
  protected def _sqlContext: SQLContext

  /**
   * An implicit conversion that turns a Scala `Symbol` into a [[Column]].
   * @since 1.3.0
   * Symbol参数的列名字,转换成列名对象
   */
  implicit def symbolToColumn(s: Symbol): ColumnName = new ColumnName(s.name)

  /**
   * Creates a DataFrame from an RDD of Product (e.g. case classes, tuples).
   * @since 1.3.0
   * 如何将RDD转换成DataFrame
   */
  implicit def rddToDataFrameHolder[A <: Product : TypeTag](rdd: RDD[A]): DataFrameHolder = {
    DataFrameHolder(_sqlContext.createDataFrame(rdd))
  }

  /**
   * Creates a DataFrame from a local Seq of Product.
   * @since 1.3.0
   * 如果将本地的集合数据,转换成DataFrame
   */
  implicit def localSeqToDataFrameHolder[A <: Product : TypeTag](data: Seq[A]): DataFrameHolder =
  {
    DataFrameHolder(_sqlContext.createDataFrame(data))
  }

  // Do NOT add more implicit conversions. They are likely to break source compatibility by
  // making existing implicit conversions ambiguous. In particular, RDD[Double] is dangerous
  // because of [[DoubleRDDFunctions]].

  /**
   * Creates a single column DataFrame from an RDD[Int].
   * @since 1.3.0
   * 创建每一行只有一个int列的DataFrame对象
   */
  implicit def intRddToDataFrameHolder(data: RDD[Int]): DataFrameHolder = {
    val dataType = IntegerType //数据列只有一个列,并且是Int类型的
    val rows = data.mapPartitions { iter => //循环RDD每一个分区
      val row = new SpecificMutableRow(dataType :: Nil) //每一行只有一列
      iter.map { v =>
        row.setInt(0, v) //将该列的值转换成int,存储到row中
        row: InternalRow
      }
    }
    DataFrameHolder(
      _sqlContext.internalCreateDataFrame(rows, StructType(StructField("_1", dataType) :: Nil)))
  }

  /**
   * Creates a single column DataFrame from an RDD[Long].
   * @since 1.3.0
   * 创建每一行只有一个long列的DataFrame对象
   */
  implicit def longRddToDataFrameHolder(data: RDD[Long]): DataFrameHolder = {
    val dataType = LongType
    val rows = data.mapPartitions { iter =>
      val row = new SpecificMutableRow(dataType :: Nil)
      iter.map { v =>
        row.setLong(0, v)
        row: InternalRow
      }
    }
    DataFrameHolder(
      _sqlContext.internalCreateDataFrame(rows, StructType(StructField("_1", dataType) :: Nil)))
  }

  /**
   * Creates a single column DataFrame from an RDD[String].
   * @since 1.3.0
   * 创建每一行只有一个Strin列的DataFrame对象
   */
  implicit def stringRddToDataFrameHolder(data: RDD[String]): DataFrameHolder = {
    val dataType = StringType
    val rows = data.mapPartitions { iter =>
      val row = new SpecificMutableRow(dataType :: Nil)
      iter.map { v =>
        row.update(0, UTF8String.fromString(v))
        row: InternalRow
      }
    }
    DataFrameHolder(
      _sqlContext.internalCreateDataFrame(rows, StructType(StructField("_1", dataType) :: Nil)))
  }
}
