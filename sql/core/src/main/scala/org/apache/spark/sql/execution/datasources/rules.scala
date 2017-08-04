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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalyst.analysis.{Catalog, EliminateSubQueries}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.sources.{BaseRelation, HadoopFsRelation, InsertableRelation}

/**
 * A rule to do pre-insert data type casting and field renaming. Before we insert into
 * an [[InsertableRelation]], we will use this rule to make sure that
 * the columns to be inserted have the correct data type and fields have the correct names.
 * 插入insert语法之前.保证select中的字段与insert要的字段的数据类型是相同的,如果不同则要强转。
 * 也要保证name属性名字是相同的,如果不同,则对select部分添加别名,别名的name就是insert的name
 */
private[sql] object PreInsertCastAndRename extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      // Wait until children are resolved.
      case p: LogicalPlan if !p.childrenResolved => p

      // We are inserting into an InsertableRelation or HadoopFsRelation.
      case i @ InsertIntoTable(
      l @ LogicalRelation(_: InsertableRelation | _: HadoopFsRelation), _, child, _, _) => { //child表示具体的select,而LogicalRelation表示insert into的内容,即是存储到一个table表,还是存储到HDFS上
        // First, make sure the data to be inserted have the same number of fields with the
        // schema of the relation.确保插入的数据的域数量是与schema相同的
        if (l.output.size != child.output.size) {//说明插入的schema数量不匹配
          sys.error(
            s"$l requires that the query in the SELECT clause of the INSERT INTO/OVERWRITE " +
              s"statement generates the same number of columns as its schema.")
        }
        castAndRenameChildOutput(i, l.output, child) //i表示最终插入的表,l.out表示最终插入表的schema输出属性,child表示select中对应的逻辑计划
      }
  }

  /** If necessary, cast data types and rename fields to the expected types and names.
    * 如果需要,强制转换数据类型以及更改属性别名到期望的类型
    **/
  def castAndRenameChildOutput(
      insertInto: InsertIntoTable,//insert into要插入的表
      expectedOutput: Seq[Attribute],//insert into里面定义的字段属性
      child: LogicalPlan): InsertIntoTable = {//select中的逻辑计划
    val newChildOutput = expectedOutput.zip(child.output).map {//期待的和select真实的属性一一映射
      case (expected, actual) => //前者是insert的字段,后者是数据表的真实字段
        val needCast = !expected.dataType.sameType(actual.dataType) //说明类型不一样
        // We want to make sure the filed names in the data to be inserted exactly match
        // names in the schema.
        val needRename = expected.name != actual.name //说明命名都不一样
        //返回每一个select字段的强制转换类型,或者更改别名
        (needCast, needRename) match {
          case (true, _) => Alias(Cast(actual, expected.dataType), expected.name)() //说明需要强转,并且有一个别名,就是insert的name即可
          case (false, true) => Alias(actual, expected.name)() //说明不需要强转,只需要更改别名
          case (_, _) => actual //说明类型和别名都相同,因此不需要增加别名修改
        }
    }

    if (newChildOutput == child.output) {//说明类型,和属性name都能匹配成功,没有变化,因此保留原样
      insertInto
    } else {
      insertInto.copy(child = Project(newChildOutput, child))//说明类型或者属性name有变化,因此重新赋值-----只是改变select部分类型以及name 别名即可
    }
  }
}

/**
 * A rule to do various checks before inserting into or writing to a data source table.
 * 在insert into前,各种校验规则
 */
private[sql] case class PreWriteCheck(catalog: Catalog) extends (LogicalPlan => Unit) {
  def failAnalysis(msg: String): Unit = { throw new AnalysisException(msg) } //打印错误异常信息

  def apply(plan: LogicalPlan): Unit = {
    plan.foreach {//对计划本身,以及子子孙孙都做以下的函数校验
      case i @ logical.InsertIntoTable(
        l @ LogicalRelation(t: InsertableRelation), partition, query, overwrite, ifNotExists) => //说明此时是insert into语法,插入到一个表中
        // Right now, we do not support insert into a data source table with partition specs.
        //目前我们不支持插入数据到一个指定的分区表中
        if (partition.nonEmpty) {//有分区信息,因此抛异常
          failAnalysis(s"Insert into a partition is not allowed because $l is not partitioned.")//因为l表不是一个分区表.因此分区信息存在的时候抛异常
        } else {//说明没有设置分区信息
          // Get all input data source relations of the query.
          val srcRelations = query.collect {//收集select中依赖的表
            case LogicalRelation(src: BaseRelation) => src
          }
          if (srcRelations.contains(t)) {//说明本身还依赖自己,因此抛异常
            failAnalysis(
              "Cannot insert overwrite into table that is also being read from.")
          } else {
            // OK
          }
        }

      case logical.InsertIntoTable(
        LogicalRelation(r: HadoopFsRelation), part, query, overwrite, _) => //说明此时是insert into语法,插入到HDFS中
        // We need to make sure the partition columns specified by users do match partition
        // columns of the relation.
        val existingPartitionColumns = r.partitionColumns.fieldNames.toSet
        val specifiedPartitionColumns = part.keySet
        if (existingPartitionColumns != specifiedPartitionColumns) {//确保分区字段是存在的,并且数量是相同的
          failAnalysis(s"Specified partition columns " +
            s"(${specifiedPartitionColumns.mkString(", ")}) " +
            s"do not match the partition columns of the table. Please use " +
            s"(${existingPartitionColumns.mkString(", ")}) as the partition columns.")
        } else {
          // OK
        }

        // Get all input data source relations of the query.
        val srcRelations = query.collect {//获取该表依赖的表
          case LogicalRelation(src: BaseRelation) => src
        }
        if (srcRelations.contains(r)) {//自己查询自己是不允许的
          failAnalysis(
            "Cannot insert overwrite into table that is also being read from.")
        } else {
          // OK
        }

      case logical.InsertIntoTable(l: LogicalRelation, _, _, _, _) =>
        // The relation in l is not an InsertableRelation.
        failAnalysis(s"$l does not allow insertion.")

      case logical.InsertIntoTable(t, _, _, _, _) =>
        if (!t.isInstanceOf[LeafNode] || t == OneRowRelation || t.isInstanceOf[LocalRelation]) {
          failAnalysis(s"Inserting into an RDD-based table is not allowed.")
        } else {
          // OK
        }

      case CreateTableUsingAsSelect(tableIdent, _, _, _, SaveMode.Overwrite, _, query) => //CTAS方式,暂时不考虑
        // When the SaveMode is Overwrite, we need to check if the table is an input table of
        // the query. If so, we will throw an AnalysisException to let users know it is not allowed.
        if (catalog.tableExists(tableIdent.toSeq)) {
          // Need to remove SubQuery operator.
          EliminateSubQueries(catalog.lookupRelation(tableIdent.toSeq)) match {
            // Only do the check if the table is a data source table
            // (the relation is a BaseRelation).
            case l @ LogicalRelation(dest: BaseRelation) =>
              // Get all input data source relations of the query.
              val srcRelations = query.collect {
                case LogicalRelation(src: BaseRelation) => src
              }
              if (srcRelations.contains(dest)) {
                failAnalysis(
                  s"Cannot overwrite table $tableIdent that is also being read from.")
              } else {
                // OK
              }

            case _ => // OK
          }
        } else {
          // OK
        }

      case _ => // OK
    }
  }
}
