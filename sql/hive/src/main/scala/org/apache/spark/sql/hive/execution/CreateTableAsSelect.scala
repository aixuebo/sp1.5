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

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan}
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.hive.client.{HiveColumn, HiveTable}
import org.apache.spark.sql.hive.{HiveContext, HiveMetastoreTypes, MetastoreRelation}
import org.apache.spark.sql.{AnalysisException, Row, SQLContext}

/**
 * Create table and insert the query result into it.
 * 创建一个表,并且将查询的结果插入该表中
 * @param tableDesc the Table Describe, which may contains serde, storage handler etc.
 * @param query the query whose result will be insert into the new relation
 * @param allowExisting allow continue working if it's already exists, otherwise
 *                      raise exception
 */
private[hive]
case class CreateTableAsSelect(
    tableDesc: HiveTable,//要插入的表
    query: LogicalPlan,//select的查询计划
    allowExisting: Boolean)
  extends RunnableCommand {

  def database: String = tableDesc.database
  def tableName: String = tableDesc.name

  override def children: Seq[LogicalPlan] = Seq(query)

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val hiveContext = sqlContext.asInstanceOf[HiveContext]
    //获取新创建表的元数据
    lazy val metastoreRelation: MetastoreRelation = {
      import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
      import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe
      import org.apache.hadoop.io.Text
      import org.apache.hadoop.mapred.TextInputFormat

      val withFormat =
        tableDesc.copy(
          inputFormat =
            tableDesc.inputFormat.orElse(Some(classOf[TextInputFormat].getName)),
          outputFormat =
            tableDesc.outputFormat
              .orElse(Some(classOf[HiveIgnoreKeyTextOutputFormat[Text, Text]].getName)),
          serde = tableDesc.serde.orElse(Some(classOf[LazySimpleSerDe].getName())))

      val withSchema = if (withFormat.schema.isEmpty) {//如果没有输出,则要将select的内容作为输出
        // Hive doesn't support specifying the column list for target table in CTAS
        // However we don't think SparkSQL should follow that.
        tableDesc.copy(schema =
        query.output.map(c =>
          HiveColumn(c.name, HiveMetastoreTypes.toMetastoreType(c.dataType), null)))
      } else {
        withFormat
      }

      hiveContext.catalog.client.createTable(withSchema) //创建一个hive表

      // Get the Metastore Relation
      hiveContext.catalog.lookupRelation(Seq(database, tableName), None) match {
        case r: MetastoreRelation => r
      }//获取该表的元数据
    }
    // TODO ideally, we should get the output data ready first and then
    // add the relation into catalog, just in case of failure occurs while data
    // processing.
    if (hiveContext.catalog.tableExists(Seq(database, tableName))) {//说明表存在
      if (allowExisting) {
        // table already exists, will do nothing, to keep consistent with Hive 因为该表已经存在了,则什么也不做
      } else {
        throw new AnalysisException(s"$database.$tableName already exists.")
      }
    } else {//说明表不存在
      hiveContext.executePlan(InsertIntoTable(metastoreRelation, Map(), query, true, false)).toRdd
    }

    Seq.empty[Row]
  }

  override def argString: String = {
    s"[Database:$database, TableName: $tableName, InsertIntoHiveTable]"
  }
}
