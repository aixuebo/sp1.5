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

import java.util.Properties

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.{SqlParser, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoTable
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.execution.datasources.{CreateTableUsingAsSelect, ResolvedDataSource}
import org.apache.spark.sql.sources.HadoopFsRelation


/**
 * :: Experimental ::
 * Interface used to write a [[DataFrame]] to external storage systems (e.g. file systems,
 * key-value stores, etc). Use [[DataFrame.write]] to access this.
 * 一个接口,将DataFrame的数据内容,输出到外部存储系统中
 * @since 1.4.0
 * 内部维护了一个DF,说明该类是针对一个DF进一步的处理,主要用于写数据的处理
 *
 * 1.写出方式:追加还是覆盖等
 * 2.最终文件的输出格式,json还是orc还是parquet等
 * 3.一个map表示需要配置的额外属性key=value,比如path 即输出的文件路径
 */
@Experimental
final class DataFrameWriter private[sql](df: DataFrame) {

  /**
   * Specifies the behavior when data or table already exists. Options include:
   *   - `SaveMode.Overwrite`: overwrite the existing data.
   *   - `SaveMode.Append`: append the data.
   *   - `SaveMode.Ignore`: ignore the operation (i.e. no-op).
   *   - `SaveMode.ErrorIfExists`: default option, throw an exception at runtime.
   *
   * @since 1.4.0
   */
  def mode(saveMode: SaveMode): DataFrameWriter = {
    this.mode = saveMode
    this
  }

  /**
   * Specifies the behavior when data or table already exists. Options include:
   *   - `overwrite`: overwrite the existing data.
   *   - `append`: append the data.
   *   - `ignore`: ignore the operation (i.e. no-op).
   *   - `error`: default option, throw an exception at runtime.
   *
   * @since 1.4.0
   */
  def mode(saveMode: String): DataFrameWriter = {
    this.mode = saveMode.toLowerCase match {
      case "overwrite" => SaveMode.Overwrite
      case "append" => SaveMode.Append
      case "ignore" => SaveMode.Ignore
      case "error" | "default" => SaveMode.ErrorIfExists
      case _ => throw new IllegalArgumentException(s"Unknown save mode: $saveMode. " +
        "Accepted modes are 'overwrite', 'append', 'ignore', 'error'.")
    }
    this
  }

  /**
   * Specifies the underlying output data source. Built-in options include "parquet", "json", etc.
   * 最终文件的输出格式,json还是orc还是parquet等
   * @since 1.4.0
   */
  def format(source: String): DataFrameWriter = {
    this.source = source
    this
  }

  /**
   * Adds an output option for the underlying data source.
   * 额外的属性信息集合
   * @since 1.4.0
   */
  def option(key: String, value: String): DataFrameWriter = {
    this.extraOptions += (key -> value)
    this
  }

  /**
   * (Scala-specific) Adds output options for the underlying data source.
   *
   * @since 1.4.0
   */
  def options(options: scala.collection.Map[String, String]): DataFrameWriter = {
    this.extraOptions ++= options
    this
  }

  /**
   * Adds output options for the underlying data source.
   *
   * @since 1.4.0
   */
  def options(options: java.util.Map[String, String]): DataFrameWriter = {
    this.options(scala.collection.JavaConversions.mapAsScalaMap(options))
    this
  }

  /**
   * Partitions the output by the given columns on the file system. If specified, the output is
   * laid out on the file system similar to Hive's partitioning scheme.
   *
   * This is only applicable for Parquet at the moment.
   * 通过哪些列集合进行partition分组
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def partitionBy(colNames: String*): DataFrameWriter = {
    this.partitioningColumns = Option(colNames)
    this
  }

  /**
   * Saves the content of the [[DataFrame]] at the specified path.
   * 将数据保存在path下
   * @since 1.4.0
   */
  def save(path: String): Unit = {
    this.extraOptions += ("path" -> path)
    save()
  }

  /**
   * Saves the content of the [[DataFrame]] as the specified table.
   *
   * @since 1.4.0
   */
  def save(): Unit = {
    ResolvedDataSource(
      df.sqlContext,
      source,
      partitioningColumns.map(_.toArray).getOrElse(Array.empty[String]),//将partition的列的Seq集合转换成数组,如果没有列的话,则返回空数组.即此时就是partition的列的数组集合
      mode,
      extraOptions.toMap,
      df)
  }

  /**
   * Inserts the content of the [[DataFrame]] to the specified table. It requires that
   * the schema of the [[DataFrame]] is the same as the schema of the table.
   *
   * Because it inserts data to an existing table, format or options will be ignored.
   *
   * @since 1.4.0
   * 将DataFrame的内容插入到给定的表中
   * 注意 要求给定的表的schema与DataFrame的schema要相同即可
   *
   * 参数为database.tableName形式
   */
  def insertInto(tableName: String): Unit = {
    insertInto(new SqlParser().parseTableIdentifier(tableName))
  }

  private def insertInto(tableIdent: TableIdentifier): Unit = {
    val partitions = partitioningColumns.map(_.map(col => col -> (None: Option[String])).toMap)//因为partitioningColumns是Seq对象,因此对该对象进行map,每一个列创建一个Map集合,即属性=value
    val overwrite = mode == SaveMode.Overwrite
    df.sqlContext.executePlan(
      InsertIntoTable(
        UnresolvedRelation(tableIdent.toSeq),
        partitions.getOrElse(Map.empty[String, Option[String]]),
        df.logicalPlan,
        overwrite,
        ifNotExists = false)).toRdd
  }

  /**
   * Saves the content of the [[DataFrame]] as the specified table.
   *
   * In the case the table already exists, behavior of this function depends on the
   * save mode, specified by the `mode` function (default to throwing an exception).
   * When `mode` is `Overwrite`, the schema of the [[DataFrame]] does not need to be
   * the same as that of the existing table.
   * When `mode` is `Append`, the schema of the [[DataFrame]] need to be
   * the same as that of the existing table, and format or options will be ignored.
   *
   * When the DataFrame is created from a non-partitioned [[HadoopFsRelation]] with a single input
   * path, and the data source provider can be mapped to an existing Hive builtin SerDe (i.e. ORC
   * and Parquet), the table is persisted in a Hive compatible format, which means other systems
   * like Hive will be able to read this table. Otherwise, the table is persisted in a Spark SQL
   * specific format.
   *
   * @since 1.4.0
   */
  def saveAsTable(tableName: String): Unit = {
    saveAsTable(new SqlParser().parseTableIdentifier(tableName))
  }

  private def saveAsTable(tableIdent: TableIdentifier): Unit = {
    val tableExists = df.sqlContext.catalog.tableExists(tableIdent.toSeq)

    (tableExists, mode) match {
      case (true, SaveMode.Ignore) =>
        // Do nothing

      case (true, SaveMode.ErrorIfExists) => //状态是表存在的时候不允许保存,因此要抛异常
        throw new AnalysisException(s"Table $tableIdent already exists.")

      case (true, SaveMode.Append) => //说明是追加数据,因此表的schema要与df的schema数据保持一致
        // If it is Append, we just ask insertInto to handle it. We will not use insertInto
        // to handle saveAsTable with Overwrite because saveAsTable can change the schema of
        // the table. But, insertInto with Overwrite requires the schema of data be the same
        // the schema of the table.
        insertInto(tableIdent)

      case _ => //因为不是追加,而是覆盖,因此不要求表的schema和df的schema一致,甚至表不存在也没问题,可以自己创建对应的表
        val cmd =
          CreateTableUsingAsSelect(
            tableIdent,
            source,
            temporary = false,
            partitioningColumns.map(_.toArray).getOrElse(Array.empty[String]),
            mode,
            extraOptions.toMap,
            df.logicalPlan)
        df.sqlContext.executePlan(cmd).toRdd
    }
  }

  /**
   * Saves the content of the [[DataFrame]] to a external database table via JDBC. In the case the
   * table already exists in the external database, behavior of this function depends on the
   * save mode, specified by the `mode` function (default to throwing an exception).
   *
   * Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash
   * your external database systems.
   *
   * @param url JDBC database url of the form `jdbc:subprotocol:subname`
   * @param table Name of the table in the external database.
   * @param connectionProperties JDBC database connection arguments, a list of arbitrary string
   *                             tag/value. Normally at least a "user" and "password" property
   *                             should be included.
   * 保存df的数据到jdbc数据库中
   */
  def jdbc(url: String, table: String, connectionProperties: Properties): Unit = {
    val props = new Properties()
    extraOptions.foreach { case (key, value) =>
      props.put(key, value)
    }
    // connectionProperties should override settings in extraOptions
    props.putAll(connectionProperties) //connectionProperties 可以覆盖存在的属性
    val conn = JdbcUtils.createConnection(url, props)

    try {
      var tableExists = JdbcUtils.tableExists(conn, table) //判断table是否存在

      if (mode == SaveMode.Ignore && tableExists) {
        return
      }

      if (mode == SaveMode.ErrorIfExists && tableExists) {
        sys.error(s"Table $table already exists.")
      }

      if (mode == SaveMode.Overwrite && tableExists) {
        JdbcUtils.dropTable(conn, table)
        tableExists = false
      }

      // Create the table if the table didn't exist.
      //说明table不存在,则创建该table
      if (!tableExists) {
        val schema = JdbcUtils.schemaString(df, url)
        val sql = s"CREATE TABLE $table ($schema)"
        conn.prepareStatement(sql).executeUpdate()
      }
    } finally {
      conn.close()
    }

    //保存df的数据到table中
    JdbcUtils.saveTable(df, url, table, props)
  }

  /**
   * Saves the content of the [[DataFrame]] in JSON format at the specified path.
   * This is equivalent to:
   * {{{
   *   format("json").save(path)
   * }}}
   *
   * @since 1.4.0
   */
  def json(path: String): Unit = format("json").save(path)

  /**
   * Saves the content of the [[DataFrame]] in Parquet format at the specified path.
   * This is equivalent to:
   * {{{
   *   format("parquet").save(path)
   * }}}
   *
   * @since 1.4.0
   */
  def parquet(path: String): Unit = format("parquet").save(path)

  /**
   * Saves the content of the [[DataFrame]] in ORC format at the specified path.
   * This is equivalent to:
   * {{{
   *   format("orc").save(path)
   * }}}
   *
   * @since 1.5.0
   * @note Currently, this method can only be used together with `HiveContext`.
   */
  def orc(path: String): Unit = format("orc").save(path)

  ///////////////////////////////////////////////////////////////////////////////////////
  // Builder pattern config options
  ///////////////////////////////////////////////////////////////////////////////////////

  private var source: String = df.sqlContext.conf.defaultDataSourceName

  private var mode: SaveMode = SaveMode.ErrorIfExists

  private var extraOptions = new scala.collection.mutable.HashMap[String, String] //额外的键值对集合

  private var partitioningColumns: Option[Seq[String]] = None //通过哪些列集合进行partition分组

}
