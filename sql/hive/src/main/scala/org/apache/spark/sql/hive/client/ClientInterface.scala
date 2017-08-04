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

package org.apache.spark.sql.hive.client

import java.io.PrintStream
import java.util.{Map => JMap}

import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchTableException}
import org.apache.spark.sql.catalyst.expressions.Expression

//该文件表示一个客户端接口,在一个接口存在的情况下,如何了解hive的全部表的信息情况

//表示一个数据库
private[hive] case class HiveDatabase(
    name: String,//数据库名字
    location: String)//数据库根路径

private[hive] abstract class TableType { val name: String } //数据库表类型
private[hive] case object ExternalTable extends TableType { override val name = "EXTERNAL_TABLE" }//外部表
private[hive] case object IndexTable extends TableType { override val name = "INDEX_TABLE" }//索引表
private[hive] case object ManagedTable extends TableType { override val name = "MANAGED_TABLE" }//内部表
private[hive] case object VirtualView extends TableType { override val name = "VIRTUAL_VIEW" }//虚拟表

// TODO: Use this for Tables and Partitions 表示一个表或者分区的详细信息
private[hive] case class HiveStorageDescriptor(
    location: String,//表所在路径
    inputFormat: String,//输入格式
    outputFormat: String,//输出格式
    serde: String,//序列化
    serdeProperties: Map[String, String])//序列化属性

private[hive] case class HivePartition(
    values: Seq[String],
    storage: HiveStorageDescriptor)

private[hive] case class HiveColumn(name: String, hiveType: String, comment: String) //表示一个hive的列---列名字、类型、备注
//表示一个hive的表
private[hive] case class HiveTable(
    specifiedDatabase: Option[String],//表所属数据库
    name: String,//表名
    schema: Seq[HiveColumn],//列的属性集合
    partitionColumns: Seq[HiveColumn],//分区列集合
    properties: Map[String, String],//表的配置信息
    serdeProperties: Map[String, String],//序列化配置信息
    tableType: TableType,//表的类型
    location: Option[String] = None,//表的hdfs路径
    inputFormat: Option[String] = None,//表的输入格式
    outputFormat: Option[String] = None,//表的输出格式
    serde: Option[String] = None,//序列化信息
    viewText: Option[String] = None) {

  //设置一个hive的接口实例
  @transient
  private[client] var client: ClientInterface = _

  private[client] def withClient(ci: ClientInterface): this.type = {
    client = ci
    this
  }

  def database: String = specifiedDatabase.getOrElse(sys.error("database not resolved")) //表所属数据库

  def isPartitioned: Boolean = partitionColumns.nonEmpty //是否是分区表

  def getAllPartitions: Seq[HivePartition] = client.getAllPartitions(this) //返回表的所有的分区集合

  def getPartitions(predicates: Seq[Expression]): Seq[HivePartition] =
    client.getPartitionsByFilter(this, predicates) //在分区表中进行filter过滤,选择合适的分区表集合

  // Hive does not support backticks when passing names to the client.
  def qualifiedName: String = s"$database.$name" //数据库.table名
}

/**
 * An externally visible interface to the Hive client.
 * hive的客户端外部可见的接口
 * This interface is shared across both the
 * internal and external classloaders for a given version of Hive and thus must expose only
 * shared classes.
 */
private[hive] trait ClientInterface {

  /** Returns the Hive Version of this client.
    * hive的版本号
    **/
  def version: HiveVersion

  /** Returns the configuration for the given key in the current session.
    * 返回当前session对应的key对应的值
    **/
  def getConf(key: String, defaultValue: String): String

  /**
   * Runs a HiveQL command using Hive, returning the results as a list of strings.  Each row will
   * result in one string.
   * 运行一个hive的命令sql,返回值中每一行是一个set的元素
   */
  def runSqlHive(sql: String): Seq[String]

  //设置三个输出流
  def setOut(stream: PrintStream): Unit
  def setInfo(stream: PrintStream): Unit
  def setError(stream: PrintStream): Unit

  /** Returns the names of all tables in the given database.
    * 获取全部数据库下所有的表集合
    **/
  def listTables(dbName: String): Seq[String]

  /** Returns the name of the active database.
    * 返回当前数据库
    **/
  def currentDatabase: String

  /** Returns the metadata for specified database, throwing an exception if it doesn't exist
    * 返回一个数据库对象
    **/
  def getDatabase(name: String): HiveDatabase = {
    getDatabaseOption(name).getOrElse(throw new NoSuchDatabaseException)
  }

  /** Returns the metadata for a given database, or None if it doesn't exist.
    * 返回一个数据库对象
    **/
  def getDatabaseOption(name: String): Option[HiveDatabase]

  /** Returns the specified table, or throws [[NoSuchTableException]].
    * 返回table对象
    **/
  def getTable(dbName: String, tableName: String): HiveTable = {
    getTableOption(dbName, tableName).getOrElse(throw new NoSuchTableException)
  }

  /** Returns the metadata for the specified table or None if it doens't exist.
    * 返回table对象
    **/
  def getTableOption(dbName: String, tableName: String): Option[HiveTable]

  /** Creates a table with the given metadata.
    * 创建一个hive表
    **/
  def createTable(table: HiveTable): Unit

  /** Updates the given table with new metadata.
    * 更新hive表的元数据
    **/
  def alterTable(table: HiveTable): Unit

  /** Creates a new database with the given name.
    * 创建hive的一个数据库
    **/
  def createDatabase(database: HiveDatabase): Unit

  /** Returns the specified paritition or None if it does not exist.
    * 返回hive的一个分区
    **/
  def getPartitionOption(
      hTable: HiveTable,//hive的表
      partitionSpec: JMap[String, String]):Option[HivePartition] //分区信息参数


  /** Returns all partitions for the given table.
    * 返回表的所有的分区集合
    **/
  def getAllPartitions(hTable: HiveTable): Seq[HivePartition]

  /** Returns partitions filtered by predicates for the given table.
    * 使用表达式过滤,保留过滤后的分区集合
    **/
  def getPartitionsByFilter(hTable: HiveTable, predicates: Seq[Expression]): Seq[HivePartition]

  /** Loads a static partition into an existing table.
    * 加载一个静态的分区
    **/
  def loadPartition(
      loadPath: String,
      tableName: String,
      partSpec: java.util.LinkedHashMap[String, String], // Hive relies on LinkedHashMap ordering
      replace: Boolean,
      holdDDLTime: Boolean,
      inheritTableSpecs: Boolean,
      isSkewedStoreAsSubdir: Boolean): Unit

  /** Loads data into an existing table.
    * 加载数据到一个表里面
    **/
  def loadTable(
      loadPath: String, // TODO URI 数据路径
      tableName: String,//加载到哪个表里面
      replace: Boolean,
      holdDDLTime: Boolean): Unit

  /** Loads new dynamic partitions into an existing table. */
  def loadDynamicPartitions(
      loadPath: String,
      tableName: String,
      partSpec: java.util.LinkedHashMap[String, String], // Hive relies on LinkedHashMap ordering
      replace: Boolean,
      numDP: Int,
      holdDDLTime: Boolean,
      listBucketingEnabled: Boolean): Unit

  /** Used for testing only.  Removes all metadata from this instance of Hive. */
  def reset(): Unit
}
