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

package org.apache.spark.sql.catalyst.analysis

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{TableIdentifier, CatalystConf, EmptyConf}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery}

/**
 * Thrown by a catalog when a table cannot be found.  The analyzer will rethrow the exception
 * as an AnalysisException with the correct position information.
 */
class NoSuchTableException extends Exception //没有表异常

class NoSuchDatabaseException extends Exception //没有数据库异常

/**
 * An interface for looking up relations by name.  Used by an [[Analyzer]].
 * 目录接口,用于查找一个表名对应的逻辑计划
 * 相当于一个缓存
 */
trait Catalog {

  val conf: CatalystConf //配置文件

  def tableExists(tableIdentifier: Seq[String]): Boolean //该表是否存在

  //查获表名对应的逻辑计划,并且为表设置别名
  def lookupRelation(
      tableIdentifier: Seq[String],
      alias: Option[String] = None): LogicalPlan

  /**
   * Returns tuples of (tableName, isTemporary) for all tables in the given database.
   * isTemporary is a Boolean value indicates if a table is a temporary or not.
   * 返回在当前数据库下所有table名字和是否是临时表的元组
   */
  def getTables(databaseName: Option[String]): Seq[(String, Boolean)]

  def refreshTable(tableIdent: TableIdentifier): Unit

  // TODO: Refactor it in the work of SPARK-10104
  //注册一个表名和逻辑计划的关系
  def registerTable(tableIdentifier: Seq[String], plan: LogicalPlan): Unit

  // TODO: Refactor it in the work of SPARK-10104
  //取消一个表
  def unregisterTable(tableIdentifier: Seq[String]): Unit

  //取消所有的表
  def unregisterAllTables(): Unit

  // TODO: Refactor it in the work of SPARK-10104
  //对表名进行处理,让其都转换成大写
  protected def processTableIdentifier(tableIdentifier: Seq[String]): Seq[String] = {
    if (conf.caseSensitiveAnalysis) {//对大小写敏感
      tableIdentifier //保持原样
    } else {
      tableIdentifier.map(_.toLowerCase) //对大小写不敏感,都让其大写
    }
  }

  // TODO: Refactor it in the work of SPARK-10104
  //转换成datababses.table形式 或者table形式
  protected def getDbTableName(tableIdent: Seq[String]): String = {
    val size = tableIdent.size
    if (size <= 2) {
      tableIdent.mkString(".")
    } else {
      tableIdent.slice(size - 2, size).mkString(".")//只获取最后两个
    }
  }

  // TODO: Refactor it in the work of SPARK-10104
  //返回databases,table的元组
  protected def getDBTable(tableIdent: Seq[String]) : (Option[String], String) = {
    (tableIdent.lift(tableIdent.size - 2), tableIdent.last)
  }

  /**
   * It is not allowed to specifiy database name for tables stored in [[SimpleCatalog]].
   * We use this method to check it.
   * 校验table名字是否合法
   */
  protected def checkTableIdentifier(tableIdentifier: Seq[String]): Unit = {
    if (tableIdentifier.length > 1) {//必须要大于1长度,否则是非法的
      throw new AnalysisException("Specifying database name or other qualifiers are not allowed " +
        "for temporary tables. If the table name has dots (.) in it, please quote the " +
        "table name with backticks (`).")
    }
  }
}

class SimpleCatalog(val conf: CatalystConf) extends Catalog {
  val tables = new ConcurrentHashMap[String, LogicalPlan] //每一个表和该表的逻辑计划映射

  //注册一个表和逻辑计划映射
  override def registerTable(
      tableIdentifier: Seq[String],
      plan: LogicalPlan): Unit = {
    checkTableIdentifier(tableIdentifier) //校验合法性
    val tableIdent = processTableIdentifier(tableIdentifier) //对表名进行处理,让其都转换成大写
    tables.put(getDbTableName(tableIdent), plan)
  }

  override def unregisterTable(tableIdentifier: Seq[String]): Unit = {
    checkTableIdentifier(tableIdentifier)
    val tableIdent = processTableIdentifier(tableIdentifier) //对表名进行处理,让其都转换成大写
    tables.remove(getDbTableName(tableIdent))
  }

  //取消所有注册过的表
  override def unregisterAllTables(): Unit = {
    tables.clear()
  }

  //表是否存在
  override def tableExists(tableIdentifier: Seq[String]): Boolean = {
    checkTableIdentifier(tableIdentifier)
    val tableIdent = processTableIdentifier(tableIdentifier) //对表名进行处理,让其都转换成大写
    tables.containsKey(getDbTableName(tableIdent))
  }

  //找到对应的表,并且为表设置别名
  override def lookupRelation(
      tableIdentifier: Seq[String],
      alias: Option[String] = None): LogicalPlan = {
    checkTableIdentifier(tableIdentifier)
    val tableIdent = processTableIdentifier(tableIdentifier) //对表名进行处理,让其都转换成大写
    val tableFullName = getDbTableName(tableIdent) //获取表名
    val table = tables.get(tableFullName) //是否存在该表
    if (table == null) {
      sys.error(s"Table Not Found: $tableFullName")
    }
    val tableWithQualifiers = Subquery(tableIdent.last, table) //设置别名和逻辑计划,别名默认就是表名

    // If an alias was specified by the lookup, wrap the plan in a subquery so that attributes are
    // properly qualified with this alias.
    alias.map(a => Subquery(a, tableWithQualifiers)).getOrElse(tableWithQualifiers) //因为别名存在,则设置别名
  }

  //返回在当前数据库下所有table名字和是否是临时表的元组
  override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    val result = ArrayBuffer.empty[(String, Boolean)]
    for (name <- tables.keySet()) {
      result += ((name, true))
    }
    result
  }

  override def refreshTable(tableIdent: TableIdentifier): Unit = {
    throw new UnsupportedOperationException
  }
}

/**
 * A trait that can be mixed in with other Catalogs allowing specific tables to be overridden with
 * new logical plans.  This can be used to bind query result to virtual tables, or replace tables
 * with in-memory cached versions.  Note that the set of overrides is stored in memory and thus
 * lost when the JVM exits.
 */
trait OverrideCatalog extends Catalog {

  // TODO: This doesn't work when the database changes...
  val overrides = new mutable.HashMap[(Option[String], String), LogicalPlan]()

  override def registerTable(
                              tableIdentifier: Seq[String],
                              plan: LogicalPlan): Unit = {
    checkTableIdentifier(tableIdentifier)
    val tableIdent = processTableIdentifier(tableIdentifier) //对表名进行处理,让其都转换成大写
    overrides.put(getDBTable(tableIdent), plan)
  }

  override def unregisterTable(tableIdentifier: Seq[String]): Unit = {
    // A temporary tables only has a single part in the tableIdentifier.
    // If tableIdentifier has more than one parts, it is not a temporary table
    // and we do not need to do anything at here.
    if (tableIdentifier.length == 1) {
      val tableIdent = processTableIdentifier(tableIdentifier)
      overrides.remove(getDBTable(tableIdent))
    }
  }

  override def unregisterAllTables(): Unit = {
    overrides.clear()
  }

  abstract override def tableExists(tableIdentifier: Seq[String]): Boolean = {
    val tableIdent = processTableIdentifier(tableIdentifier) //对table名字进行大小写处理
    // A temporary tables only has a single part in the tableIdentifier.
    val overriddenTable = if (tableIdentifier.length > 1) {
      None: Option[LogicalPlan]
    } else {
      overrides.get(getDBTable(tableIdent)) //返回databases,table的元组
    }
    overriddenTable match {
      case Some(_) => true
      case None => super.tableExists(tableIdentifier)
    }
  }

  abstract override def lookupRelation(
      tableIdentifier: Seq[String],
      alias: Option[String] = None): LogicalPlan = {
    val tableIdent = processTableIdentifier(tableIdentifier)
    // A temporary tables only has a single part in the tableIdentifier.
    val overriddenTable = if (tableIdentifier.length > 1) {
      None: Option[LogicalPlan]
    } else {
      overrides.get(getDBTable(tableIdent))
    }
    val tableWithQualifers = overriddenTable.map(r => Subquery(tableIdent.last, r))

    // If an alias was specified by the lookup, wrap the plan in a subquery so that attributes are
    // properly qualified with this alias.
    val withAlias =
      tableWithQualifers.map(r => alias.map(a => Subquery(a, r)).getOrElse(r))

    withAlias.getOrElse(super.lookupRelation(tableIdentifier, alias))
  }

  abstract override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    // We always return all temporary tables.
    val temporaryTables = overrides.map {
      case ((_, tableName), _) => (tableName, true)
    }.toSeq

    temporaryTables ++ super.getTables(databaseName)
  }
}

/**
 * A trivial catalog that returns an error when a relation is requested.  Used for testing when all
 * relations are already filled in and the analyzer needs only to resolve attribute references.
 */
object EmptyCatalog extends Catalog {

  override val conf: CatalystConf = EmptyConf

  override def tableExists(tableIdentifier: Seq[String]): Boolean = {
    throw new UnsupportedOperationException
  }

  override def lookupRelation(
      tableIdentifier: Seq[String],
      alias: Option[String] = None): LogicalPlan = {
    throw new UnsupportedOperationException
  }

  override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    throw new UnsupportedOperationException
  }

  override def registerTable(tableIdentifier: Seq[String], plan: LogicalPlan): Unit = {
    throw new UnsupportedOperationException
  }

  override def unregisterTable(tableIdentifier: Seq[String]): Unit = {
    throw new UnsupportedOperationException
  }

  override def unregisterAllTables(): Unit = {}

  override def refreshTable(tableIdent: TableIdentifier): Unit = {
    throw new UnsupportedOperationException
  }
}
