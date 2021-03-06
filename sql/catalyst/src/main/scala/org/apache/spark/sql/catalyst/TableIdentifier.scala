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

package org.apache.spark.sql.catalyst

/**
 * Identifies a `table` in `database`.  If `database` is not defined, the current database is used.
 * 表示一个表名字,表名字一定存在,库名字可能存在,因此库名字是Option
 */
private[sql] case class TableIdentifier(table: String, database: Option[String] = None) {
  def withDatabase(database: String): TableIdentifier = this.copy(database = Some(database)) //仅仅赋值给database属性

  def toSeq: Seq[String] = database.toSeq :+ table //库+表组成集合

  override def toString: String = quotedString //产生 `database`.`table` 格式

  def quotedString: String = toSeq.map("`" + _ + "`").mkString(".") //加入``,即`database`.`table`

  def unquotedString: String = toSeq.mkString(".") //database.table形式的正常格式
}
