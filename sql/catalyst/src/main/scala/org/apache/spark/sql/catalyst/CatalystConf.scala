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

private[spark] trait CatalystConf {
  def caseSensitiveAnalysis: Boolean //敏感分析,true表示敏感,即table名字大小写是敏感的
}

/**
 * A trivial conf that is empty.  Used for testing when all
 * relations are already filled in and the analyser needs only to resolve attribute references.
 */
object EmptyConf extends CatalystConf {
  override def caseSensitiveAnalysis: Boolean = {
    throw new UnsupportedOperationException
  }
}

/** A CatalystConf that can be used for local testing.
  * 简单的配置文件.通过构造函数构造一个boolean类型,从而简单的实现
  **/
case class SimpleCatalystConf(caseSensitiveAnalysis: Boolean) extends CatalystConf
