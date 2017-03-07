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

import java.util.ServiceLoader

import scala.collection.JavaConversions._
import scala.language.{existentials, implicitConversions}
import scala.util.{Success, Failure, Try}

import org.apache.hadoop.fs.Path

import org.apache.spark.Logging
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.{DataFrame, SaveMode, AnalysisException, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{CalendarIntervalType, StructType}
import org.apache.spark.util.Utils


case class ResolvedDataSource(provider: Class[_], relation: BaseRelation)


object ResolvedDataSource extends Logging {

  /** A map to maintain backward compatibility in case we move data sources around.
    * 文件格式与解析类映射
    **/
  private val backwardCompatibilityMap = Map(
    "org.apache.spark.sql.jdbc" -> classOf[jdbc.DefaultSource].getCanonicalName,
    "org.apache.spark.sql.jdbc.DefaultSource" -> classOf[jdbc.DefaultSource].getCanonicalName,
    "org.apache.spark.sql.json" -> classOf[json.DefaultSource].getCanonicalName,
    "org.apache.spark.sql.json.DefaultSource" -> classOf[json.DefaultSource].getCanonicalName,
    "org.apache.spark.sql.parquet" -> classOf[parquet.DefaultSource].getCanonicalName,
    "org.apache.spark.sql.parquet.DefaultSource" -> classOf[parquet.DefaultSource].getCanonicalName
  )

  /** Given a provider name, look up the data source class definition.
    * 找到对应的data source的解析类
    **/
  def lookupDataSource(provider0: String): Class[_] = {
    val provider = backwardCompatibilityMap.getOrElse(provider0, provider0)
    val provider2 = s"$provider.DefaultSource"
    val loader = Utils.getContextOrSparkClassLoader
    val serviceLoader = ServiceLoader.load(classOf[DataSourceRegister], loader)

    serviceLoader.iterator().filter(_.shortName().equalsIgnoreCase(provider)).toList match {
      /** the provider format did not match any given registered aliases */
      case Nil => Try(loader.loadClass(provider)).orElse(Try(loader.loadClass(provider2))) match {//没有匹配的
        case Success(dataSource) => dataSource
        case Failure(error) =>
          if (provider.startsWith("org.apache.spark.sql.hive.orc")) {
            throw new ClassNotFoundException(
              "The ORC data source must be used with Hive support enabled.", error)
          } else {
            throw new ClassNotFoundException(
              s"Failed to load class for data source: $provider.", error)
          }
      }
      /** there is exactly one registered alias 只有一个匹配的*/
      case head :: Nil => head.getClass
      /** There are multiple registered aliases for the input */
      case sources => sys.error(s"Multiple sources found for $provider, " + //说明有多个匹配的,则打印错误信息
        s"(${sources.map(_.getClass.getName).mkString(", ")}), " +
        "please specify the fully qualified class name.")
    }
  }

  /** Create a [[ResolvedDataSource]] for reading data in. */
  def apply(
      sqlContext: SQLContext,
      userSpecifiedSchema: Option[StructType],
      partitionColumns: Array[String],//partition的列的数组集合
      provider: String,//source   json还是parquet还是orc等
      options: Map[String, String]) //额外配置中包含path路径属性
    : ResolvedDataSource = {
    val clazz: Class[_] = lookupDataSource(provider)//解析类
    def className: String = clazz.getCanonicalName

    val relation = userSpecifiedSchema match {
      case Some(schema: StructType) => clazz.newInstance() match {
        case dataSource: SchemaRelationProvider =>
          dataSource.createRelation(sqlContext, new CaseInsensitiveMap(options), schema)
        case dataSource: HadoopFsRelationProvider =>
          val maybePartitionsSchema = if (partitionColumns.isEmpty) {
            None
          } else {
            Some(partitionColumnsSchema(schema, partitionColumns))
          }

          val caseInsensitiveOptions = new CaseInsensitiveMap(options)
          val paths = {
            val patternPath = new Path(caseInsensitiveOptions("path"))
            val fs = patternPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
            val qualifiedPattern = patternPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
            SparkHadoopUtil.get.globPathIfNecessary(qualifiedPattern).map(_.toString).toArray
          }

          val dataSchema =
            StructType(schema.filterNot(f => partitionColumns.contains(f.name))).asNullable

          dataSource.createRelation(
            sqlContext,
            paths,
            Some(dataSchema),
            maybePartitionsSchema,
            caseInsensitiveOptions)
        case dataSource: org.apache.spark.sql.sources.RelationProvider =>
          throw new AnalysisException(s"$className does not allow user-specified schemas.")
        case _ =>
          throw new AnalysisException(s"$className is not a RelationProvider.")
      }

      case None => clazz.newInstance() match {//创建一个解析类
        case dataSource: RelationProvider =>
          dataSource.createRelation(sqlContext, new CaseInsensitiveMap(options))
        case dataSource: HadoopFsRelationProvider =>
          val caseInsensitiveOptions = new CaseInsensitiveMap(options)
          val paths = {
            val patternPath = new Path(caseInsensitiveOptions("path"))
            val fs = patternPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
            val qualifiedPattern = patternPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
            SparkHadoopUtil.get.globPathIfNecessary(qualifiedPattern).map(_.toString).toArray
          }
          dataSource.createRelation(sqlContext, paths, None, None, caseInsensitiveOptions)
        case dataSource: org.apache.spark.sql.sources.SchemaRelationProvider =>
          throw new AnalysisException(
            s"A schema needs to be specified when using $className.")
        case _ =>
          throw new AnalysisException(
            s"$className is neither a RelationProvider nor a FSBasedRelationProvider.")
      }
    }
    new ResolvedDataSource(clazz, relation)
  }

  private def partitionColumnsSchema(
      schema: StructType,
      partitionColumns: Array[String]): StructType = {
    StructType(partitionColumns.map { col =>
      schema.find(_.name == col).getOrElse {
        throw new RuntimeException(s"Partition column $col not found in schema $schema")
      }
    }).asNullable
  }

  /** Create a [[ResolvedDataSource]] for saving the content of the given DataFrame. */
  def apply(
      sqlContext: SQLContext,
      provider: String,//json还是parquet还是orc等
      partitionColumns: Array[String],//partition的列的数组集合
      mode: SaveMode,
      options: Map[String, String],//额外的配置信息集合
      data: DataFrame) //数据集合
      : ResolvedDataSource = {
    if (data.schema.map(_.dataType).exists(_.isInstanceOf[CalendarIntervalType])) {//不支持保存时间类型,因为时间类型是spark sql自己支持的类型,外界不支持
      throw new AnalysisException("Cannot save interval data type into external storage.")
    }
    val clazz: Class[_] = lookupDataSource(provider)
    val relation = clazz.newInstance() match {
      case dataSource: CreatableRelationProvider =>
        dataSource.createRelation(sqlContext, mode, options, data)
      case dataSource: HadoopFsRelationProvider =>
        // Don't glob path for the write path.  The contracts here are:
        //  1. Only one output path can be specified on the write path;
        //  2. Output path must be a legal HDFS style file system path;
        //  3. It's OK that the output path doesn't exist yet;
        val caseInsensitiveOptions = new CaseInsensitiveMap(options)
        val outputPath = {
          val path = new Path(caseInsensitiveOptions("path"))
          val fs = path.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
          path.makeQualified(fs.getUri, fs.getWorkingDirectory)
        }
        val dataSchema = StructType(data.schema.filterNot(f => partitionColumns.contains(f.name)))
        val r = dataSource.createRelation(
          sqlContext,
          Array(outputPath.toString),
          Some(dataSchema.asNullable),
          Some(partitionColumnsSchema(data.schema, partitionColumns)),
          caseInsensitiveOptions)

        // For partitioned relation r, r.schema's column ordering can be different from the column
        // ordering of data.logicalPlan (partition columns are all moved after data column).  This
        // will be adjusted within InsertIntoHadoopFsRelation.
        sqlContext.executePlan(
          InsertIntoHadoopFsRelation(
            r,
            data.logicalPlan,
            mode)).toRdd
        r
      case _ =>
        sys.error(s"${clazz.getCanonicalName} does not allow create table as select.")
    }
    ResolvedDataSource(clazz, relation)
  }
}