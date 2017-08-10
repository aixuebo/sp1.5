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

package org.apache.spark.sql.hive

import scala.collection.JavaConversions._
import scala.collection.mutable

import com.google.common.base.Objects
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.common.StatsSetupConst
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.Warehouse
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.ql.metadata._
import org.apache.hadoop.hive.ql.plan.TableDesc

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.{Catalog, MultiInstanceRelation, OverrideCatalog}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.{InternalRow, SqlParser, TableIdentifier}
import org.apache.spark.sql.execution.datasources.parquet.ParquetRelation
import org.apache.spark.sql.execution.datasources.{CreateTableUsingAsSelect, LogicalRelation, Partition => ParquetPartition, PartitionSpec, ResolvedDataSource}
import org.apache.spark.sql.execution.{FileRelation, datasources}
import org.apache.spark.sql.hive.client._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, SQLContext, SaveMode}

private[hive] case class HiveSerDe(
    inputFormat: Option[String] = None,
    outputFormat: Option[String] = None,
    serde: Option[String] = None)

private[hive] object HiveSerDe {
  /**
   * Get the Hive SerDe information from the data source abbreviation string or classname.
   *
   * @param source Currently the source abbreviation can be one of the following:
   *               SequenceFile, RCFile, ORC, PARQUET, and case insensitive.
   * @param hiveConf Hive Conf
   * @return HiveSerDe associated with the specified source
   */
  def sourceToSerDe(source: String, hiveConf: HiveConf): Option[HiveSerDe] = {
    val serdeMap = Map(
      "sequencefile" ->
        HiveSerDe(
          inputFormat = Option("org.apache.hadoop.mapred.SequenceFileInputFormat"),
          outputFormat = Option("org.apache.hadoop.mapred.SequenceFileOutputFormat")),

      "rcfile" ->
        HiveSerDe(
          inputFormat = Option("org.apache.hadoop.hive.ql.io.RCFileInputFormat"),
          outputFormat = Option("org.apache.hadoop.hive.ql.io.RCFileOutputFormat"),
          serde = Option(hiveConf.getVar(HiveConf.ConfVars.HIVEDEFAULTRCFILESERDE))),

      "orc" ->
        HiveSerDe(
          inputFormat = Option("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"),
          outputFormat = Option("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"),
          serde = Option("org.apache.hadoop.hive.ql.io.orc.OrcSerde")),

      "parquet" ->
        HiveSerDe(
          inputFormat = Option("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"),
          outputFormat = Option("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
          serde = Option("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")))

    val key = source.toLowerCase match {
      case s if s.startsWith("org.apache.spark.sql.parquet") => "parquet"
      case s if s.startsWith("org.apache.spark.sql.orc") => "orc"
      case s => s
    }

    serdeMap.get(key)
  }
}

//如何读取hive的元数据
private[hive] class HiveMetastoreCatalog(val client: ClientInterface, hive: HiveContext)
  extends Catalog with Logging {

  val conf = hive.conf

  /** Usages should lock on `this`. */
  protected[hive] lazy val hiveWarehouse = new Warehouse(hive.hiveconf)

  // TODO: Use this everywhere instead of tuples or databaseName, tableName,.
  /** A fully qualified identifier for a table (i.e., database.tableName) */
  case class QualifiedTableName(database: String, name: String) {
    def toLowerCase: QualifiedTableName = QualifiedTableName(database.toLowerCase, name.toLowerCase)//全部小写
  }

  /** A cache of Spark SQL data source tables that have been accessed.
    * 是一个缓存,key是表的全名,即数据库,tableName，value是该表的逻辑计划
    */
  protected[hive] val cachedDataSourceTables: LoadingCache[QualifiedTableName, LogicalPlan] = {
    val cacheLoader = new CacheLoader[QualifiedTableName, LogicalPlan]() {
      override def load(in: QualifiedTableName): LogicalPlan = {//如果不存在,则要初始化加载
        logDebug(s"Creating new cached data source for $in")
        val table = client.getTable(in.database, in.name)

        def schemaStringFromParts: Option[String] = {
          table.properties.get("spark.sql.sources.schema.numParts").map { numParts =>
            val parts = (0 until numParts.toInt).map { index =>
              val part = table.properties.get(s"spark.sql.sources.schema.part.$index").orNull //获取每一个位置对应的分区信息
              if (part == null) {
                throw new AnalysisException(
                  "Could not read schema from the metastore because it is corrupted " +
                    s"(missing part $index of the schema, $numParts parts are expected).")
              }

              part
            }
            // Stick all parts back to a single schema string.
            parts.mkString
          }
        }

        // Originally, we used spark.sql.sources.schema to store the schema of a data source table.
        // After SPARK-6024, we removed this flag.
        // Although we are not using spark.sql.sources.schema any more, we need to still support.
        val schemaString =
          table.properties.get("spark.sql.sources.schema").orElse(schemaStringFromParts) //获取table的schema

        val userSpecifiedSchema =
          schemaString.map(s => DataType.fromJson(s).asInstanceOf[StructType])

        // We only need names at here since userSpecifiedSchema we loaded from the metastore
        // contains partition columns. We can always get datatypes of partitioning columns
        // from userSpecifiedSchema.
        val partitionColumns = table.partitionColumns.map(_.name) //分区列集合

        // It does not appear that the ql client for the metastore has a way to enumerate all the
        // SerDe properties directly...
        val options = table.serdeProperties

        val resolvedRelation =
          ResolvedDataSource(
            hive,
            userSpecifiedSchema,
            partitionColumns.toArray,
            table.properties("spark.sql.sources.provider"),
            options)

        LogicalRelation(resolvedRelation.relation)
      }
    }

    CacheBuilder.newBuilder().maximumSize(1000).build(cacheLoader)
  }

  override def refreshTable(tableIdent: TableIdentifier): Unit = {
    // refreshTable does not eagerly reload the cache. It just invalidate the cache.
    // Next time when we use the table, it will be populated in the cache.
    // Since we also cache ParquetRelations converted from Hive Parquet tables and
    // adding converted ParquetRelations into the cache is not defined in the load function
    // of the cache (instead, we add the cache entry in convertToParquetRelation),
    // it is better at here to invalidate the cache to avoid confusing waring logs from the
    // cache loader (e.g. cannot find data source provider, which is only defined for
    // data source table.).
    invalidateTable(tableIdent)
  }

  def invalidateTable(tableIdent: TableIdentifier): Unit = {
    val databaseName = tableIdent.database.getOrElse(client.currentDatabase)
    val tableName = tableIdent.table

    cachedDataSourceTables.invalidate(QualifiedTableName(databaseName, tableName).toLowerCase)
  }

  val caseSensitive: Boolean = false

  /**
   * Creates a data source table (a table created with USING clause) in Hive's metastore.
   * Returns true when the table has been created. Otherwise, false.
   */
  // TODO: Remove this in SPARK-10104.
  def createDataSourceTable(
      tableName: String,
      userSpecifiedSchema: Option[StructType],
      partitionColumns: Array[String],
      provider: String,
      options: Map[String, String],
      isExternal: Boolean): Unit = {
    createDataSourceTable(
      new SqlParser().parseTableIdentifier(tableName),
      userSpecifiedSchema,
      partitionColumns,
      provider,
      options,
      isExternal)
  }

  def createDataSourceTable(
      tableIdent: TableIdentifier,
      userSpecifiedSchema: Option[StructType],
      partitionColumns: Array[String],
      provider: String,
      options: Map[String, String],
      isExternal: Boolean): Unit = {
    val (dbName, tblName) = {
      val database = tableIdent.database.getOrElse(client.currentDatabase)
      processDatabaseAndTableName(database, tableIdent.table)
    }

    val tableProperties = new mutable.HashMap[String, String]
    tableProperties.put("spark.sql.sources.provider", provider)

    // Saves optional user specified schema.  Serialized JSON schema string may be too long to be
    // stored into a single metastore SerDe property.  In this case, we split the JSON string and
    // store each part as a separate SerDe property.
    userSpecifiedSchema.foreach { schema =>
      val threshold = conf.schemaStringLengthThreshold
      val schemaJsonString = schema.json
      // Split the JSON string.
      val parts = schemaJsonString.grouped(threshold).toSeq
      tableProperties.put("spark.sql.sources.schema.numParts", parts.size.toString)
      parts.zipWithIndex.foreach { case (part, index) =>
        tableProperties.put(s"spark.sql.sources.schema.part.$index", part)
      }
    }

    val metastorePartitionColumns = userSpecifiedSchema.map { schema =>
      val fields = partitionColumns.map(col => schema(col))
      fields.map { field =>
        HiveColumn(
          name = field.name,
          hiveType = HiveMetastoreTypes.toMetastoreType(field.dataType),
          comment = "")
      }.toSeq
    }.getOrElse {
      if (partitionColumns.length > 0) {
        // The table does not have a specified schema, which means that the schema will be inferred
        // when we load the table. So, we are not expecting partition columns and we will discover
        // partitions when we load the table. However, if there are specified partition columns,
        // we simply ignore them and provide a warning message.
        logWarning(
          s"The schema and partitions of table $tableIdent will be inferred when it is loaded. " +
            s"Specified partition columns (${partitionColumns.mkString(",")}) will be ignored.")
      }
      Seq.empty[HiveColumn]
    }

    val tableType = if (isExternal) {
      tableProperties.put("EXTERNAL", "TRUE")
      ExternalTable
    } else {
      tableProperties.put("EXTERNAL", "FALSE")
      ManagedTable
    }

    val maybeSerDe = HiveSerDe.sourceToSerDe(provider, hive.hiveconf)
    val dataSource = ResolvedDataSource(
      hive, userSpecifiedSchema, partitionColumns, provider, options)

    def newSparkSQLSpecificMetastoreTable(): HiveTable = {
      HiveTable(
        specifiedDatabase = Option(dbName),
        name = tblName,
        schema = Seq.empty,
        partitionColumns = metastorePartitionColumns,
        tableType = tableType,
        properties = tableProperties.toMap,
        serdeProperties = options)
    }

    def newHiveCompatibleMetastoreTable(relation: HadoopFsRelation, serde: HiveSerDe): HiveTable = {
      def schemaToHiveColumn(schema: StructType): Seq[HiveColumn] = {
        schema.map { field =>
          HiveColumn(
            name = field.name,
            hiveType = HiveMetastoreTypes.toMetastoreType(field.dataType),
            comment = "")
        }
      }

      val partitionColumns = schemaToHiveColumn(relation.partitionColumns)
      val dataColumns = schemaToHiveColumn(relation.schema).filterNot(partitionColumns.contains)

      HiveTable(
        specifiedDatabase = Option(dbName),
        name = tblName,
        schema = dataColumns,
        partitionColumns = partitionColumns,
        tableType = tableType,
        properties = tableProperties.toMap,
        serdeProperties = options,
        location = Some(relation.paths.head),
        viewText = None, // TODO We need to place the SQL string here.
        inputFormat = serde.inputFormat,
        outputFormat = serde.outputFormat,
        serde = serde.serde)
    }

    // TODO: Support persisting partitioned data source relations in Hive compatible format
    val hiveTable = (maybeSerDe, dataSource.relation) match {
      case (Some(serde), relation: HadoopFsRelation)
          if relation.paths.length == 1 && relation.partitionColumns.isEmpty =>
        // Hive ParquetSerDe doesn't support decimal type until 1.2.0.
        val isParquetSerDe = serde.inputFormat.exists(_.toLowerCase.contains("parquet"))
        val hasDecimalFields = relation.schema.existsRecursively(_.isInstanceOf[DecimalType])

        val hiveParquetSupportsDecimal = client.version match {
          case org.apache.spark.sql.hive.client.hive.v1_2 => true
          case _ => false
        }

        if (isParquetSerDe && !hiveParquetSupportsDecimal && hasDecimalFields) {
          // If Hive version is below 1.2.0, we cannot save Hive compatible schema to
          // metastore when the file format is Parquet and the schema has DecimalType.
          logWarning {
            "Persisting Parquet relation with decimal field(s) into Hive metastore in Spark SQL " +
              "specific format, which is NOT compatible with Hive. Because ParquetHiveSerDe in " +
              s"Hive ${client.version.fullVersion} doesn't support decimal type. See HIVE-6384."
          }
          newSparkSQLSpecificMetastoreTable()
        } else {
          logInfo {
            "Persisting data source relation with a single input path into Hive metastore in " +
              s"Hive compatible format. Input path: ${relation.paths.head}"
          }
          newHiveCompatibleMetastoreTable(relation, serde)
        }

      case (Some(serde), relation: HadoopFsRelation) if relation.partitionColumns.nonEmpty =>
        logWarning {
          "Persisting partitioned data source relation into Hive metastore in " +
            s"Spark SQL specific format, which is NOT compatible with Hive.  Input path(s): " +
            relation.paths.mkString("\n", "\n", "")
        }
        newSparkSQLSpecificMetastoreTable()

      case (Some(serde), relation: HadoopFsRelation) =>
        logWarning {
          "Persisting data source relation with multiple input paths into Hive metastore in " +
            s"Spark SQL specific format, which is NOT compatible with Hive.  Input paths: " +
            relation.paths.mkString("\n", "\n", "")
        }
        newSparkSQLSpecificMetastoreTable()

      case (Some(serde), _) =>
        logWarning {
          s"Data source relation is not a ${classOf[HadoopFsRelation].getSimpleName}. " +
            "Persisting it into Hive metastore in Spark SQL specific format, " +
            "which is NOT compatible with Hive."
        }
        newSparkSQLSpecificMetastoreTable()

      case _ =>
        logWarning {
          s"Couldn't find corresponding Hive SerDe for data source provider $provider. " +
            "Persisting data source relation into Hive metastore in Spark SQL specific format, " +
            "which is NOT compatible with Hive."
        }
        newSparkSQLSpecificMetastoreTable()
    }

    client.createTable(hiveTable)
  }

  //参数是数据库.tableName形式
  //返回该数据库对应的表的根目录path
  def hiveDefaultTableFilePath(tableName: String): String = {
    hiveDefaultTableFilePath(new SqlParser().parseTableIdentifier(tableName))
  }

  //返回该数据库对应的表的根目录path
  def hiveDefaultTableFilePath(tableIdent: TableIdentifier): String = {
    // Code based on: hiveWarehouse.getTablePath(currentDatabase, tableName)
    val database = tableIdent.database.getOrElse(client.currentDatabase) //获取数据库name

    new Path(
      new Path(client.getDatabase(database).location),
      tableIdent.table.toLowerCase).toString //获取数据库path以及table的path
  }

  //获取该数据库是否存在
  def tableExists(tableIdentifier: Seq[String]): Boolean = {
    val tableIdent = processTableIdentifier(tableIdentifier)
    val databaseName =
      tableIdent
        .lift(tableIdent.size - 2)
        .getOrElse(client.currentDatabase)
    val tblName = tableIdent.last
    client.getTableOption(databaseName, tblName).isDefined
  }

  //查询一个数据库表
  def lookupRelation(
      tableIdentifier: Seq[String],
      alias: Option[String]): LogicalPlan = {
    val tableIdent = processTableIdentifier(tableIdentifier)
    val databaseName = tableIdent.lift(tableIdent.size - 2).getOrElse(
      client.currentDatabase)
    val tblName = tableIdent.last
    val table = client.getTable(databaseName, tblName) //获取对应的hive表

    if (table.properties.get("spark.sql.sources.provider").isDefined) {//说明该表是spark的表,不是hive的
      val dataSourceTable =
        cachedDataSourceTables(QualifiedTableName(databaseName, tblName).toLowerCase)
      // Then, if alias is specified, wrap the table with a Subquery using the alias.
      // Otherwise, wrap the table with a Subquery using the table name.
      val withAlias =
        alias.map(a => Subquery(a, dataSourceTable)).getOrElse(
          Subquery(tableIdent.last, dataSourceTable))//形成一个子查询

      withAlias
    } else if (table.tableType == VirtualView) {//说明是视图的虚拟表
      val viewText = table.viewText.getOrElse(sys.error("Invalid view without text.")) //获取视图的内容
      alias match {
        // because hive use things like `_c0` to build the expanded text
        // currently we cannot support view from "create view v1(c1) as ..."
        case None => Subquery(table.name, HiveQl.createPlan(viewText)) //为视图创建一个子查询,别名默认是视图表的name
        case Some(aliasText) => Subquery(aliasText, HiveQl.createPlan(viewText))
      }
    } else {
      MetastoreRelation(databaseName, tblName, alias)(table)(hive)//说明是具体的表
    }
  }

  private def convertToParquetRelation(metastoreRelation: MetastoreRelation): LogicalRelation = {
    val metastoreSchema = StructType.fromAttributes(metastoreRelation.output)
    val mergeSchema = hive.convertMetastoreParquetWithSchemaMerging

    // NOTE: Instead of passing Metastore schema directly to `ParquetRelation`, we have to
    // serialize the Metastore schema to JSON and pass it as a data source option because of the
    // evil case insensitivity issue, which is reconciled within `ParquetRelation`.
    val parquetOptions = Map(
      ParquetRelation.METASTORE_SCHEMA -> metastoreSchema.json,
      ParquetRelation.MERGE_SCHEMA -> mergeSchema.toString)
    val tableIdentifier =
      QualifiedTableName(metastoreRelation.databaseName, metastoreRelation.tableName)

    def getCached(
        tableIdentifier: QualifiedTableName,
        pathsInMetastore: Seq[String],
        schemaInMetastore: StructType,
        partitionSpecInMetastore: Option[PartitionSpec]): Option[LogicalRelation] = {
      cachedDataSourceTables.getIfPresent(tableIdentifier) match {
        case null => None // Cache miss
        case logical @ LogicalRelation(parquetRelation: ParquetRelation) =>
          // If we have the same paths, same schema, and same partition spec,
          // we will use the cached Parquet Relation.
          val useCached =
            parquetRelation.paths.toSet == pathsInMetastore.toSet &&
            logical.schema.sameType(metastoreSchema) &&
            parquetRelation.partitionSpec == partitionSpecInMetastore.getOrElse {
              PartitionSpec(StructType(Nil), Array.empty[datasources.Partition])
            }

          if (useCached) {
            Some(logical)
          } else {
            // If the cached relation is not updated, we invalidate it right away.
            cachedDataSourceTables.invalidate(tableIdentifier)
            None
          }
        case other =>
          logWarning(
            s"${metastoreRelation.databaseName}.${metastoreRelation.tableName} should be stored " +
              s"as Parquet. However, we are getting a $other from the metastore cache. " +
              s"This cached entry will be invalidated.")
          cachedDataSourceTables.invalidate(tableIdentifier)
          None
      }
    }

    val result = if (metastoreRelation.hiveQlTable.isPartitioned) {
      val partitionSchema = StructType.fromAttributes(metastoreRelation.partitionKeys)
      val partitionColumnDataTypes = partitionSchema.map(_.dataType)
      // We're converting the entire table into ParquetRelation, so predicates to Hive metastore
      // are empty.
      val partitions = metastoreRelation.getHiveQlPartitions().map { p =>
        val location = p.getLocation
        val values = InternalRow.fromSeq(p.getValues.zip(partitionColumnDataTypes).map {
          case (rawValue, dataType) => Cast(Literal(rawValue), dataType).eval(null)
        })
        ParquetPartition(values, location)
      }
      val partitionSpec = PartitionSpec(partitionSchema, partitions)
      val paths = partitions.map(_.path)

      val cached = getCached(tableIdentifier, paths, metastoreSchema, Some(partitionSpec))
      val parquetRelation = cached.getOrElse {
        val created = LogicalRelation(
          new ParquetRelation(
            paths.toArray, None, Some(partitionSpec), parquetOptions)(hive))
        cachedDataSourceTables.put(tableIdentifier, created)
        created
      }

      parquetRelation
    } else {
      val paths = Seq(metastoreRelation.hiveQlTable.getDataLocation.toString)

      val cached = getCached(tableIdentifier, paths, metastoreSchema, None)
      val parquetRelation = cached.getOrElse {
        val created = LogicalRelation(
          new ParquetRelation(paths.toArray, None, None, parquetOptions)(hive))
        cachedDataSourceTables.put(tableIdentifier, created)
        created
      }

      parquetRelation
    }

    result.newInstance()
  }

  //返回该数据库下所有的表集合,第二个参数暂时都是false
  override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    val db = databaseName.getOrElse(client.currentDatabase)

    client.listTables(db).map(tableName => (tableName, false))
  }

  //对数据库name和表的name都格式化成小写
  protected def processDatabaseAndTableName(
      databaseName: Option[String],
      tableName: String): (Option[String], String) = {
    if (!caseSensitive) {
      (databaseName.map(_.toLowerCase), tableName.toLowerCase)
    } else {
      (databaseName, tableName)
    }
  }

  //对数据库name和表的name都格式化成小写
  protected def processDatabaseAndTableName(
      databaseName: String,
      tableName: String): (String, String) = {
    if (!caseSensitive) {
      (databaseName.toLowerCase, tableName.toLowerCase)
    } else {
      (databaseName, tableName)
    }
  }

  /**
   * When scanning or writing to non-partitioned Metastore Parquet tables, convert them to Parquet
   * data source relations for better performance.
   */
  object ParquetConversions extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      if (!plan.resolved || plan.analyzed) {
        return plan
      }

      // Collects all `MetastoreRelation`s which should be replaced
      val toBeReplaced = plan.collect {
        // Write path
        case InsertIntoTable(relation: MetastoreRelation, _, _, _, _)
            // Inserting into partitioned table is not supported in Parquet data source (yet).
            if !relation.hiveQlTable.isPartitioned &&
              hive.convertMetastoreParquet &&
              relation.tableDesc.getSerdeClassName.toLowerCase.contains("parquet") =>
          val parquetRelation = convertToParquetRelation(relation)
          val attributedRewrites = relation.output.zip(parquetRelation.output)
          (relation, parquetRelation, attributedRewrites)

        // Write path
        case InsertIntoHiveTable(relation: MetastoreRelation, _, _, _, _)
          // Inserting into partitioned table is not supported in Parquet data source (yet).
          if !relation.hiveQlTable.isPartitioned &&
            hive.convertMetastoreParquet &&
            relation.tableDesc.getSerdeClassName.toLowerCase.contains("parquet") =>
          val parquetRelation = convertToParquetRelation(relation)
          val attributedRewrites = relation.output.zip(parquetRelation.output)
          (relation, parquetRelation, attributedRewrites)

        // Read path
        case relation: MetastoreRelation if hive.convertMetastoreParquet &&
              relation.tableDesc.getSerdeClassName.toLowerCase.contains("parquet") =>
          val parquetRelation = convertToParquetRelation(relation)
          val attributedRewrites = relation.output.zip(parquetRelation.output)
          (relation, parquetRelation, attributedRewrites)
      }

      val relationMap = toBeReplaced.map(r => (r._1, r._2)).toMap
      val attributedRewrites = AttributeMap(toBeReplaced.map(_._3).fold(Nil)(_ ++: _))

      // Replaces all `MetastoreRelation`s with corresponding `ParquetRelation2`s, and fixes
      // attribute IDs referenced in other nodes.
      plan.transformUp {
        case r: MetastoreRelation if relationMap.contains(r) =>
          val parquetRelation = relationMap(r)
          val alias = r.alias.getOrElse(r.tableName)
          Subquery(alias, parquetRelation)

        case InsertIntoTable(r: MetastoreRelation, partition, child, overwrite, ifNotExists)
          if relationMap.contains(r) =>
          val parquetRelation = relationMap(r)
          InsertIntoTable(parquetRelation, partition, child, overwrite, ifNotExists)

        case InsertIntoHiveTable(r: MetastoreRelation, partition, child, overwrite, ifNotExists)
          if relationMap.contains(r) =>
          val parquetRelation = relationMap(r)
          InsertIntoTable(parquetRelation, partition, child, overwrite, ifNotExists)

        case other => other.transformExpressions {
          case a: Attribute if a.resolved => attributedRewrites.getOrElse(a, a)
        }
      }
    }
  }

  /**
   * Creates any tables required for query execution.
   * For example, because of a CREATE TABLE X AS statement.
   * 为查询的query创建一个table
   */
  object CreateTables extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      // Wait until children are resolved.
      case p: LogicalPlan if !p.childrenResolved => p
      case p: LogicalPlan if p.resolved => p
      case p @ CreateTableAsSelect(table, child, allowExisting) =>
        val schema = if (table.schema.nonEmpty) {
          table.schema
        } else {
          child.output.map {//select的输出列
            attr => new HiveColumn(
              attr.name,
              HiveMetastoreTypes.toMetastoreType(attr.dataType), null)
          }
        }

        val desc = table.copy(schema = schema)

        if (hive.convertCTAS && table.serde.isEmpty) {
          // Do the conversion when spark.sql.hive.convertCTAS is true and the query
          // does not specify any storage format (file format and storage handler).
          if (table.specifiedDatabase.isDefined) {
            throw new AnalysisException(
              "Cannot specify database name in a CTAS statement " +
                "when spark.sql.hive.convertCTAS is set to true.")
          }

          val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists
          CreateTableUsingAsSelect(
            TableIdentifier(desc.name),
            hive.conf.defaultDataSourceName,
            temporary = false,
            Array.empty[String],
            mode,
            options = Map.empty[String, String],
            child
          )
        } else {
          val desc = if (table.serde.isEmpty) {
            // add default serde
            table.copy(
              serde = Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
          } else {
            table
          }

          val (dbName, tblName) =
            processDatabaseAndTableName(
              desc.specifiedDatabase.getOrElse(client.currentDatabase), desc.name)

          execution.CreateTableAsSelect(
            desc.copy(
              specifiedDatabase = Some(dbName),
              name = tblName),
            child,
            allowExisting)
        }
    }
  }

  /**
   * Casts input data to correct data types according to table definition before inserting into
   * that table.
   */
  object PreInsertionCasts extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
      // Wait until children are resolved.
      case p: LogicalPlan if !p.childrenResolved => p

      case p @ InsertIntoTable(table: MetastoreRelation, _, child, _, _) =>
        castChildOutput(p, table, child)
    }

    def castChildOutput(p: InsertIntoTable, table: MetastoreRelation, child: LogicalPlan)
      : LogicalPlan = {
      val childOutputDataTypes = child.output.map(_.dataType)
      val numDynamicPartitions = p.partition.values.count(_.isEmpty)
      val tableOutputDataTypes =
        (table.attributes ++ table.partitionKeys.takeRight(numDynamicPartitions))
          .take(child.output.length).map(_.dataType)

      if (childOutputDataTypes == tableOutputDataTypes) {
        InsertIntoHiveTable(table, p.partition, p.child, p.overwrite, p.ifNotExists)
      } else if (childOutputDataTypes.size == tableOutputDataTypes.size &&
        childOutputDataTypes.zip(tableOutputDataTypes)
          .forall { case (left, right) => left.sameType(right) }) {
        // If both types ignoring nullability of ArrayType, MapType, StructType are the same,
        // use InsertIntoHiveTable instead of InsertIntoTable.
        InsertIntoHiveTable(table, p.partition, p.child, p.overwrite, p.ifNotExists)
      } else {
        // Only do the casting when child output data types differ from table output data types.
        val castedChildOutput = child.output.zip(table.output).map {
          case (input, output) if input.dataType != output.dataType =>
            Alias(Cast(input, output.dataType), input.name)()
          case (input, _) => input
        }

        p.copy(child = logical.Project(castedChildOutput, child))
      }
    }
  }

  /**
   * UNIMPLEMENTED: It needs to be decided how we will persist in-memory tables to the metastore.
   * For now, if this functionality is desired mix in the in-memory [[OverrideCatalog]].
   */
  override def registerTable(tableIdentifier: Seq[String], plan: LogicalPlan): Unit = {
    throw new UnsupportedOperationException
  }

  /**
   * UNIMPLEMENTED: It needs to be decided how we will persist in-memory tables to the metastore.
   * For now, if this functionality is desired mix in the in-memory [[OverrideCatalog]].
   */
  override def unregisterTable(tableIdentifier: Seq[String]): Unit = {
    throw new UnsupportedOperationException
  }

  override def unregisterAllTables(): Unit = {}
}

/**
 * A logical plan representing insertion into Hive table.
 * This plan ignores nullability of ArrayType, MapType, StructType unlike InsertIntoTable
 * because Hive table doesn't have nullability for ARRAY, MAP, STRUCT types.
 */
private[hive] case class InsertIntoHiveTable(
    table: MetastoreRelation,//最终select的结果要插入到哪里去
    partition: Map[String, Option[String]],//插入到哪个分区小
    child: LogicalPlan,//select的逻辑计划
    overwrite: Boolean,
    ifNotExists: Boolean)
  extends LogicalPlan {

  override def children: Seq[LogicalPlan] = child :: Nil
  override def output: Seq[Attribute] = Seq.empty //insert是不需要有输出的,直接结果存储到文件中

  //动态分区的字段个数
  val numDynamicPartitions = partition.values.count(_.isEmpty) //分区的value是空的个数,即如果分区字段只有key,没有value,则说明该字段是动态分区

  // This is the expected schema of the table prepared to be inserted into,
  // including dynamic partition columns.
  val tableOutput = table.attributes ++ table.partitionKeys.takeRight(numDynamicPartitions) //输出的是table的需要的输出 以及 对应的动态分区的输出列数量之和

  //校验数据是否合法
  override lazy val resolved: Boolean = childrenResolved && child.output.zip(tableOutput).forall {
    case (childAttr, tableAttr) => childAttr.dataType.sameType(tableAttr.dataType) //确保数据类型是一致的,即select的每一个字段 和 insert的每一个字段
  }
}

//参数表示数据库名字、表名字 以及表的别名
private[hive] case class MetastoreRelation
    (databaseName: String, tableName: String, alias: Option[String])
    (val table: HiveTable) //传入spark封装的hive元数据对象
    (@transient sqlContext: SQLContext)
  extends LeafNode with MultiInstanceRelation with FileRelation {

  override def equals(other: Any): Boolean = other match {
    case relation: MetastoreRelation =>
      databaseName == relation.databaseName &&
        tableName == relation.tableName &&
        alias == relation.alias &&
        output == relation.output //数据的输出列也相同,说明两个表是相同的
    case _ => false
  }

  override def hashCode(): Int = {
    Objects.hashCode(databaseName, tableName, alias, output)
  }

  //转换成hive的table对象
  @transient val hiveQlTable: Table = {
    // We start by constructing an API table as Hive performs several important transformations
    // internally when converting an API table to a QL table.
    val tTable = new org.apache.hadoop.hive.metastore.api.Table()
    tTable.setTableName(table.name)
    tTable.setDbName(table.database)

    val tableParameters = new java.util.HashMap[String, String]()
    tTable.setParameters(tableParameters)
    table.properties.foreach { case (k, v) => tableParameters.put(k, v) }//设置table的参数

    tTable.setTableType(table.tableType.name)//设置table的数据类型

    val sd = new org.apache.hadoop.hive.metastore.api.StorageDescriptor()
    tTable.setSd(sd)
    sd.setCols(table.schema.map(c => new FieldSchema(c.name, c.hiveType, c.comment)))//设置列
    tTable.setPartitionKeys(
      table.partitionColumns.map(c => new FieldSchema(c.name, c.hiveType, c.comment)))//设置分区

    table.location.foreach(sd.setLocation) //设置setLocation方法
    table.inputFormat.foreach(sd.setInputFormat)
    table.outputFormat.foreach(sd.setOutputFormat)

    val serdeInfo = new org.apache.hadoop.hive.metastore.api.SerDeInfo
    table.serde.foreach(serdeInfo.setSerializationLib)
    sd.setSerdeInfo(serdeInfo)

    val serdeParameters = new java.util.HashMap[String, String]()
    table.serdeProperties.foreach { case (k, v) => serdeParameters.put(k, v) }
    serdeInfo.setParameters(serdeParameters)

    new Table(tTable)
  }

  @transient override lazy val statistics: Statistics = Statistics(
    sizeInBytes = {
      val totalSize = hiveQlTable.getParameters.get(StatsSetupConst.TOTAL_SIZE)
      val rawDataSize = hiveQlTable.getParameters.get(StatsSetupConst.RAW_DATA_SIZE)
      // TODO: check if this estimate is valid for tables after partition pruning.
      // NOTE: getting `totalSize` directly from params is kind of hacky, but this should be
      // relatively cheap if parameters for the table are populated into the metastore.  An
      // alternative would be going through Hadoop's FileSystem API, which can be expensive if a lot
      // of RPCs are involved.  Besides `totalSize`, there are also `numFiles`, `numRows`,
      // `rawDataSize` keys (see StatsSetupConst in Hive) that we can look at in the future.
      BigInt(
        // When table is external,`totalSize` is always zero, which will influence join strategy
        // so when `totalSize` is zero, use `rawDataSize` instead
        // if the size is still less than zero, we use default size
        Option(totalSize).map(_.toLong).filter(_ > 0)
          .getOrElse(Option(rawDataSize).map(_.toLong).filter(_ > 0)
          .getOrElse(sqlContext.conf.defaultSizeInBytes)))
    }
  )

  // When metastore partition pruning is turned off, we cache the list of all partitions to
  // mimic the behavior of Spark < 1.5
  lazy val allPartitions = table.getAllPartitions //请求hive元数据,获取表的所有分区

  def getHiveQlPartitions(predicates: Seq[Expression] = Nil): Seq[Partition] = {

    //获取hive的分区集合
    val rawPartitions = if (sqlContext.conf.metastorePartitionPruning) {//true表示要对hive的分区进行过滤
      table.getPartitions(predicates) //进行过滤
    } else {
      allPartitions
    }

    //转换成hive的partition对象
    rawPartitions.map { p =>
      val tPartition = new org.apache.hadoop.hive.metastore.api.Partition //根据分区名字 构建hive的分区对象
      tPartition.setDbName(databaseName)
      tPartition.setTableName(tableName)
      tPartition.setValues(p.values)

      val sd = new org.apache.hadoop.hive.metastore.api.StorageDescriptor()
      tPartition.setSd(sd)
      sd.setCols(table.schema.map(c => new FieldSchema(c.name, c.hiveType, c.comment)))

      sd.setLocation(p.storage.location)
      sd.setInputFormat(p.storage.inputFormat)
      sd.setOutputFormat(p.storage.outputFormat)

      val serdeInfo = new org.apache.hadoop.hive.metastore.api.SerDeInfo
      sd.setSerdeInfo(serdeInfo)
      serdeInfo.setSerializationLib(p.storage.serde)

      val serdeParameters = new java.util.HashMap[String, String]()
      serdeInfo.setParameters(serdeParameters)
      table.serdeProperties.foreach { case (k, v) => serdeParameters.put(k, v) } //设置序列化的属性信息
      p.storage.serdeProperties.foreach { case (k, v) => serdeParameters.put(k, v) }

      new Partition(hiveQlTable, tPartition)
    }
  }

  /** Only compare database and tablename, not alias. */
  override def sameResult(plan: LogicalPlan): Boolean = {
    plan match {
      case mr: MetastoreRelation =>
        mr.databaseName == databaseName && mr.tableName == tableName
      case _ => false
    }
  }

  val tableDesc = new TableDesc(
    hiveQlTable.getInputFormatClass,
    // The class of table should be org.apache.hadoop.hive.ql.metadata.Table because
    // getOutputFormatClass will use HiveFileFormatUtils.getOutputFormatSubstitute to
    // substitute some output formats, e.g. substituting SequenceFileOutputFormat to
    // HiveSequenceFileOutputFormat.
    hiveQlTable.getOutputFormatClass,
    hiveQlTable.getMetadata
  )

  //获取一个属性对应的name、数据类型、是否允许为null
  implicit class SchemaAttribute(f: HiveColumn) {
    def toAttribute: AttributeReference = AttributeReference(
      f.name,
      HiveMetastoreTypes.toDataType(f.hiveType),
      // Since data can be dumped in randomly with no validation, everything is nullable.
      nullable = true
    )(qualifiers = Seq(alias.getOrElse(tableName)))
  }

  /** PartitionKey attributes 表的分区属性集合*/
  val partitionKeys = table.partitionColumns.map(_.toAttribute)

  /** Non-partitionKey attributes */
  val attributes = table.schema.map(_.toAttribute) //表的属性集合

  val output = attributes ++ partitionKeys //所有的属性

  /** An attribute map that can be used to lookup original attributes based on expression id. */
  val attributeMap = AttributeMap(output.map(o => (o, o)))

  /** An attribute map for determining the ordinal for non-partition columns. */
  val columnOrdinals = AttributeMap(attributes.zipWithIndex) //表的属性 追加索引序号

  //返回hdfs上的路径集合
  override def inputFiles: Array[String] = {
    val partLocations = table.getPartitions(Nil).map(_.storage.location).toArray //获取所有分区对应的hdfs路径集合
    if (partLocations.nonEmpty) {
      partLocations
    } else {
      Array(
        table.location.getOrElse(
          sys.error(s"Could not get the location of ${table.qualifiedName}."))) //获取表的路径
    }
  }


  override def newInstance(): MetastoreRelation = {
    MetastoreRelation(databaseName, tableName, alias)(table)(sqlContext)
  }
}


private[hive] object HiveMetastoreTypes {
  //将字符串形式的数据类型转换成数据类型对象
  def toDataType(metastoreType: String): DataType = DataTypeParser.parse(metastoreType)

  //将sparksql支持的数据类型,转换成hive支持的字符串形式的属性类型
  def decimalMetastoreString(decimalType: DecimalType): String = decimalType match {
    case DecimalType.Fixed(precision, scale) => s"decimal($precision,$scale)"
    case _ => s"decimal($HiveShim.UNLIMITED_DECIMAL_PRECISION,$HiveShim.UNLIMITED_DECIMAL_SCALE)"
  }

  //将sparksql支持的数据类型,转换成hive支持的字符串形式的属性类型
  def toMetastoreType(dt: DataType): String = dt match {
    case ArrayType(elementType, _) => s"array<${toMetastoreType(elementType)}>"
    case StructType(fields) =>
      s"struct<${fields.map(f => s"${f.name}:${toMetastoreType(f.dataType)}").mkString(",")}>"
    case MapType(keyType, valueType, _) =>
      s"map<${toMetastoreType(keyType)},${toMetastoreType(valueType)}>"
    case StringType => "string"
    case FloatType => "float"
    case IntegerType => "int"
    case ByteType => "tinyint"
    case ShortType => "smallint"
    case DoubleType => "double"
    case LongType => "bigint"
    case BinaryType => "binary"
    case BooleanType => "boolean"
    case DateType => "date"
    case d: DecimalType => decimalMetastoreString(d)
    case TimestampType => "timestamp"
    case NullType => "void"
    case udt: UserDefinedType[_] => toMetastoreType(udt.sqlType)
  }
}
