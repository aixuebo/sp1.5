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

package org.apache.spark.sql.execution.datasources.json

import java.io.CharArrayWriter

import com.fasterxml.jackson.core.JsonFactory
import com.google.common.base.Objects
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapred.{JobConf, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.mapreduce.{Job, RecordWriter, TaskAttemptContext}

import org.apache.spark.Logging
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.mapred.SparkHadoopMapRedUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionSpec
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, Row, SQLContext}
import org.apache.spark.util.SerializableConfiguration


class DefaultSource extends HadoopFsRelationProvider with DataSourceRegister {

  override def shortName(): String = "json"

  override def createRelation(
      sqlContext: SQLContext,
      paths: Array[String],
      dataSchema: Option[StructType],
      partitionColumns: Option[StructType],
      parameters: Map[String, String]): HadoopFsRelation = {
    val samplingRatio = parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0) //默认是1.0,抽样比例

    new JSONRelation(None, samplingRatio, dataSchema, None, partitionColumns, paths)(sqlContext)
  }
}

private[sql] class JSONRelation(
    val inputRDD: Option[RDD[String]],//输入内容RDD
    val samplingRatio: Double,//抽样比例
    val maybeDataSchema: Option[StructType],//可能的数据结构
    val maybePartitionSpec: Option[PartitionSpec],
    override val userDefinedPartitionColumns: Option[StructType],
    override val paths: Array[String] = Array.empty[String])//加载的数据路径
  (@transient val sqlContext: SQLContext)
  extends HadoopFsRelation(maybePartitionSpec) {

  /** Constraints to be imposed on schema to be stored.
    * 校验约束信息
    **/
  private def checkConstraints(schema: StructType): Unit = {
    if (schema.fieldNames.length != schema.fieldNames.distinct.length) {//fieldNames名字不能重复
      val duplicateColumns = schema.fieldNames.groupBy(identity).collect {//找到重复的fieldName打印出来
        case (x, ys) if ys.length > 1 => "\"" + x + "\""
      }.mkString(", ")
      throw new AnalysisException(s"Duplicate column(s) : $duplicateColumns found, " +
        s"cannot save to JSON format")
    }
  }

  override val needConversion: Boolean = false

  //参数是hadoop上要读取的文件集合,返回值是读取hadoop上的内容集合RDD
  private def createBaseRdd(inputPaths: Array[FileStatus]): RDD[String] = {//返回的RDD[String]就是hadoop上存储的每一行内容
    val job = new Job(sqlContext.sparkContext.hadoopConfiguration)
    val conf = job.getConfiguration

    val paths = inputPaths.map(_.getPath)

    if (paths.nonEmpty) {
      FileInputFormat.setInputPaths(job, paths: _*)
    }

    sqlContext.sparkContext.hadoopRDD(
      conf.asInstanceOf[JobConf],
      classOf[TextInputFormat],
      classOf[LongWritable],
      classOf[Text]).map(_._2.toString) // get the text line
  }

  //获取数据的Schema,即StructType类型
  override lazy val dataSchema = {
    val jsonSchema = maybeDataSchema.getOrElse {//如果没有设置可能的数据结构
      val files = cachedLeafStatuses().filterNot { status => //过滤需要的文件内容
        val name = status.getPath.getName
        name.startsWith("_") || name.startsWith(".")
      }.toArray

      //推算数据结构
      InferSchema(
        inputRDD.getOrElse(createBaseRdd(files)),
        samplingRatio,//抽样数据比例
        sqlContext.conf.columnNameOfCorruptRecord)//出错时候的字段填写内容
    }
    checkConstraints(jsonSchema) //校验约束信息

    jsonSchema
  }

  //扫描文件,根据requiredColumns返回最终的RDD[Row]
  override def buildScan(
      requiredColumns: Array[String],//要求扫描哪些列
      filters: Array[Filter],
      inputPaths: Array[FileStatus]) //存储在hadoop上的存储路径集合
    : RDD[Row] = {
    JacksonParser(
      inputRDD.getOrElse(createBaseRdd(inputPaths)),//解析hadoop上的存储路径,返回RDD[String],每一个String表示hadoop上文件的一行内容
      StructType(requiredColumns.map(dataSchema(_))),//调用StructType的apply方法,传入StructField的name,返回StructField对象,即返回新的数据结构
      sqlContext.conf.columnNameOfCorruptRecord).asInstanceOf[RDD[Row]]
  }

  override def equals(other: Any): Boolean = other match {
    case that: JSONRelation =>
      ((inputRDD, that.inputRDD) match {
        case (Some(thizRdd), Some(thatRdd)) => thizRdd eq thatRdd
        case (None, None) => true
        case _ => false
      }) && paths.toSet == that.paths.toSet &&
        dataSchema == that.dataSchema &&
        schema == that.schema
    case _ => false
  }

  override def hashCode(): Int = {
    Objects.hashCode(
      inputRDD,
      paths.toSet,
      dataSchema,
      schema,
      partitionColumns)
  }

  override def prepareJobForWrite(job: Job): OutputWriterFactory = {
    new OutputWriterFactory {
      override def newInstance(
          path: String,//文件输出的目录
          dataSchema: StructType,//文件输出的格式
          context: TaskAttemptContext): OutputWriter = {
        new JsonOutputWriter(path, dataSchema, context)
      }
    }
  }
}

private[json] class JsonOutputWriter(
    path: String,//文件输出的目录
    dataSchema: StructType,//文件输出的格式
    context: TaskAttemptContext)
  extends OutputWriter with SparkHadoopMapRedUtil with Logging {

  val writer = new CharArrayWriter()
  // create the Generator without separator inserted between 2 records
  val gen = new JsonFactory().createGenerator(writer).setRootValueSeparator(null) //json解析对象

  val result = new Text()

  private val recordWriter: RecordWriter[NullWritable, Text] = {
    new TextOutputFormat[NullWritable, Text]() {
      override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
        val configuration = SparkHadoopUtil.get.getConfigurationFromJobContext(context)
        val uniqueWriteJobId = configuration.get("spark.sql.sources.writeJobUUID")
        val taskAttemptId = SparkHadoopUtil.get.getTaskAttemptIDFromTaskAttemptContext(context)
        val split = taskAttemptId.getTaskID.getId
        new Path(path, f"part-r-$split%05d-$uniqueWriteJobId$extension")
      }
    }.getRecordWriter(context)
  }

  override def write(row: Row): Unit = throw new UnsupportedOperationException("call writeInternal") //请调用writeInternal方法,因为传入的参数不是Row类型的,而是InternalRow类型的

  override protected[sql] def writeInternal(row: InternalRow): Unit = {
    JacksonGenerator(dataSchema, gen, row) //将row转换成json对象
    gen.flush()

    result.set(writer.toString)
    writer.reset() //每次写完一个row后重置内存

    recordWriter.write(NullWritable.get(), result) //将json写入到hdfs的文件上
  }

  override def close(): Unit = {
    gen.close()
    recordWriter.close(context)
  }
}
