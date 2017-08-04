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

package org.apache.spark.sql.execution

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.{RDD, RDDOperationScope}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.metric.{LongSQLMetric, SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.DataType

object SparkPlan {
  protected[sql] val currentContext = new ThreadLocal[SQLContext]()
}

/**
 * :: DeveloperApi ::
 * spark真正的执行计划,子类需要覆盖execute方法,表示该如何执行计划逻辑
 */
@DeveloperApi
abstract class SparkPlan extends QueryPlan[SparkPlan] with Logging with Serializable {

  /**
   * A handle to the SQL Context that was used to create this plan.   Since many operators need
   * access to the sqlContext for RDD operations or configuration this field is automatically
   * populated by the query planning infrastructure.
   */
  @transient
  protected[spark] final val sqlContext = SparkPlan.currentContext.get()

  protected def sparkContext = sqlContext.sparkContext

  // sqlContext will be null when we are being deserialized on the slaves.  In this instance
  // the value of codegenEnabled/unsafeEnabled will be set by the desserializer after the
  // constructor has run.
  val codegenEnabled: Boolean = if (sqlContext != null) {
    sqlContext.conf.codegenEnabled
  } else {
    false
  }
  val unsafeEnabled: Boolean = if (sqlContext != null) {
    sqlContext.conf.unsafeEnabled
  } else {
    false
  }

  /**
   * Whether the "prepare" method is called.
   */
  private val prepareCalled = new AtomicBoolean(false)

  /** Overridden make copy also propogates sqlContext to copied plan. */
  override def makeCopy(newArgs: Array[AnyRef]): SparkPlan = {
    SparkPlan.currentContext.set(sqlContext)
    super.makeCopy(newArgs)
  }

  /**
   * Return all metrics containing metrics of this SparkPlan.
   */
  private[sql] def metrics: Map[String, SQLMetric[_, _]] = Map.empty

  /**
   * Return a LongSQLMetric according to the name.
   */
  private[sql] def longMetric(name: String): LongSQLMetric =
    metrics(name).asInstanceOf[LongSQLMetric]

  // TODO: Move to `DistributedPlan`
  /** Specifies how data is partitioned across different nodes in the cluster. */
  def outputPartitioning: Partitioning = UnknownPartitioning(0) // TODO: WRONG WIDTH!

  /** Specifies any partition requirements on the input data for this operator. */
  def requiredChildDistribution: Seq[Distribution] =
    Seq.fill(children.size)(UnspecifiedDistribution)

  /** Specifies how data is ordered in each partition. */
  def outputOrdering: Seq[SortOrder] = Nil

  /** Specifies sort order for each partition requirements on the input data for this operator. */
  def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq.fill(children.size)(Nil)

  /** Specifies whether this operator outputs UnsafeRows */
  def outputsUnsafeRows: Boolean = false

  /** Specifies whether this operator is capable of processing UnsafeRows */
  def canProcessUnsafeRows: Boolean = false

  /**
   * Specifies whether this operator is capable of processing Java-object-based Rows (i.e. rows
   * that are not UnsafeRows).
   */
  def canProcessSafeRows: Boolean = true

  /**
   * Returns the result of this query as an RDD[InternalRow] by delegating to doExecute
   * after adding query plan information to created RDDs for visualization.
   * Concrete implementations of SparkPlan should override doExecute instead.
   */
  final def execute(): RDD[InternalRow] = {
    if (children.nonEmpty) {
      val hasUnsafeInputs = children.exists(_.outputsUnsafeRows)
      val hasSafeInputs = children.exists(!_.outputsUnsafeRows)
      assert(!(hasSafeInputs && hasUnsafeInputs),
        "Child operators should output rows in the same format")
      assert(canProcessSafeRows || canProcessUnsafeRows,
        "Operator must be able to process at least one row format")
      assert(!hasSafeInputs || canProcessSafeRows,
        "Operator will receive safe rows as input but cannot process safe rows")
      assert(!hasUnsafeInputs || canProcessUnsafeRows,
        "Operator will receive unsafe rows as input but cannot process unsafe rows")
    }
    RDDOperationScope.withScope(sparkContext, nodeName, false, true) {
      prepare()//预先执行
      doExecute()//真正执行
    }
  }

  /**
   * Prepare a SparkPlan for execution. It's idempotent.
   */
  final def prepare(): Unit = {
    if (prepareCalled.compareAndSet(false, true)) {
      doPrepare() //执行本类以及子子孙孙的预执行方法
      children.foreach(_.prepare())
    }
  }

  /**
   * Overridden by concrete implementations of SparkPlan. It is guaranteed to run before any
   * `execute` of SparkPlan. This is helpful if we want to set up some state before executing the
   * query, e.g., `BroadcastHashJoin` uses it to broadcast asynchronously.
   *
   * Note: the prepare method has already walked down the tree, so the implementation doesn't need
   * to call children's prepare methods.
   * 子类可以去覆盖,该方法表示预先执行的代码逻辑
   */
  protected def doPrepare(): Unit = {}

  /**
   * Overridden by concrete implementations of SparkPlan.
   * Produces the result of the query as an RDD[InternalRow]
   * 真正执行,由子类去实现
   */
  protected def doExecute(): RDD[InternalRow]

  /**
   * Runs this query returning the result as an array.
   * 执行该查询,并且将执行结果收集到本地
   */
  def executeCollect(): Array[Row] = {
    execute()//该方法执行sql查询
      .mapPartitions { iter => //该部分是将执行结果在每一个节点进行转换
      val converter = CatalystTypeConverters.createToScalaConverter(schema)
      iter.map(converter(_).asInstanceOf[Row])
    }.collect() //该方法是将最终的结果抓去到本地
  }

  /**
   * Runs this query returning the first `n` rows as an array.
   * 执行查询,返回前N条数据到本地节点
   * This is modeled after RDD.take but never runs any job locally on the driver.
   */
  def executeTake(n: Int): Array[Row] = {
    if (n == 0) {
      return new Array[Row](0)
    }

    val childRDD = execute().map(_.copy())//执行结果是RDD,即分布式

    val buf = new ArrayBuffer[InternalRow] //本地存储n条数据的结果集
    val totalParts = childRDD.partitions.length //总partition分区数量
    var partsScanned = 0 //扫描到第几个partition分区了
    while (buf.size < n && partsScanned < totalParts) { //只要buf空间还存在空余,则就不断循环抓去数据,同时分区还存在尚未抓去的分区
      // The number of partitions to try in this iteration. It is ok for this number to be
      // greater than totalParts because we actually cap it at totalParts in runJob.
      var numPartsToTry = 1
      if (partsScanned > 0) {//说明不是扫描第一个分区,因此要设置抓去多少个partition
        // If we didn't find any rows after the first iteration, just try all partitions next.
        // Otherwise, interpolate the number of partitions we need to try, but overestimate it
        // by 50%.
        if (buf.size == 0) {
          numPartsToTry = totalParts - 1 //抓去到最后一个partition
        } else {
          numPartsToTry = (1.5 * n * partsScanned / buf.size).toInt
        }
      }
      numPartsToTry = math.max(0, numPartsToTry)  // guard against negative num of partitions

      val left = n - buf.size //在以下partition上抓去多少条数据
      val p = partsScanned until math.min(partsScanned + numPartsToTry, totalParts) //设置要抓去的partition区间范围
      val sc = sqlContext.sparkContext
      val res =
        sc.runJob(childRDD, (it: Iterator[InternalRow]) => it.take(left).toArray, p) //真正去抓去数据

      res.foreach(buf ++= _.take(n - buf.size)) //抓去数据的结果存储到buf中
      partsScanned += numPartsToTry //更新扫描到第几个partition分区了
    }

    val converter = CatalystTypeConverters.createToScalaConverter(schema) //设置类型转换器
    buf.toArray.map(converter(_).asInstanceOf[Row]) //对buf内的数据进行类型转换
  }

  private[this] def isTesting: Boolean = sys.props.contains("spark.testing")//是否是测试

  //参数记录select中的表达式集合,以及from表所有的属性集合
  protected def newProjection(
      expressions: Seq[Expression], inputSchema: Seq[Attribute]): Projection = {
    log.debug(
      s"Creating Projection: $expressions, inputSchema: $inputSchema, codegen:$codegenEnabled")
    if (codegenEnabled) {
      try {
        GenerateProjection.generate(expressions, inputSchema)
      } catch {
        case e: Exception =>
          if (isTesting) {
            throw e
          } else {
            log.error("Failed to generate projection, fallback to interpret", e)
            new InterpretedProjection(expressions, inputSchema)
          }
      }
    } else {
      new InterpretedProjection(expressions, inputSchema)
    }
  }

  //易变的project,参数记录select中的表达式集合,以及from表所有的属性集合
  protected def newMutableProjection(
      expressions: Seq[Expression],
      inputSchema: Seq[Attribute]): () => MutableProjection = {
    log.debug(
      s"Creating MutableProj: $expressions, inputSchema: $inputSchema, codegen:$codegenEnabled")
    if(codegenEnabled) {
      try {
        GenerateMutableProjection.generate(expressions, inputSchema)
      } catch {
        case e: Exception =>
          if (isTesting) {
            throw e
          } else {
            log.error("Failed to generate mutable projection, fallback to interpreted", e)
            () => new InterpretedMutableProjection(expressions, inputSchema)
          }
      }
    } else {
      () => new InterpretedMutableProjection(expressions, inputSchema)
    }
  }

  //给定一行数据,返回boolean
  //参数是一个表达式,以及该表达式可能涉及到的from表的所有字段信息,将一行数据传给表达式,返回boolean
  protected def newPredicate(
      expression: Expression, inputSchema: Seq[Attribute]): (InternalRow) => Boolean = {
    if (codegenEnabled) {
      try {
        GeneratePredicate.generate(expression, inputSchema)
      } catch {
        case e: Exception =>
          if (isTesting) {
            throw e
          } else {
            log.error("Failed to generate predicate, fallback to interpreted", e)
            InterpretedPredicate.create(expression, inputSchema)
          }
      }
    } else {
      InterpretedPredicate.create(expression, inputSchema)
    }
  }

  //如何对一行数据进行排序
  protected def newOrdering(
      order: Seq[SortOrder],
      inputSchema: Seq[Attribute]): Ordering[InternalRow] = {
    if (codegenEnabled) {
      try {
        GenerateOrdering.generate(order, inputSchema)
      } catch {
        case e: Exception =>
          if (isTesting) {
            throw e
          } else {
            log.error("Failed to generate ordering, fallback to interpreted", e)
            new InterpretedOrdering(order, inputSchema)
          }
      }
    } else {
      new InterpretedOrdering(order, inputSchema)
    }
  }
  /**
   * Creates a row ordering for the given schema, in natural ascending order.
   * 返回自然排序的结果
   */
  protected def newNaturalAscendingOrdering(dataTypes: Seq[DataType]): Ordering[InternalRow] = {
    val order: Seq[SortOrder] = dataTypes.zipWithIndex.map {
      case (dt, index) => new SortOrder(BoundReference(index, dt, nullable = true), Ascending)
    }
    newOrdering(order, Seq.empty)
  }
}

//表示是一个叶子节点,没有子节点
private[sql] trait LeafNode extends SparkPlan {
  override def children: Seq[SparkPlan] = Nil
}

//一元操作符---一个子节点参数
private[sql] trait UnaryNode extends SparkPlan {
  def child: SparkPlan

  override def children: Seq[SparkPlan] = child :: Nil

  override def outputPartitioning: Partitioning = child.outputPartitioning
}

//二元操作符---二个子节点
private[sql] trait BinaryNode extends SparkPlan {
  def left: SparkPlan
  def right: SparkPlan

  override def children: Seq[SparkPlan] = Seq(left, right)
}
