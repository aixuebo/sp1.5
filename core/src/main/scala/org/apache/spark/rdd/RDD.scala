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

package org.apache.spark.rdd

import java.util.Random

import scala.collection.{mutable, Map}
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.reflect.{classTag, ClassTag}

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import org.apache.hadoop.io.{BytesWritable, NullWritable, Text}
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.TextOutputFormat

import org.apache.spark._
import org.apache.spark.Partitioner._
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.partial.BoundedDouble
import org.apache.spark.partial.CountEvaluator
import org.apache.spark.partial.GroupedCountEvaluator
import org.apache.spark.partial.PartialResult
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{BoundedPriorityQueue, Utils}
import org.apache.spark.util.collection.OpenHashMap
import org.apache.spark.util.random.{BernoulliSampler, PoissonSampler, BernoulliCellSampler,
  SamplingUtils}

/**
 * A Resilient Distributed Dataset (RDD), the basic abstraction in Spark. Represents an immutable,
 * RDD是弹性的分布式数据集,在spark中抽象的基础类,代表不变的类,
 * partitioned collection of elements that can be operated on in parallel. 
 * 被分隔的数据集能够支持并发的去操作
 * This class contains the basic operations available on all RDDs, such as `map`, `filter`, and `persist`. 
 * 在所有的RDD上都包含基本的操作,比如map filter persist等
 * In addition, [[org.apache.spark.rdd.PairRDDFunctions]] contains operations available only on RDDs of key-value
 * pairs, such as `groupByKey` and `join`;
 * 另外PairRDDFunctions包含的操作仅仅被适用于key-value键值对的RDD类型,例如groupByKey或者join
 * [[org.apache.spark.rdd.DoubleRDDFunctions]] contains operations available only on RDDs of
 * Doubles; 
 * DoubleRDDFunctions 包含的操作仅仅用于Double类型的RDD数据集
 * and [[org.apache.spark.rdd.SequenceFileRDDFunctions]] contains operations available on RDDs that
 * can be saved as SequenceFiles.
 * SequenceFileRDDFunctions包含的操作仅仅被用于SequenceFiles序列化文件的RDD数据集上
 * 
 * All operations are automatically available on any RDD of the right type (e.g. RDD[(Int, Int)]
 * through implicit.
 * 所有的操作被自动用于任何正确的RDD类型上,例如RDD[(Int, Int)
 * Internally, each RDD is characterized by five main properties:
 * 内部,每一个RDD的特性都有以下5个主要特点
 *  - A list of partitions 一个被分隔后的结果集,每一个分隔的内容可以支持并发操作
 *  - A function for computing each split 函数式计算每一个被拆分的数据集
 *  - A list of dependencies on other RDDs 可以序列化的方式依赖一组其他的RDD集合
 *  - Optionally, a Partitioner for key-value RDDs (e.g. to say that the RDD is hash-partitioned) key-value形式的RDD很随意的依赖任何拆分实现类
 *  - Optionally, a list of preferred locations to compute each split on (e.g. block locations for
 *    an HDFS file)优先本地数据计算每一个split,例如数据块在HDFS所在节点上优先运行
 *
 * All of the scheduling and execution in Spark is done based on these methods, allowing each RDD
 * to implement its own way of computing itself. 
 * 在spark中,所有的调度以及执行,都允许每一个RDD去用自己的方式去实现计算
 * Indeed, users can implement custom RDDs (e.g. for reading data from a new storage system) by overriding these functions.
 * 因此,用户能够实现自定义RDD,例如从一个存储系统中读取数据,覆盖以上那些函数即可
 * Please refer to the
 * [[http://www.cs.berkeley.edu/~matei/papers/2012/nsdi_spark.pdf Spark paper]] for more details
 * on RDD internals.
 * 详细可以参见该spark关于RDD的论文
 * 
 * @参数 T,是一个泛型.对应RDD的类型class,例如RDD[Double],表示Double类型的RDD,RDD[(K, V)]表示元组元素
 * 注意:RDD的泛型不允许是RDD类型,如果是RDD类型,说明就是RDD嵌套了,暂时是不支持的
 */
abstract class RDD[T: ClassTag](
    @transient private var _sc: SparkContext,//spark环境信息
    @transient private var deps: Seq[Dependency[_]] //该RDD依赖的RDD集合
  ) extends Serializable with Logging {

  if (classOf[RDD[_]].isAssignableFrom(elementClassTag.runtimeClass)) {//即RDD的泛型不允许是RDD类型,如果是RDD类型,说明就是RDD嵌套了,暂时是不支持的
    // This is a warning instead of an exception in order to avoid breaking user programs that
    // might have defined nested RDDs without running jobs with them.
    //这是一个警告,去代替一个异常信息,为了避免中断用户的程序,该警告的意思是说可能RDD被定义成嵌套的RDD了,该RDD是不能被运行的
    logWarning("Spark does not support nested RDDs (see SPARK-5063)") //目前spark不支持嵌套RDD
  }

  override def hashCode(): Int = super.hashCode()

  //RDD所持有的spark环境必须存在,不能是null
  private def sc: SparkContext = {
    if (_sc == null) {
      throw new SparkException(
        "RDD transformations and actions can only be invoked by the driver, not inside of other " +
        "transformations; for example, rdd1.map(x => rdd2.values.count() * x) is invalid because " +
        "the values transformation and count action cannot be performed inside of the rdd1.map " +
        "transformation. For more information, see SPARK-5063.")
      /**
       * RDD的转变和行为操作仅仅能被执行时通过自己的Driver,而不是其他RDD内部调用
       * 例如rdd1.map(x => rdd2.values.count() * x)  是非法操作,
       * 因为values转换和count计算不是在RDD1的map中进行转换的
       * 即RDD内部的转换必须接收的是函数式编程,而不是其他RDD
       */
    }
    _sc
  }

  /** Construct an RDD with just a one-to-one dependency on one parent 
   * 创建一个RDD,该创建的RDD仅仅依赖一个RDD 
   **/
  def this(@transient oneParent: RDD[_]) =
    this(oneParent.context , List(new OneToOneDependency(oneParent)))

  private[spark] def conf = sc.conf //获取环境信息
  // =======================================================================
  // Methods that should be implemented by subclasses of RDD 子类RDD要自己实现的方法
  // =======================================================================

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   * 子类进行操作每一个partition,返回值是RDD的泛型类型的迭代器
    *
    * 相当于hadoop的InputFormat中的read一个数据块的功能
   */
  @DeveloperApi
  def compute(split: Partition, context: TaskContext): Iterator[T]

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   * 子类实现,将RDD进行拆分,拆分成一组partitions集合,这些partitions可以并发执行
   * 该方法仅仅被调用一次，因此安全的去实现一个非常耗时的计算在这里面
    *
    * 相当于hadoop的InputFormat中的split功能
   */
  protected def getPartitions: Array[Partition]

  /**
   * Implemented by subclasses to return how this RDD depends on parent RDDs. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   * 子类去实现,该RDD如何依赖父RDD,
   * 这个方法也仅仅被调用一次,也因此安全的去实现一个非常耗时的计算在这里面
   *
   * 当一个子RDD对应多个父RDD的时候这个方法最常用,因为该方法返回每一个RDD的依赖
   *
   * 返回该RDD与父RDD的依赖关系,是一对多还是多对多,以及Dependency中可以让一个partition获取对应的父RDD的若干个匹配的partition
   */
  protected def getDependencies: Seq[Dependency[_]] = deps

  /**
   * Optionally overridden by subclasses to specify placement preferences.
   * 获取该partition对应的文件在哪些路径上可以获取
   */
  protected def getPreferredLocations(split: Partition): Seq[String] = Nil

  /** Optionally overridden by subclasses to specify how they are partitioned. 
   *  拆分对象,该类去指明如何拆分RDD
    *  相当于hadoop的map中如何将key分配到哪个reduce的过程
   **/
  @transient val partitioner: Option[Partitioner] = None

  // =======================================================================
  // Methods and fields available on all RDDs
  // =======================================================================

  /** The SparkContext that created this RDD. */
  def sparkContext: SparkContext = sc

  /** A unique ID for this RDD (within its SparkContext). 
   * RDD的唯一ID 
   **/
  val id: Int = sc.newRddId()

  /** A friendly name for this RDD 
   * RDD友好的名字 
   **/
  @transient var name: String = null

  /** Assign a name to this RDD */
  def setName(_name: String): this.type = {
    name = _name
    this
  }

  /**
   * Mark this RDD for persisting using the specified level.
   *
   * @param newLevel the target storage level
   * @param allowOverride whether to override any existing level with the new one 是否覆盖已经存在的存储级别
   */
  private def persist(newLevel: StorageLevel, allowOverride: Boolean): this.type = {
    // TODO: Handle changes of StorageLevel
    if (storageLevel != StorageLevel.NONE && newLevel != storageLevel && !allowOverride) {//如果allowOverride是false,表示不能更改存储级别
      throw new UnsupportedOperationException(
        "Cannot change storage level of an RDD after it was already assigned a level")
    }
    // If this is the first time this RDD is marked for persisting, register it
    // with the SparkContext for cleanups and accounting. Do this only once.
    //如果是第一次表示存储该RDD,因此要给context注册,这个操作只会做一次
    if (storageLevel == StorageLevel.NONE) {
      sc.cleaner.foreach(_.registerRDDForCleanup(this))
      sc.persistRDD(this)//存储该RDD
    }
    storageLevel = newLevel //更改存储级别
    this
  }

  /**
   * Set this RDD's storage level to persist its values across operations after the first time
   * it is computed. This can only be used to assign a new storage level if the RDD does not
   * have a storage level set yet. Local checkpointing is an exception.
   * 仅仅是应用在该RDD没有设置存储级别的时候
   */
  def persist(newLevel: StorageLevel): this.type = {
    if (isLocallyCheckpointed) {
      // This means the user previously called localCheckpoint(), which should have already
      // marked this RDD for persisting. Here we should override the old storage level with
      // one that is explicitly requested by the user (after adapting it to use disk).
      persist(LocalRDDCheckpointData.transformStorageLevel(newLevel), allowOverride = true)
    } else {
      persist(newLevel, allowOverride = false) //不允许覆盖
    }
  }

  /** Persist this RDD with the default storage level (`MEMORY_ONLY`). 
   *  默认存储RDD在内存中
   **/
  def persist(): this.type = persist(StorageLevel.MEMORY_ONLY)

  /** Persist this RDD with the default storage level (`MEMORY_ONLY`).
   *  存储RDD在内存中
   **/
  def cache(): this.type = persist()

  /**
   * Mark the RDD as non-persistent, and remove all blocks for it from memory and disk.
   * 使RDD不进行缓存,并且从内存和磁盘上移除所有的数据块
   * @param blocking Whether to block until all blocks are deleted.是否阻塞,直到所有的数据块被删除
   * @return This RDD.
   * 移除RDD的缓存
   */
  def unpersist(blocking: Boolean = true): this.type = {
    logInfo("Removing RDD " + id + " from persistence list")
    sc.unpersistRDD(id, blocking) //取消注册rdd的实例化
    storageLevel = StorageLevel.NONE //存储级别为NOE
    this
  }

  /** Get the RDD's current storage level, or StorageLevel.NONE if none is set. */
  def getStorageLevel: StorageLevel = storageLevel

  // Our dependencies and partitions will be gotten by calling subclass's methods below, and will
  // be overwritten when we're checkpointed
  private var dependencies_ : Seq[Dependency[_]] = null
  @transient private var partitions_ : Array[Partition] = null//该RDD分成多少个分片,如果已经RDD已经checkpoint到HDFS上了,那么也不再需要父RDD的依赖关系了

  /** An Option holding our checkpoint RDD, if we are checkpointed 如果我们已经checkPoint了,则获取该RDD的checkPoint对象*/
  private def checkpointRDD: Option[CheckpointRDD[T]] = checkpointData.flatMap(_.checkpointRDD)

  /**
   * Get the list of dependencies of this RDD, taking into account whether the
   * RDD is checkpointed or not.
   */
  final def dependencies: Seq[Dependency[_]] = {
    checkpointRDD.map(r => List(new OneToOneDependency(r))).getOrElse {//先从checkpointRDD中获取依赖,如果没有,则获取本身依赖的RDD,即该rdd依赖checkpoint对应的RDD
      if (dependencies_ == null) {
        dependencies_ = getDependencies
      }
      dependencies_
    }
  }

  /**
   * Get the array of partitions of this RDD, taking into account whether the
   * RDD is checkpointed or not.
   * 返回该RDD所有的partition分区集合
   */
  final def partitions: Array[Partition] = {
    checkpointRDD.map(_.partitions).getOrElse {
      if (partitions_ == null) {
        partitions_ = getPartitions
      }
      partitions_
    }
  }

  /**
   * Get the preferred locations of a partition, taking into account whether the
   * RDD is checkpointed.
   * 获取该partition对应的文件在哪些路径上可以获取
   */
  final def preferredLocations(split: Partition): Seq[String] = {
    checkpointRDD.map(_.getPreferredLocations(split)).getOrElse {
      getPreferredLocations(split)
    }
  }

  /**
   * Internal method to this RDD; will read from cache if applicable, or otherwise compute it.
   * This should ''not'' be called by users directly, but is available for implementors of custom
   * subclasses of RDD.
   */
  final def iterator(split: Partition, context: TaskContext): Iterator[T] = {
    if (storageLevel != StorageLevel.NONE) {//说明有缓存,从缓存中获取
      SparkEnv.get.cacheManager.getOrCompute(this, split, context, storageLevel)
    } else {//说明没有缓存,读取原始文件或者从checkpoint中获取
      computeOrReadCheckpoint(split, context)
    }
  }

  /**
   * Return the ancestors of the given RDD that are related to it only through a sequence of
   * narrow dependencies. This traverses the given RDD's dependency tree using DFS, but maintains
   * no ordering on the RDDs returned.
    * 返回给定RDD的祖先关系,仅仅要祖先是NarrowDependency的,返回结果是没有顺序的
   */
  private[spark] def getNarrowAncestors: Seq[RDD[_]] = {
    val ancestors = new mutable.HashSet[RDD[_]]//存储所有父RDD的集合,是没有顺序的

    def visit(rdd: RDD[_]) {//递归参数RDD操作
      val narrowDependencies = rdd.dependencies.filter(_.isInstanceOf[NarrowDependency[_]])//只要父RDD中NarrowDependency类型的
      val narrowParents = narrowDependencies.map(_.rdd)//父RDD集合
      val narrowParentsNotVisited = narrowParents.filterNot(ancestors.contains)//过滤ancestors不存在的父RDD
      narrowParentsNotVisited.foreach { parent =>
        ancestors.add(parent)//添加父RDD
        visit(parent)//继续递归该父RDD的父RDD
      }
    }

    visit(this)//将RDD本身作为参数传过去,进行递归操作

    // In case there is a cycle, do not include the root itself 刨除自己RDD本身
    ancestors.filterNot(_ == this).toSeq
  }

  /**
   * Compute an RDD partition or read it from a checkpoint if the RDD is checkpointing.
   * 要么从Checkpoint的RDD中找到结果,要么直接读取HDFS属于该数据块的内容,返回数据块行记录的迭代器
   */
  private[spark] def computeOrReadCheckpoint(split: Partition, context: TaskContext): Iterator[T] =
  {
    if (isCheckpointedAndMaterialized) {//说明已经checkpoint了,因此firstParent就是存储HDFS上的内容
      firstParent[T].iterator(split, context)//读取HDFS上的内容
    } else {
      compute(split, context)
    }
  }

  /**
   * Execute a block of code in a scope such that all new RDDs created in this body will
   * be part of the same scope. For more detail, see {{org.apache.spark.rdd.RDDOperationScope}}.
   *
   * Note: Return statements are NOT allowed in the given body.
    * 执行body代码块在一个单独的scope中,所有在body中被创建的RDD都是有相同的scope
    * body是返回U的,因此该返回值也是返回U
   * 执行一个无法参数的函数体,有返回值
   */
  private[spark] def withScope[U](body: => U): U = RDDOperationScope.withScope[U](sc)(body)

  // Transformations (return a new RDD)

  /**
   * Return a new RDD by applying a function to all elements of this RDD.
   * 返回一个新的RDD,参数是函数f,该函数的f是RDD的泛型对象,转换成U对象
   */
  def map[U: ClassTag](f: T => U): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[U, T](this, (context, pid, iter) => iter.map(cleanF))//表示每一个元素都执行f函数,将原本的T转换成U
  }

  /**
   *  Return a new RDD by first applying a function to all elements of this
   *  RDD, and then flattening the results.
   *  参数f表示传入RDD的每一条记录,返回可以迭代U新的RDD的迭代器
   *
   *  每一个元素进行flatMap处理
   *  与map类似，区别是原RDD中的元素经map处理后只能生成一个元素，而原RDD中的元素经flatmap处理后可生成多个元素来构建新RDD。
举例：对原RDD中的每个元素x产生y个元素（从1到y，y为元素x的值）
scala> val a = sc.parallelize(1 to 4, 2) scala> val b = a.flatMap(x => 1 to x) scala> b.collect res12: Array[Int] = Array(1, 1, 2, 1, 2, 3, 1, 2, 3, 4)
   */
  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[U, T](this, (context, pid, iter) => iter.flatMap(cleanF))//表示每一个元素都f函数处理后的结果,在进行scala的flatMap处理
  }

  /**
   * Return a new RDD containing only the elements that satisfy a predicate.
   * 每一个partition对每一行记录进行filter处理,处理函数是f
   * 参数f表示传入RDD的每一条记录,返回repartitionAndSorboolean值,true表示需要被保留
   */
  def filter(f: T => Boolean): RDD[T] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[T, T](
      this,
      (context, pid, iter) => iter.filter(cleanF),
      preservesPartitioning = true)
  }

  /**
   * Return a new RDD containing the distinct elements in this RDD.
    * 返回一个新的RDD,新的RDD就是把老的RDD中重复的元素去掉了
   * 1.RDD -> MapPartitionsRDD[U, T]  即x--转换成x null的key-value形式RDD
   * 2.key-value的RDD 隐式转换--PairRDDFunctions,并且调用reduceByKey方法,返回RDD[(K, V)]
   reduceByKey参数(x, y) => x  表示输入x和y的内容,即连续两个value,返回任意一个value即可    numPartitions表示最终的结果要输出到几个partition中
   * 3.获取最终的RDD[Key]即可
   */
  def distinct(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
    //这一系列过程其实是中间转换了好几次RDD切换,因此已经有好几层的RDD了,并且partitioner也不一样了
    //reduceByKey中的函数是元组x,y转换成x
    map(x => (x, null)).reduceByKey((x, y) => x, numPartitions).map(_._1)//其实函数可以固定成任意字符,也是可以的,因为是过滤key,与value无关
  }

  /**
   * Return a new RDD containing the distinct elements in this RDD.
   */
  def distinct(): RDD[T] = withScope {
    distinct(partitions.length)//最终partition数量就是和本身RDD的数量相同
  }

  /**
   * Return a new RDD that has exactly numPartitions partitions.
   *
   * Can increase or decrease the level of parallelism in this RDD. Internally, this uses
   * a shuffle to redistribute data.
   *
   * If you are decreasing the number of partitions in this RDD, consider using `coalesce`,
   * which can avoid performing a shuffle.
   */
  def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
    coalesce(numPartitions, shuffle = true)
  }

  /**
   * Return a new RDD that is reduced into `numPartitions` partitions.
   *
   * This results in a narrow dependency, e.g. if you go from 1000 partitions
   * to 100 partitions, there will not be a shuffle, instead each of the 100
   * new partitions will claim 10 of the current partitions.
   *
   * However, if you're doing a drastic coalesce, e.g. to numPartitions = 1,
   * this may result in your computation taking place on fewer nodes than
   * you like (e.g. one node in the case of numPartitions = 1). To avoid this,
   * you can pass shuffle = true. This will add a shuffle step, but means the
   * current upstream partitions will be executed in parallel (per whatever
   * the current partitioning is).
   *
   * Note: With shuffle = true, you can actually coalesce to a larger number
   * of partitions. This is useful if you have a small number of partitions,
   * say 100, potentially with a few partitions being abnormally large. Calling
   * coalesce(1000, shuffle = true) will result in 1000 partitions with the
   * data distributed using a hash partitioner.
   * 参数shuffle = true表示要进行一次shuffe操作去重新设置partition
   * 如果shuffle = false 表示不会进行shuffle,只是一对多的,将多个父RDD的partition合并成一个,虽然有网络IO,但是不涉及key相同的都要在一个节点上存在步骤
   *
   * 参数numPartitions 表示最终需要的partition数量
   * 该函数是将父RDD的partition数量重新调整,一般用于RDD的partition较多又较小的时候,重新规划
   */
  def coalesce(numPartitions: Int, shuffle: Boolean = false)(implicit ord: Ordering[T] = null)
  : RDD[T] = withScope {
    if (shuffle) {//需要shuffle去更改父RDD的partition数量
      /** Distributes elements evenly across output partitions, starting from a random partition.
        *  对每一个partition的流进行一个函数处理,返回新的RDD
        *  index表示第几个partition,迭代器表示该partition的内部元素
        **/
      val distributePartition = (index: Int, items: Iterator[T]) => {
        var position = (new Random(index)).nextInt(numPartitions) //随机找一个新的partition位置
        items.map { t =>
          // Note that the hash code of the key will just be the key itself. The HashPartitioner
          // will mod it with the number of total partitions.
          position = position + 1
          (position, t)
        }
      } : Iterator[(Int, T)] //保证每一个partition的内容转换成顺序的intid以及对应的原始对象T,即增加了一个顺序id,让shuffle根据id作为key的时候更均匀的分布在多个节点上,依然不用保证key相同的都在同一个节点上

      //shuffle的过程是根据目标reduce分区数量,在map端将所有元素随机打乱到任意一个reduce分区中
      // include a shuffle step so that our upstream tasks are still distributed

      /**
ShuffledRDD[K, V, C]
而泛型的内容是ShuffledRDD[Int, T, T],说明处理后的map端输出是[Int, T],即int和原始元素内容本身。
reduce的结果是T,即reduce合并的结果也是T,即reduce的结果是[Int,T]
       */
      new CoalescedRDD(
        new ShuffledRDD[Int, T, T](mapPartitionsWithIndex(distributePartition),
          new HashPartitioner(numPartitions)),//先进行shuffle操作,操作后再进行partition调整
        numPartitions).values
    } else {
      new CoalescedRDD(this, numPartitions)
    }
  }

  /**
   * Return a sampled subset of this RDD.
   *
   * @param withReplacement can elements be sampled multiple times (replaced when sampled out)
   * @param fraction expected size of the sample as a fraction of this RDD's size
   *  without replacement: probability that each element is chosen; fraction must be [0, 1]
   *  with replacement: expected number of times each element is chosen; fraction must be >= 0
   * @param seed seed for the random number generator
   */
  def sample(
      withReplacement: Boolean,
      fraction: Double,
      seed: Long = Utils.random.nextLong): RDD[T] = withScope {
    require(fraction >= 0.0, "Negative fraction value: " + fraction)
    if (withReplacement) {
      new PartitionwiseSampledRDD[T, T](this, new PoissonSampler[T](fraction), true, seed)
    } else {
      new PartitionwiseSampledRDD[T, T](this, new BernoulliSampler[T](fraction), true, seed)
    }
  }

  /**
   * Randomly splits this RDD with the provided weights.
   *
   * @param weights weights for splits, will be normalized if they don't sum to 1
   * @param seed random seed
   *
   * @return split RDDs in an array
   *
scala> val weights = Array[Double](0.3,0.2,0.5)
weights: Array[Double] = Array(0.3, 0.2, 0.5)

scala> val sum = weights.sum
sum: Double = 1.0

scala> val normalizedCumWeights = weights.map(_ / sum).scanLeft(0.0d)(_ + _)
normalizedCumWeights: Array[Double] = Array(0.0, 0.3, 0.5, 1.0)


根据权重,数组weights长度是多少,则就对RDD循环抽样多少次,返回RDD数组,每一个数组内容就是抽样的结果RDD
常用于从原始数据中抽出多少比例的训练集合和测试集合
   */
  def randomSplit(
      weights: Array[Double],
      seed: Long = Utils.random.nextLong): Array[RDD[T]] = withScope {
    val sum = weights.sum //数组内的数字之和
    val normalizedCumWeights = weights.map(_ / sum).scanLeft(0.0d)(_ + _) //初始化0.然后每一个数组中的元素与运算后的结果 参与运算得到的值 组成新的数组
    //normalizedCumWeights.sliding(2) 表示每两个组成一组数据,即将normalizedCumWeights拆分成每一个区间范围
    normalizedCumWeights.sliding(2).map { x =>
      randomSampleWithRange(x(0), x(1), seed) //区间的最大值和最小值以及种子
    }.toArray
  }

  /**
   * Internal method exposed for Random Splits in DataFrames. Samples an RDD given a probability
   * range.
   * @param lb lower bound to use for the Bernoulli sampler 区间的最小值
   * @param ub upper bound to use for the Bernoulli sampler 区间的最大值
   * @param seed the seed for the Random number generator 种子
   * @return A random sub-sample of the RDD without replacement. 随机抽取数据,随机数满足最大值和最小值的时候,该数据就是我们要的抽样数据
   * 该方法会循环一次RDD,产生新的一个RDD抽样结果集
   */
  private[spark] def randomSampleWithRange(lb: Double, ub: Double, seed: Long): RDD[T] = {
    this.mapPartitionsWithIndex( { (index, partition) =>
      val sampler = new BernoulliCellSampler[T](lb, ub)
      sampler.setSeed(seed + index)
      sampler.sample(partition) //对该partition的迭代器进行抽样
    }, preservesPartitioning = true)
  }

  /**
   * Return a fixed-size sampled subset of this RDD in an array
   *
   * @param withReplacement whether sampling is done with replacement
   * @param num size of the returned sample
   * @param seed seed for the random number generator
   * @return sample of specified size in an array
   */
  // TODO: rewrite this without return statements so we can wrap it in a scope
  def takeSample(
      withReplacement: Boolean,
      num: Int,
      seed: Long = Utils.random.nextLong): Array[T] = {
    val numStDev = 10.0

    if (num < 0) {
      throw new IllegalArgumentException("Negative number of elements requested")
    } else if (num == 0) {
      return new Array[T](0)
    }

    val initialCount = this.count()
    if (initialCount == 0) {
      return new Array[T](0)
    }

    val maxSampleSize = Int.MaxValue - (numStDev * math.sqrt(Int.MaxValue)).toInt
    if (num > maxSampleSize) {
      throw new IllegalArgumentException("Cannot support a sample size > Int.MaxValue - " +
        s"$numStDev * math.sqrt(Int.MaxValue)")
    }

    val rand = new Random(seed)
    if (!withReplacement && num >= initialCount) {
      return Utils.randomizeInPlace(this.collect(), rand)
    }

    val fraction = SamplingUtils.computeFractionForSampleSize(num, initialCount,
      withReplacement)

    var samples = this.sample(withReplacement, fraction, rand.nextInt()).collect()

    // If the first sample didn't turn out large enough, keep trying to take samples;
    // this shouldn't happen often because we use a big multiplier for the initial size
    var numIters = 0
    while (samples.length < num) {
      logWarning(s"Needed to re-sample due to insufficient sample size. Repeat #$numIters")
      samples = this.sample(withReplacement, fraction, rand.nextInt()).collect()
      numIters += 1
    }

    Utils.randomizeInPlace(samples, rand).take(num)
  }

  /**
   * Return the union of this RDD and another one. Any identical elements will appear multiple
   * times (use `.distinct()` to eliminate them).
   * 将两个RDD进行合并，不去重
   */
  def union(other: RDD[T]): RDD[T] = withScope {
    if (partitioner.isDefined && other.partitioner == partitioner) {
      new PartitionerAwareUnionRDD(sc, Array(this, other)) //一对一的合并,就最终一个partition各读取读取父RDD中一个partition
    } else {
      new UnionRDD(sc, Array(this, other)) //最终生成两个RDD的partition数量之和
    }
  }

  /**
   * Return the union of this RDD and another one. Any identical elements will appear multiple
   * times (use `.distinct()` to eliminate them).
   */
  def ++(other: RDD[T]): RDD[T] = withScope {
    this.union(other)
  }

  /**
   * Return this RDD sorted by the given key function.
   * 按照函数f对元素进行排序,返回的RDD还是原来的RDD,只是进行了一次按照f函数来排序的过程
   */
  def sortBy[K](
      f: (T) => K,//T转换成K的转换函数
      ascending: Boolean = true,//是否升序
      numPartitions: Int = this.partitions.length)
      (implicit ord: Ordering[K], ctag: ClassTag[K]) //K是有排序功能的
    : RDD[T] = withScope {
    this.keyBy[K](f) //T元素通过K转换成元组(K,T)
        .sortByKey(ascending, numPartitions)
        .values
  }

  /**
   * Return the intersection of this RDD and another one. The output will not contain any duplicate
   * elements, even if the input RDDs did.
   *
   * Note that this method performs a shuffle internally.
   * 函数返回两个RDD的交集，并且去重。
   */
  def intersection(other: RDD[T]): RDD[T] = withScope {
    this.map(v => (v, null)).cogroup(other.map(v => (v, null)))  //相同key--产生两个迭代器
        .filter { case (_, (leftGroup, rightGroup)) => leftGroup.nonEmpty && rightGroup.nonEmpty } //过滤,_表示相同的key,如果两个集合都存在元素,则说明该key两个有交集,因此保留
        .keys //返回所有有交集的key
  }

  /**
   * Return the intersection of this RDD and another one. The output will not contain any duplicate
   * elements, even if the input RDDs did.
   *
   * Note that this method performs a shuffle internally.
   *
   * @param partitioner Partitioner to use for the resulting RDD
   */
  def intersection(
      other: RDD[T],
      partitioner: Partitioner)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
    this.map(v => (v, null)).cogroup(other.map(v => (v, null)), partitioner)
        .filter { case (_, (leftGroup, rightGroup)) => leftGroup.nonEmpty && rightGroup.nonEmpty }
        .keys
  }

  /**
   * Return the intersection of this RDD and another one. The output will not contain any duplicate
   * elements, even if the input RDDs did.  Performs a hash partition across the cluster
   *
   * Note that this method performs a shuffle internally.
   *
   * @param numPartitions How many partitions to use in the resulting RDD
   */
  def intersection(other: RDD[T], numPartitions: Int): RDD[T] = withScope {
    intersection(other, new HashPartitioner(numPartitions))
  }

  /**
   * Return an RDD created by coalescing all elements within each partition into an array.
   * 每一个分区内所有元素都进入到一个数组中
   * 函数是将RDD中每一个分区中类型为T的元素转换成Array[T]，这样每一个分区就只有一个数组元素。
   */
  def glom(): RDD[Array[T]] = withScope {
    new MapPartitionsRDD[Array[T], T](this, (context, pid, iter) => Iterator(iter.toArray)) //每一个partition内元素转换成数组iter.toArray
  }

  /**
   * Return the Cartesian product of this RDD and another one, that is, the RDD of all pairs of
   * elements (a, b) where a is in `this` and b is in `other`.
   * 对两个RDD进行笛卡尔计算,返回RDD[(T, U)],这个函数对内存消耗较大,使用时候需要谨慎
   * 即两个RDD每一条记录都相互笛卡尔运算
   */
  def cartesian[U: ClassTag](other: RDD[U]): RDD[(T, U)] = withScope {
    new CartesianRDD(sc, this, other)
  }

  /**
   * Return an RDD of grouped items. Each group consists of a key and a sequence of elements
   * mapping to that key. The ordering of elements within each group is not guaranteed, and
   * may even differ each time the resulting RDD is evaluated.
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using [[PairRDDFunctions.aggregateByKey]]
   * or [[PairRDDFunctions.reduceByKey]] will provide much better performance.
   *
   * function返回key，传入的RDD的各个元素根据这个key进行分组,value就是原始内容组成的集合
   val a = sc.parallelize(1 to 9, 3)
  groupBy(x => { if (x % 2 == 0) "even" else "odd" }).collect//分成两组
  结果
  Array(
  (even,ArrayBuffer(2, 4, 6, 8)),
  (odd,ArrayBuffer(1, 3, 5, 7, 9))
  )
   */
  def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = withScope {
    groupBy[K](f, defaultPartitioner(this))
  }

  /**
   * Return an RDD of grouped elements. Each group consists of a key and a sequence of elements
   * mapping to that key. The ordering of elements within each group is not guaranteed, and
   * may even differ each time the resulting RDD is evaluated.
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using [[PairRDDFunctions.aggregateByKey]]
   * or [[PairRDDFunctions.reduceByKey]] will provide much better performance.
   */
  def groupBy[K](
      f: T => K,
      numPartitions: Int)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = withScope {
    groupBy(f, new HashPartitioner(numPartitions))
  }

  /**
   * Return an RDD of grouped items. Each group consists of a key and a sequence of elements
   * mapping to that key. The ordering of elements within each group is not guaranteed, and
   * may even differ each time the resulting RDD is evaluated.
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using [[PairRDDFunctions.aggregateByKey]]
   * or [[PairRDDFunctions.reduceByKey]] will provide much better performance.
   */
  def groupBy[K](f: T => K, p: Partitioner)(implicit kt: ClassTag[K], ord: Ordering[K] = null)
      : RDD[(K, Iterable[T])] = withScope {
    val cleanF = sc.clean(f)
    this.map(t => (cleanF(t), t)).groupByKey(p) //map 将key转换成(value,key),然后按照value进行group by操作
  }

  /**
   * Return an RDD created by piping elements to a forked external process.
   */
  def pipe(command: String): RDD[String] = withScope {
    new PipedRDD(this, command)
  }

  /**
   * Return an RDD created by piping elements to a forked external process.
   */
  def pipe(command: String, env: Map[String, String]): RDD[String] = withScope {
    new PipedRDD(this, command, env)
  }

  /**
   * Return an RDD created by piping elements to a forked external process.
   * The print behavior can be customized by providing two functions.
   *
   * @param command command to run in forked process.要运行的命令
   * @param env environment variables to set.设置额外的环境变量信息
   * @param printPipeContext Before piping elements, this function is called as an opportunity
   *                         to pipe context data. Print line function (like out.println) will be
   *                         passed as printPipeContext's parameter.在管道元素之前,这个函数有一次机会去管道上下文信息
   * @param printRDDElement Use this function to customize how to pipe elements. This function
   *                        will be called with each RDD element as the 1st parameter, and the
   *                        print line function (like out.println()) as the 2nd parameter.
   *                        An example of pipe the RDD data of groupBy() in a streaming way,
   *                        instead of constructing a huge String to concat all the elements:
   *                        def printRDDElement(record:(String, Seq[String]), f:String=>Unit) =
   *                          for (e <- record._2){f(e)}
   *                         使用该函数,可以自定义将原始元素转换一下,第一个参数就是RDD的原始元素,第二个函数类似于out.println(_)这样的函数
   *                         例如:groupBy之后的数据作为数据流传入到pipe中,格式是record:(String, Seq[String])的,但是很不容易让后续脚本操作,最好脚本操作的都是String类型的,
   *                         因此代替方案是,将属于一个key的集合转换成String类型,然后在将String类型调用f函数进行输出到输入流中
   *
   * @param separateWorkingDir Use separate working directories for each task.每一个任务是否使用单独的工作目录,即是否要进行软连接copy
   * @return the result RDD
   *
   *  类似与hadoop的streaming方式,让非java和scala的用户也可以使用集群并行的运算数据
   *  只要服务器上的语言可以支持从标准输入读取数据--运算--输出字符串到标准输出 这种模式,都可以参与到并行计算里面来
   *
   */
  def pipe(
      command: Seq[String],
      env: Map[String, String] = Map(),
      printPipeContext: (String => Unit) => Unit = null,
      printRDDElement: (T, String => Unit) => Unit = null,
      separateWorkingDir: Boolean = false): RDD[String] = withScope {
    new PipedRDD(this, command, env,
      if (printPipeContext ne null) sc.clean(printPipeContext) else null,
      if (printRDDElement ne null) sc.clean(printRDDElement) else null,
      separateWorkingDir)
  }

  /**
   * Return a new RDD by applying a function to each partition of this RDD.
   *
   * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
   * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
   * 相当于Map操作
   * 参数f是传递每一个RDD上的实体T,转换成实体U
   *
   * mapPartitions是map的一个变种。map的输入函数是应用于RDD中每个元素，而mapPartitions的输入函数是应用于每个分区，也就是把每个分区中的内容作为整体来处理的。
   */
  def mapPartitions[U: ClassTag](
      f: Iterator[T] => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U] = withScope {
    val cleanedF = sc.clean(f)
    new MapPartitionsRDD(
      this,
      (context: TaskContext, index: Int, iter: Iterator[T]) => cleanedF(iter),
      preservesPartitioning)
  }

  /**
   * Return a new RDD by applying a function to each partition of this RDD, while tracking the index
   * of the original partition.
   * 对每一个partition的流进行一个函数处理,返回新的RDD
   *
   * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
   * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
   */
  def mapPartitionsWithIndex[U: ClassTag](
      f: (Int, Iterator[T]) => Iterator[U], //f参数int表示第几个partition,iter表示该partition的数据源的迭代器,返回新的迭代器
      preservesPartitioning: Boolean = false): RDD[U] = withScope {
    val cleanedF = sc.clean(f)
    new MapPartitionsRDD(
      this,
      (context: TaskContext, index: Int, iter: Iterator[T]) => cleanedF(index, iter),
      preservesPartitioning)
  }

  /**
   * :: DeveloperApi ::
   * Return a new RDD by applying a function to each partition of this RDD. This is a variant of
   * mapPartitions that also passes the TaskContext into the closure.
   *
   * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
   * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
    * 已经过期了,其实就是mapPartitions方法
   */
  @DeveloperApi
  @deprecated("use TaskContext.get", "1.2.0")
  def mapPartitionsWithContext[U: ClassTag](
      f: (TaskContext, Iterator[T]) => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    val func = (context: TaskContext, index: Int, iter: Iterator[T]) => cleanF(context, iter)
    new MapPartitionsRDD(this, sc.clean(func), preservesPartitioning)
  }

  /**
   * Return a new RDD by applying a function to each partition of this RDD, while tracking the index
   * of the original partition.
    * 已经过期了,现在使用mapPartitionsWithIndex方法
   */
  @deprecated("use mapPartitionsWithIndex", "0.7.0")
  def mapPartitionsWithSplit[U: ClassTag](
      f: (Int, Iterator[T]) => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U] = withScope {
    mapPartitionsWithIndex(f, preservesPartitioning)
  }

  /**
   * Maps f over this RDD, where f takes an additional parameter of type A.  This
   * additional parameter is produced by constructA, which is called in each
   * partition with the index of that partition.
   *
   * partition的index作为参数,传递给constructA,返回值是A
   * 然后该partition分区的每一个元素,都进行map处理,f函数的参数是元素原始内容和A
    *
    * 过期了,现在就是mapPartitionsWithIndex
   */
  @deprecated("use mapPartitionsWithIndex", "1.0.0")
  def mapWith[A, U: ClassTag]
      (constructA: Int => A, preservesPartitioning: Boolean = false)
      (f: (T, A) => U): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    val cleanA = sc.clean(constructA)
    mapPartitionsWithIndex((index, iter) => {
      val a = cleanA(index) //partition的index作为参数,传递给constructA,返回值是A
      iter.map(t => cleanF(t, a))
    }, preservesPartitioning)
  }

  /**
   * FlatMaps f over this RDD, where f takes an additional parameter of type A.  This
   * additional parameter is produced by constructA, which is called in each
   * partition with the index of that partition.
   *
   *  partition的index作为参数,传递给constructA,返回值是A
   * 然后该partition分区的每一个元素,都进行flatMap函数处理,f函数的参数是元素原始内容和A
    *
    * 过期了,现在就是mapPartitionsWithIndex
   */
  @deprecated("use mapPartitionsWithIndex and flatMap", "1.0.0")
  def flatMapWith[A, U: ClassTag]
      (constructA: Int => A, preservesPartitioning: Boolean = false)
      (f: (T, A) => Seq[U]): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    val cleanA = sc.clean(constructA)
    mapPartitionsWithIndex((index, iter) => {
      val a = cleanA(index) ////partition的index作为参数,传递给constructA,返回值是A
      iter.flatMap(t => cleanF(t, a))
    }, preservesPartitioning)
  }

  /**
   * Applies f to each element of this RDD, where f takes an additional parameter of type A.
   * This additional parameter is produced by constructA, which is called in each
   * partition with the index of that partition.
    * 过期了,现在就是mapPartitionsWithIndex
   */
  @deprecated("use mapPartitionsWithIndex and foreach", "1.0.0")
  def foreachWith[A](constructA: Int => A)(f: (T, A) => Unit): Unit = withScope {
    val cleanF = sc.clean(f)
    val cleanA = sc.clean(constructA)
    mapPartitionsWithIndex { (index, iter) =>
      val a = cleanA(index)
      iter.map(t => {cleanF(t, a); t})
    }
  }

  /**
   * Filters this RDD with p, where p takes an additional parameter of type A.  This
   * additional parameter is produced by constructA, which is called in each
   * partition with the index of that partition.
    * 过期了,现在就是mapPartitionsWithIndex
   */
  @deprecated("use mapPartitionsWithIndex and filter", "1.0.0")
  def filterWith[A](constructA: Int => A)(p: (T, A) => Boolean): RDD[T] = withScope {
    val cleanP = sc.clean(p)
    val cleanA = sc.clean(constructA)
    mapPartitionsWithIndex((index, iter) => {
      val a = cleanA(index)
      iter.filter(t => cleanP(t, a))
    }, preservesPartitioning = true)
  }

  /**
   * Zips this RDD with another one, returning key-value pairs with the first element in each RDD,
   * second element in each RDD, etc. Assumes that the two RDDs have the *same number of
   * partitions* and the *same number of elements in each partition* (e.g. one was made through
   * a map on the other).
   *
   * zip函数用于将两个RDD组合成Key/Value形式的RDD,这里默认两个RDD的partition数量以及元素数量都相同，否则会抛出异常。
scala> var rdd1 = sc.makeRDD(1 to 5,2)
rdd1: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[1] at makeRDD at :21

scala> var rdd2 = sc.makeRDD(Seq("A","B","C","D","E"),2)
rdd2: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[2] at makeRDD at :21

scala> rdd1.zip(rdd2).collect
res0: Array[(Int, String)] = Array((1,A), (2,B), (3,C), (4,D), (5,E))
   */
  def zip[U: ClassTag](other: RDD[U]): RDD[(T, U)] = withScope {
    //组合相同的partition中元素,按照元素顺序,两列组合
    zipPartitions(other, preservesPartitioning = false) { (thisIter, otherIter) => //参数分别是两个RDD对应的partition的迭代器
      new Iterator[(T, U)] {
        def hasNext: Boolean = (thisIter.hasNext, otherIter.hasNext) match {
          case (true, true) => true //必须都有下一个元素的时候才是true
          case (false, false) => false
          case _ => throw new SparkException("Can only zip RDDs with " +
            "same number of elements in each partition") //说明同一个partition中元素数量不同
        }
        def next(): (T, U) = (thisIter.next(), otherIter.next())
      }
    }
  }

  /**
   * Zip this RDD's partitions with one (or more) RDD(s) and return a new RDD by
   * applying a function to the zipped partitions. Assumes that all the RDDs have the
   * *same number of partitions*, but does *not* require them to have the same number
   * of elements in each partition.
   */
  def zipPartitions[B: ClassTag, V: ClassTag]
      (rdd2: RDD[B], preservesPartitioning: Boolean)
      (f: (Iterator[T], Iterator[B]) => Iterator[V]): RDD[V] = withScope {//针对两个RDD T和B,分别对应后,输出最终结果V
    new ZippedPartitionsRDD2(sc, sc.clean(f), this, rdd2, preservesPartitioning)
  }

  def zipPartitions[B: ClassTag, V: ClassTag]
      (rdd2: RDD[B])
      (f: (Iterator[T], Iterator[B]) => Iterator[V]): RDD[V] = withScope {
    zipPartitions(rdd2, preservesPartitioning = false)(f)
  }

  def zipPartitions[B: ClassTag, C: ClassTag, V: ClassTag]
      (rdd2: RDD[B], rdd3: RDD[C], preservesPartitioning: Boolean)
      (f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V]): RDD[V] = withScope {
    new ZippedPartitionsRDD3(sc, sc.clean(f), this, rdd2, rdd3, preservesPartitioning)
  }

  def zipPartitions[B: ClassTag, C: ClassTag, V: ClassTag]
      (rdd2: RDD[B], rdd3: RDD[C])
      (f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V]): RDD[V] = withScope {
    zipPartitions(rdd2, rdd3, preservesPartitioning = false)(f)
  }

  def zipPartitions[B: ClassTag, C: ClassTag, D: ClassTag, V: ClassTag]
      (rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D], preservesPartitioning: Boolean)
      (f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V]): RDD[V] = withScope {
    new ZippedPartitionsRDD4(sc, sc.clean(f), this, rdd2, rdd3, rdd4, preservesPartitioning)
  }

  def zipPartitions[B: ClassTag, C: ClassTag, D: ClassTag, V: ClassTag]
      (rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D])
      (f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V]): RDD[V] = withScope {
    zipPartitions(rdd2, rdd3, rdd4, preservesPartitioning = false)(f)
  }


  // Actions (launch a job to return a value to the user program)
  //以下是Action行为的方法,用于真正去分布式处理

  /**
   * Applies a function f to all elements of this RDD.
   * RDD的每一个元素都作为参数,应用一次f函数,该函数是无输出的
   * 每一个partition的一行内容,即T,被调用函数f进行处理,可以在f函数中进行打印信息操作
   */
  def foreach(f: T => Unit): Unit = withScope {
    val cleanF = sc.clean(f)

    /**
     * 让sc执行runjob方法,第一个参数是根需要的RDD
     * 第二个参数是传入一个迭代器,函数会循环迭代器,每一个元素都执行f方法
     */
    sc.runJob(this, (iter: Iterator[T]) => iter.foreach(cleanF))//迭代器每一个partition,每一个元素进行f函数处理
  }

  /**
   * Applies a function f to each partition of this RDD.
   * 将rdd的每一个partition作为参数,给函数f
   * 每一个partition的所有内容,即Iterator[T],被调用函数f进行处理,可以在f函数中进行打印信息操作
   */
  def foreachPartition(f: Iterator[T] => Unit): Unit = withScope {
    val cleanF = sc.clean(f)
    sc.runJob(this, (iter: Iterator[T]) => cleanF(iter))
  }

  /**
   * Return an array that contains all of the elements in this RDD.
   * 返回所有运行的结果集,收集每一个partition的结果,每一个partiton的结果都是数组,然后组装成一个大的数组
   */
  def collect(): Array[T] = withScope {
    val results = sc.runJob(this, (iter: Iterator[T]) => iter.toArray) //返回所有运行的结果集
    Array.concat(results: _*)
  }

  /**
   * Return an iterator that contains all of the elements in this RDD.
   *
   * The iterator will consume as much memory as the largest partition in this RDD.
   *
   * Note: this results in multiple Spark jobs, and if the input RDD is the result
   * of a wide transformation (e.g. join with different partitioners), to avoid
   * recomputing the input RDD should be cached first.
   */
  def toLocalIterator: Iterator[T] = withScope {
    //该job每次只是读取一个partition的数据
    def collectPartition(p: Int): Array[T] = {
      sc.runJob(this, (iter: Iterator[T]) => iter.toArray, Seq(p)).head //因为runJob返回的结果是数组,数组的size是Seq(p)的结果集,此时只有一个partition,因此返回head就是该partition的结果集
    }

    /**
     * (0 until 5)  输出 a: scala.collection.immutable.Range = Range(0, 1, 2, 3, 4)
     *  每一个partition都会调用一次job,一共有多少个partition,就有多少个job,每一个返回值都是一个RDD的分区的迭代器
     */
    (0 until partitions.length).iterator.flatMap(i => collectPartition(i))
  }

  /**
   * Return an array that contains all of the elements in this RDD.
   * 返回所有运行的结果集
   */
  @deprecated("use collect", "1.0.0")
  def toArray(): Array[T] = withScope {
    collect()
  }

  /**
   * Return an RDD that contains all matching values by applying `f`.
   * 包含所有匹配的value
   *
   *
   * f: PartialFunction[T, U] 偏函数,其实就是将T转换成U的函数,只是里面有一个filter功能,即T如果不满足的话,不会报错,只是过滤掉该不符合的元素
   */
  def collect[U: ClassTag](f: PartialFunction[T, U]): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    filter(cleanF.isDefinedAt).map(cleanF)
  }

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   *
   * Uses `this` partitioner/partition size, because even if `other` is huge, the resulting
   * RDD will be &lt;= us.
   * 返回在RDD中出现，并且不在otherRDD中出现的元素，不去重
   */
  def subtract(other: RDD[T]): RDD[T] = withScope {
    subtract(other, partitioner.getOrElse(new HashPartitioner(partitions.length)))
  }

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   */
  def subtract(other: RDD[T], numPartitions: Int): RDD[T] = withScope {
    subtract(other, new HashPartitioner(numPartitions))
  }

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   * 返回在RDD中出现，并且不在otherRDD中出现的元素，不去重
   *
   * 将x变成(x,null) 这样K-V结构的就可以调用的PairRDDFunctions里面<K,V>结构的逻辑
   */
  def subtract(
      other: RDD[T],
      p: Partitioner)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
    if (partitioner == Some(p)) {
      // Our partitioner knows how to handle T (which, since we have a partitioner, is
      // really (K, V)) so make a new Partitioner that will de-tuple our fake tuples
      val p2 = new Partitioner() {
        override def numPartitions: Int = p.numPartitions
        override def getPartition(k: Any): Int = p.getPartition(k.asInstanceOf[(Any, _)]._1)
      }
      // Unfortunately, since we're making a new p2, we'll get ShuffleDependencies
      // anyway, and when calling .keys, will not have a partitioner set, even though
      // the SubtractedRDD will, thanks to p2's de-tupled partitioning, already be
      // partitioned by the right/real keys (e.g. p).
      this.map(x => (x, null)).subtractByKey(other.map((_, null)), p2).keys
    } else {
      this.map(x => (x, null)).subtractByKey(other.map((_, null)), p).keys
    }
  }

  /**
   * Reduces the elements of this RDD using the specified commutative and
   * associative binary operator.
   * 连续两个元素进行f处理,最终转换成一个T元素
   *
   * reduce将RDD中元素两两传递给输入函数，同时产生一个新的值，新产生的值与RDD中下一个元素再被传递给输入函数直到最后只有一个值为止。
举例
scala> val c = sc.parallelize(1 to 10)
scala> c.reduce((x, y) => x + y)
res4: Int = 55

将每一个partition的数据,进行运算,返回一个T对象,然后每一个partition的结果T再参与运算.最终返回一个T
即RDD所有的元素进行f运算,返回一个值
   */
  def reduce(f: (T, T) => T): T = withScope {
    val cleanF = sc.clean(f)

    //如何处理一个partition迭代器,让其经过运算返回一个元素T
    //表示定义reducePartition方法,参数是 Iterator[T]  返回值是T, 实际上函数体是 iter =>...
    val reducePartition: Iterator[T] => Option[T] = iter => {
      if (iter.hasNext) {
        Some(iter.reduceLeft(cleanF)) //从左到右,连续两个元素,进行f处理,最终产生一个T元素
      } else {
        None
      }
    }


    var jobResult: Option[T] = None //最终的结果

    //合并每一个partition的结果集,参数是第几个partition,对应的结果集
    //即如何处理每一个partition的结果集,即参数 第几个partition返回的结果U
    val mergeResult = (index: Int, taskResult: Option[T]) => {
      if (taskResult.isDefined) {
        jobResult = jobResult match {
          case Some(value) => Some(f(value, taskResult.get))//将每一个结果集与总结果集运算
          case None => taskResult
        }
      }
    }
    sc.runJob(this, reducePartition, mergeResult)
    // Get the final result out of our Option, or throw an exception if the RDD was empty
    jobResult.getOrElse(throw new UnsupportedOperationException("empty collection"))
  }

  /**
   * Reduces the elements of this RDD in a multi-level tree pattern.
   *
   * @param depth suggested depth of the tree (default: 2)
   * @see [[org.apache.spark.rdd.RDD#reduce]]
   */
  def treeReduce(f: (T, T) => T, depth: Int = 2): T = withScope {
    require(depth >= 1, s"Depth must be greater than or equal to 1 but got $depth.")
    val cleanF = context.clean(f)
    //处理一个partition的所有元素的迭代器,返回一个对象T
    val reducePartition: Iterator[T] => Option[T] = iter => {
      if (iter.hasNext) {
        Some(iter.reduceLeft(cleanF))
      } else {
        None
      }
    }
    //每一个partition的元素进行f函数处理
    val partiallyReduced = mapPartitions(it => Iterator(reducePartition(it)))

    //op表示两个T参与运算,最终生成一个新的T
    //c和x分别表示两个T
    val op: (Option[T], Option[T]) => Option[T] = (c, x) => {
      if (c.isDefined && x.isDefined) {
        Some(cleanF(c.get, x.get))
      } else if (c.isDefined) {
        c
      } else if (x.isDefined) {
        x
      } else {
        None
      }
    }
    partiallyReduced.treeAggregate(Option.empty[T])(op, op, depth)
      .getOrElse(throw new UnsupportedOperationException("empty collection"))
  }

  /**
   * Aggregate the elements of each partition, and then the results for all the partitions, using a
   * given associative and commutative function and a neutral "zero value". The function
   * op(t1, t2) is allowed to modify t1 and return it as its result value to avoid object
   * allocation; however, it should not modify t2.
   *
   * This behaves somewhat differently from fold operations implemented for non-distributed
   * collections in functional languages like Scala. This fold operation may be applied to
   * partitions individually, and then fold those results into the final result, rather than
   * apply the fold to each element sequentially in some defined ordering. For functions
   * that are not commutative, the result may differ from that of a fold applied to a
   * non-distributed collection.
   *
   * 每一个partition都从zeroValue开始初始化,剩下的与reduce相同
   */
  def fold(zeroValue: T)(op: (T, T) => T): T = withScope {
    // Clone the zero value since we will also be serializing it as part of tasks
    var jobResult = Utils.clone(zeroValue, sc.env.closureSerializer.newInstance())//结果先赋值为zeroValue
    val cleanOp = sc.clean(op)
    val foldPartition = (iter: Iterator[T]) => iter.fold(zeroValue)(cleanOp) //针对一个Iterator进行处理,每一个partition都预先加了zeroValue
    val mergeResult = (index: Int, taskResult: T) => jobResult = op(jobResult, taskResult) //合并每一个partition的结果集,因为jobResult已经是zeroValue了,因此相当于结果集也加了一次zeroValue
    sc.runJob(this, foldPartition, mergeResult)
    jobResult
  }

  /**
   * Aggregate the elements of each partition, and then the results for all the partitions, using
   * given combine functions and a neutral "zero value". This function can return a different result
   * type, U, than the type of this RDD, T. Thus, we need one operation for merging a T into an U
   * and one operation for merging two U's, as in scala.TraversableOnce. Both of these functions are
   * allowed to modify and return their first argument instead of creating a new U to avoid memory
   * allocation.
   * 该函数与fold相同,但是区别在于fold是输入和输出相同的类型,因此需要一个函数即可,而该函数是输出可以与输入不相同
   * seqOp表示将输入T与初始化函数U进行处理,产生新的对象U,然后每一个partition进行如此处理,即每一个partition的输出就是一个U
   * 当合并每一个partition的U的时候,因此进入第二个函数comOp,最终输出一个U
   */
  def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U = withScope {
    // Clone the zero value since we will also be serializing it as part of tasks
    var jobResult = Utils.clone(zeroValue, sc.env.serializer.newInstance())//结果集先赋值为zeroValue
    val cleanSeqOp = sc.clean(seqOp)
    val cleanCombOp = sc.clean(combOp)
    val aggregatePartition = (it: Iterator[T]) => it.aggregate(zeroValue)(cleanSeqOp, cleanCombOp) //针对partition进行处理,每一个partiton都加了一个zeroValue
    val mergeResult = (index: Int, taskResult: U) => jobResult = combOp(jobResult, taskResult)//对每一个partition结果集合并,因为jobResult已经是zeroValue了,因此相当于结果集也加了一次zeroValue
    sc.runJob(this, aggregatePartition, mergeResult)
    jobResult
  }

  /**
   * Aggregates the elements of this RDD in a multi-level tree pattern.
   *
   * @param depth suggested depth of the tree (default: 2)
   * @see [[org.apache.spark.rdd.RDD#aggregate]]
   *
另外一种例外情况是在使用 recude 或者 aggregate action 聚集数据到 driver 时，如果数据把很多 partititon 个数的数据，单进程执行的 driver merge 所有 partition 的输出时很容易成为计算的瓶颈。
为了缓解 driver 的计算压力，可以使用 reduceByKey 或者 aggregateByKey 执行分布式的 aggregate 操作把数据分布到更少的 partition 上。每个 partition 中的数据并行的进行 merge，
再把 merge 的结果发个 driver 以进行最后一轮 aggregation。查看 treeReduce 和 treeAggregate 查看如何这么使用的例子。

   */
  def treeAggregate[U: ClassTag](zeroValue: U)(
      seqOp: (U, T) => U,
      combOp: (U, U) => U,
      depth: Int = 2): U = withScope {
    require(depth >= 1, s"Depth must be greater than or equal to 1 but got $depth.")
    if (partitions.length == 0) {
      Utils.clone(zeroValue, context.env.closureSerializer.newInstance())
    } else {
      val cleanSeqOp = context.clean(seqOp)
      val cleanCombOp = context.clean(combOp)
      val aggregatePartition =
        (it: Iterator[T]) => it.aggregate(zeroValue)(cleanSeqOp, cleanCombOp) //如何对一个partition进行合并处理

      var partiallyAggregated = mapPartitions(it => Iterator(aggregatePartition(it))) //每一个partiton如何合并处理,产生新的RDD

      var numPartitions = partiallyAggregated.partitions.length //多少个partition
      val scale = math.max(math.ceil(math.pow(numPartitions, 1.0 / depth)).toInt, 2) //对numPartitions开depth的根号得到的数据 取整 ,例如 100为numPartitions,6为depth,因此结果是2.15,去整后是3
      // If creating an extra level doesn't help reduce
      // the wall-clock time, we stop tree aggregation.

      // Don't trigger TreeAggregation when it doesn't save wall-clock time
      while (numPartitions > scale + math.ceil(numPartitions.toDouble / scale)) {
        numPartitions /= scale
        val curNumPartitions = numPartitions
        partiallyAggregated = partiallyAggregated.mapPartitionsWithIndex {
          (i, iter) => iter.map((i % curNumPartitions, _)) //将partition内的元素内容转换成(新的partition组,元素内容)组成的元组
        }.reduceByKey(new HashPartitioner(curNumPartitions), cleanCombOp).values //按照元组中新的partitionid重新设置元组
      }
      partiallyAggregated.reduce(cleanCombOp)
    }
  }

  /**
   * Return the number of elements in the RDD.
   * 第二个参数是func: Iterator[T] => U,即给定一个分区的话,如何趋合理这一个分区,并且返回一个具体的对象U,而不是迭代器
   * 而Utils.getIteratorSize方法的参数就是Iterator[T],返回值是Long
   */
  def count(): Long = sc.runJob(this, Utils.getIteratorSize _).sum

  /**
   * :: Experimental ::
   * Approximate version of count() that returns a potentially incomplete result
   * within a timeout, even if not all tasks have finished.
   */
  @Experimental
  def countApprox(
      timeout: Long,
      confidence: Double = 0.95): PartialResult[BoundedDouble] = withScope {
    val countElements: (TaskContext, Iterator[T]) => Long = { (ctx, iter) =>
      var result = 0L
      while (iter.hasNext) {
        result += 1L
        iter.next()
      }
      result
    }
    val evaluator = new CountEvaluator(partitions.length, confidence)
    sc.runApproximateJob(this, countElements, evaluator, timeout)
  }

  /**
   * Return the count of each unique value in this RDD as a local map of (value, count) pairs.
   *
   * Note that this method should only be used if the resulting map is expected to be small, as
   * the whole thing is loaded into the driver's memory.
   * To handle very large results, consider using rdd.map(x =&gt; (x, 1L)).reduceByKey(_ + _), which
   * returns an RDD[T, Long] instead of a map.
   *
   * 获取每一个值出现的次数,即1出现4次..
   * val xx = sc.parallelize(List(1,1,1,1,2,2,3,6,5,9))
println(xx.countByValue())
print Map(5 -> 1, 1 -> 4, 6 -> 1, 9 -> 1, 2 -> 2, 3 -> 1)
   */
  def countByValue()(implicit ord: Ordering[T] = null): Map[T, Long] = withScope {
    map(value => (value, null)).countByKey() //把value设置成 key-value形式
  }

  /**
   * :: Experimental ::
   * Approximate version of countByValue().
   */
  @Experimental
  def countByValueApprox(timeout: Long, confidence: Double = 0.95)
      (implicit ord: Ordering[T] = null)
      : PartialResult[Map[T, BoundedDouble]] = withScope {
    if (elementClassTag.runtimeClass.isArray) {
      throw new SparkException("countByValueApprox() does not support arrays")
    }
    val countPartition: (TaskContext, Iterator[T]) => OpenHashMap[T, Long] = { (ctx, iter) =>
      val map = new OpenHashMap[T, Long]
      iter.foreach {
        t => map.changeValue(t, 1L, _ + 1L)
      }
      map
    }
    val evaluator = new GroupedCountEvaluator[T](partitions.length, confidence)
    sc.runApproximateJob(this, countPartition, evaluator, timeout)
  }

  /**
   * :: Experimental ::
   * Return approximate number of distinct elements in the RDD.
   *
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="http://dx.doi.org/10.1145/2452376.2452456">here</a>.
   *
   * The relative accuracy is approximately `1.054 / sqrt(2^p)`. Setting a nonzero `sp &gt; p`
   * would trigger sparse representation of registers, which may reduce the memory consumption
   * and increase accuracy when the cardinality is small.
   *
   * @param p The precision value for the normal set.
   *          `p` must be a value between 4 and `sp` if `sp` is not zero (32 max).
   * @param sp The precision value for the sparse set, between 0 and 32.
   *           If `sp` equals 0, the sparse representation is skipped.
   */
  @Experimental
  def countApproxDistinct(p: Int, sp: Int): Long = withScope {
    require(p >= 4, s"p ($p) must be >= 4")
    require(sp <= 32, s"sp ($sp) must be <= 32")
    require(sp == 0 || p <= sp, s"p ($p) cannot be greater than sp ($sp)")
    val zeroCounter = new HyperLogLogPlus(p, sp)
    aggregate(zeroCounter)(
      (hll: HyperLogLogPlus, v: T) => {
        hll.offer(v)
        hll
      },
      (h1: HyperLogLogPlus, h2: HyperLogLogPlus) => {
        h1.addAll(h2)
        h1
      }).cardinality()
  }

  /**
   * Return approximate number of distinct elements in the RDD.
   *
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="http://dx.doi.org/10.1145/2452376.2452456">here</a>.
   *
   * @param relativeSD Relative accuracy. Smaller values create counters that require more space.
   *                   It must be greater than 0.000017.
   */
  def countApproxDistinct(relativeSD: Double = 0.05): Long = withScope {
    require(relativeSD > 0.000017, s"accuracy ($relativeSD) must be greater than 0.000017")
    val p = math.ceil(2.0 * math.log(1.054 / relativeSD) / math.log(2)).toInt
    countApproxDistinct(if (p < 4) 4 else p, 0)
  }

  /**
   * Zips this RDD with its element indices. The ordering is first based on the partition index
   * and then the ordering of items within each partition. So the first item in the first
   * partition gets index 0, and the last item in the last partition receives the largest index.
   *
   * This is similar to Scala's zipWithIndex but it uses Long instead of Int as the index type.
   * This method needs to trigger a spark job when this RDD contains more than one partitions.
   *
   * Note that some RDDs, such as those returned by groupBy(), do not guarantee order of
   * elements in a partition. The index assigned to each element is therefore not guaranteed,
   * and may even change if the RDD is reevaluated. If a fixed ordering is required to guarantee
   * the same index assignments, you should sort the RDD with sortByKey() or save it to a file.
   * 为每一个元素产生一个唯一的序号
   * 该方式不太好,因为他要先扫描所有的partition元素,确定每一个partition中有多少个元素,然后设置每一个partition序号的最小值,然后算出来的序号
   * 比如3个partition,第一个partition有3个记录,第二个partition有5条记录,第三个partition有2条记录
   * 因此第一个partition的序号就是0 1 2 第二个partition序号就是3 4 5 6 7 第三个partition序号就是8 9
   */
  def zipWithIndex(): RDD[(T, Long)] = withScope {
    new ZippedWithIndexRDD(this)
  }

  /**
   * Zips this RDD with generated unique Long ids. Items in the kth partition will get ids k, n+k,
   * 2*n+k, ..., where n is the number of partitions. So there may exist gaps, but this method
   * won't trigger a spark job, which is different from [[org.apache.spark.rdd.RDD#zipWithIndex]].
   *
   * Note that some RDDs, such as those returned by groupBy(), do not guarantee order of
   * elements in a partition. The unique ID assigned to each element is therefore not guaranteed,
   * and may even change if the RDD is reevaluated. If a fixed ordering is required to guarantee
   * the same index assignments, you should sort the RDD with sortByKey() or save it to a file.
   * 为每一个元素添加一个唯一的序号,最后组成元组,
   * 唯一的序号算法:
   * 比如有3个partition 因此n=3
   * 第一个partition的前三个元素分别是0 1 2 即i = 0 1 2
   * k是第一个partitition,因此k=0
   * 因此第一个partition的序号是0*3+0  1*3+0  2*3+0 即0 3 6
   *
   * 以此类推,第二个partition产生的序号是0*3+1  1*3+1  2*3+1 即1 4 7
   *
   * 即算法的核心逻辑是,有多少个分区,就让每一个分区的数字空出来,让其他分区添进来,这样速度上比上一个方法要快很多
   */
  def zipWithUniqueId(): RDD[(T, Long)] = withScope {
    val n = this.partitions.length.toLong
    this.mapPartitionsWithIndex { case (k, iter) =>
      iter.zipWithIndex.map { case (item, i) => //迭代器的zipWithIndex为每一个元素分配一个唯一的ID
        (item, i * n + k) //i*n表示该迭代器的序号*总分区数量,即在所有分区中应该排列第几,比如第2个元素,总分区为10,那么第2个元素就应该从20开始计数,那么他准确的值是多少呢,即+分区ID
      }
    }
  }

  /**
   * Take the first num elements of the RDD. It works by first scanning one partition, and use the
   * results from that partition to estimate the number of additional partitions needed to satisfy
   * the limit.
   *
   * @note due to complications in the internal implementation, this method will raise
   * an exception if called on an RDD of `Nothing` or `Null`.
   * 获取前num个元素,这些元素是从第0个partition开始获取,一直获取到num数量为止,获取的数据跟排序没关系
   *
   * 从分区中获取多个数据,因此返回的是数组,此时数组要是返回很大的话确实是有问题的
   */
  def take(num: Int): Array[T] = withScope {
    if (num == 0) {
      new Array[T](0)
    } else {
      val buf = new ArrayBuffer[T]//拿回来的元素存储在这里
      val totalParts = this.partitions.length //总partition数量
      var partsScanned = 0//当前在第一个partition上扫描数据呢
      while (buf.size < num && partsScanned < totalParts) {//只要数据没有取到位,就继续去取,并且只要扫描完一个partitioon后,还有partition,就继续扫描
        // The number of partitions to try in this iteration. It is ok for this number to be 尝试去循环多少个partition
        // greater than totalParts because we actually cap it at totalParts in runJob.
        var numPartsToTry = 1//本次尝试接下来扫描多少个partition
        if (partsScanned > 0) {//如果已经扫描了一个partition了,则可能接下来一次要扫描N个partition了,主要是为了提升效率
          // If we didn't find any rows after the previous iteration, quadruple and retry.
          // Otherwise, interpolate the number of partitions we need to try, but overestimate
          // it by 50%. We also cap the estimation in the end.
          if (buf.size == 0) {
            numPartsToTry = partsScanned * 4 //如果还没有数据呢,则扫描多些partition,因为可能数据太少了,很多partition文件内是没有元素的,所以要多扫描一些partition
          } else {
            // the left side of max is >=1 whenever partsScanned >= 2
            numPartsToTry = Math.max((1.5 * num * partsScanned / buf.size).toInt - partsScanned, 1)//最少扫描一个partition
            numPartsToTry = Math.min(numPartsToTry, partsScanned * 4)//控制上限,最多也就是partsScanned * 4
          }
        }

        val left = num - buf.size//还要拿去多少个元素

        /**
 3 until 8
c: scala.collection.immutable.Range = Range(3, 4, 5, 6, 7)
         */
        val p = partsScanned until math.min(partsScanned + numPartsToTry, totalParts)//从当前partition开始 获取N个接下来的partition
        val res = sc.runJob(this, (it: Iterator[T]) => it.take(left).toArray, p)//从p的一组partition中以此获取数据,每一个partition最多获取left个数据
        //sc.runJob返回值是Array[U],但是每一个U又是一个数组,因为it.take(left).toArray
        res.foreach(buf ++= _.take(num - buf.size))//不断的把每一个partition获取的元素拿去出来,直到拿完为止,就还下一个partition
        partsScanned += numPartsToTry
      }

      buf.toArray
    }
  }

  /**
   * Return the first element in this RDD.
   * 获取第一个元素,该第一个元素不是排序后的第一个元素,而就是随意产生的一个元素
   */
  def first(): T = withScope {
    take(1) match {
      case Array(t) => t //因为就take了1条数据,因此Array里面就一个元素,即t就是最终返回的值
      case _ => throw new UnsupportedOperationException("empty collection")
    }
  }

  /**
   * Returns the top k (largest) elements from this RDD as defined by the specified
   * implicit Ordering[T]. This does the opposite of [[takeOrdered]]. For example:
   * {{{
   *   sc.parallelize(Seq(10, 4, 2, 12, 3)).top(1)
   *   // returns Array(12)
   *
   *   sc.parallelize(Seq(2, 3, 4, 5, 6)).top(2)
   *   // returns Array(6, 5)
   * }}}
   *
   * @param num k, the number of top elements to return
   * @param ord the implicit ordering for T
   * @return an array of top elements
   * takeOrdered的倒序
   */
  def top(num: Int)(implicit ord: Ordering[T]): Array[T] = withScope {
    takeOrdered(num)(ord.reverse)
  }

  /**
   * Returns the first k (smallest) elements from this RDD as defined by the specified
   * implicit Ordering[T] and maintains the ordering. This does the opposite of [[top]].
   * For example:
   * {{{
   *   sc.parallelize(Seq(10, 4, 2, 12, 3)).takeOrdered(1)
   *   // returns Array(2)
   *
   *   sc.parallelize(Seq(2, 3, 4, 5, 6)).takeOrdered(2)
   *   // returns Array(2, 3)
   * }}}
   *
   * @param num k, the number of elements to return
   * @param ord the implicit ordering for T
   * @return an array of top elements
   *
   * 1.每一个partition都有一个队列,都从队列中获取num个元素
   * 2.合并每一个队列,但是最终合并后的队列也是num个元素
   *
   * 将RDD的元素进行排序,然后返回num个元素
   */
  def takeOrdered(num: Int)(implicit ord: Ordering[T]): Array[T] = withScope {
    if (num == 0) {
      Array.empty
    } else {

      //每一个partition进行迭代
      val mapRDDs = mapPartitions { items =>
        // Priority keeps the largest elements, so let's reverse the ordering.
        val queue = new BoundedPriorityQueue[T](num)(ord.reverse) //按照ord的倒叙创建一个队列,该队列最多存放num个元素
        queue ++= util.collection.Utils.takeOrdered(items, num)(ord) //循环迭代器,将每一个元素存放到优先队列中
        Iterator.single(queue)
      }
      if (mapRDDs.partitions.length == 0) {//没有partition,因此结果是空
        Array.empty
      } else {//合并队列,每一个队列的所有元素进行合并,组成大队列,由于每一个队列都是有上限的,因此最终合并后的队列也是有上限的,就是num个元素,然后排序
        mapRDDs.reduce { (queue1, queue2) =>
          queue1 ++= queue2
          queue1 //返回的queue1依然是一个优先队列.因此两个队列merge后的数量不会累加,排序低的就会被抛弃掉了
        }.toArray.sorted(ord) //对优先队列的数据进行排序
      }
    }
  }

  /**
   * Returns the max of this RDD as defined by the implicit Ordering[T].
   * @return the maximum element of the RDD
   * */
  def max()(implicit ord: Ordering[T]): T = withScope {
    this.reduce(ord.max)
  }

  /**
   * Returns the min of this RDD as defined by the implicit Ordering[T].
   * @return the minimum element of the RDD
   * */
  def min()(implicit ord: Ordering[T]): T = withScope {
    this.reduce(ord.min)
  }

  /**
   * @note due to complications in the internal implementation, this method will raise an
   * exception if called on an RDD of `Nothing` or `Null`. This may be come up in practice
   * because, for example, the type of `parallelize(Seq())` is `RDD[Nothing]`.
   * (`parallelize(Seq())` should be avoided anyway in favor of `parallelize(Seq[T]())`.)
   * @return true if and only if the RDD contains no elements at all. Note that an RDD
   *         may be empty even when it has at least 1 partition.
   */
  def isEmpty(): Boolean = withScope {
    partitions.length == 0 || take(1).length == 0
  }

  /**
   * Save this RDD as a text file, using string representations of elements.
   * 保存到path路径下,key的类型是NullWritable,value的类型是Text
   * 将每一个元素存储到text中
   */
  def saveAsTextFile(path: String): Unit = withScope {
    // https://issues.apache.org/jira/browse/SPARK-2075
    //
    // NullWritable is a `Comparable` in Hadoop 1.+, so the compiler cannot find an implicit
    // Ordering for it and will use the default `null`. However, it's a `Comparable[NullWritable]`
    // in Hadoop 2.+, so the compiler will call the implicit `Ordering.ordered` method to create an
    // Ordering for `NullWritable`. That's why the compiler will generate different anonymous
    // classes for `saveAsTextFile` in Hadoop 1.+ and Hadoop 2.+.
    //
    // Therefore, here we provide an explicit Ordering `null` to make sure the compiler generate
    // same bytecodes for `saveAsTextFile`.
    val nullWritableClassTag = implicitly[ClassTag[NullWritable]]//key的类型
    val textClassTag = implicitly[ClassTag[Text]]//value的类型

    //循环每一个partition的元素
    val r = this.mapPartitions { iter =>
      val text = new Text()
      //将每一个元素赋值为x,将x的内容存储到text中,然后存储到hadoop中
      iter.map { x =>
        text.set(x.toString)
        (NullWritable.get(), text)
      }
    }

    RDD.rddToPairRDDFunctions(r)(nullWritableClassTag, textClassTag, null)
      .saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](path)//保存到path路径下,key的类型是NullWritable,value的类型是Text
  }

  /**
   * Save this RDD as a compressed text file, using string representations of elements.
   * 保存到path路径下,key的类型是NullWritable,value的类型是Text
   * 将每一个元素存储到text中,保存过程中可以编码
   */
  def saveAsTextFile(path: String, codec: Class[_ <: CompressionCodec]): Unit = withScope {
    // https://issues.apache.org/jira/browse/SPARK-2075
    val nullWritableClassTag = implicitly[ClassTag[NullWritable]]//key的类型
    val textClassTag = implicitly[ClassTag[Text]]//value的类型

    //循环每一个partition的元素
    val r = this.mapPartitions { iter =>
      val text = new Text()
      //将每一个元素赋值为x,将x的内容存储到text中,然后存储到hadoop中
      iter.map { x =>
        text.set(x.toString)
        (NullWritable.get(), text)
      }
    }

    RDD.rddToPairRDDFunctions(r)(nullWritableClassTag, textClassTag, null)
      .saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](path, codec) //保存到path路径下,key的类型是NullWritable,value的类型是Text
  }

  /**
   * Save this RDD as a SequenceFile of serialized objects.
   * 保存成序列化文件
   */
  def saveAsObjectFile(path: String): Unit = withScope {
    this.mapPartitions(iter => iter.grouped(10).map(_.toArray))
      .map(x => (NullWritable.get(), new BytesWritable(Utils.serialize(x))))
      .saveAsSequenceFile(path)
  }

  /**
   * Creates tuples of the elements in this RDD by applying `f`.
   * 为各个元素，按指定的函数生成key，形成key-value的RDD。
  元素T通过函数转换成k,因此结果就是RDD[K,V]

scala> val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
a: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[123] at parallelize at <console>:21

scala> val b = a.keyBy(_.length)
b: org.apache.spark.rdd.RDD[(Int, String)] = MapPartitionsRDD[124] at keyBy at <console>:23

scala> b.collect
res80: Array[(Int, String)] = Array((3,dog), (6,salmon), (6,salmon), (3,rat), (8,elephant))
   */
  def keyBy[K](f: T => K): RDD[(K, T)] = withScope {
    val cleanedF = sc.clean(f)
    map(x => (cleanedF(x), x))//该方法没有经过shuffler操作,不耗费性能,只是转换成<K,V>结果,K是v通过f函数转变的
  }

  /** A private method for tests, to look at the contents of each partition
    * 返回每一个partition的迭代器集合,即如果RDD有5个partition,则返回数组是5个,其中每一个元素又是一个迭代器,因此是数组套数组
    **/
  private[spark] def collectPartitions(): Array[Array[T]] = withScope {
    sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
  }

  /**
   * Mark this RDD for checkpointing. It will be saved to a file inside the checkpoint
   * directory set with `SparkContext#setCheckpointDir` and all references to its parent
   * RDDs will be removed. This function must be called before any job has been
   * executed on this RDD. It is strongly recommended that this RDD is persisted in
   * memory, otherwise saving it on a file will require recomputation.
   */
  def checkpoint(): Unit = RDDCheckpointData.synchronized {
    // NOTE: we use a global lock here due to complexities downstream with ensuring
    // children RDD partitions point to the correct parent partitions. In the future
    // we should revisit this consideration.
    if (context.checkpointDir.isEmpty) {
      throw new SparkException("Checkpoint directory has not been set in the SparkContext")
    } else if (checkpointData.isEmpty) {//说明此时没有进行checkPoint
      checkpointData = Some(new ReliableRDDCheckpointData(this))//因此对该rdd进行checkPoint备份到磁盘上,该备份不会随着系统删除而备份清理
    }
  }

  /**
   * Mark this RDD for local checkpointing using Spark's existing caching layer.
   *
   * This method is for users who wish to truncate RDD lineages while skipping the expensive
   * step of replicating the materialized data in a reliable distributed file system. This is
   * useful for RDDs with long lineages that need to be truncated periodically (e.g. GraphX).
   *
   * Local checkpointing sacrifices fault-tolerance for performance. In particular, checkpointed
   * data is written to ephemeral local storage in the executors instead of to a reliable,
   * fault-tolerant storage. The effect is that if an executor fails during the computation,
   * the checkpointed data may no longer be accessible, causing an irrecoverable job failure.
   *
   * This is NOT safe to use with dynamic allocation, which removes executors along
   * with their cached blocks. If you must use both features, you are advised to set
   * `spark.dynamicAllocation.cachedExecutorIdleTimeout` to a high value.
   *
   * The checkpoint directory set through `SparkContext#setCheckpointDir` is not used.
   */
  def localCheckpoint(): this.type = RDDCheckpointData.synchronized {
    if (conf.getBoolean("spark.dynamicAllocation.enabled", false) &&
        conf.contains("spark.dynamicAllocation.cachedExecutorIdleTimeout")) {
      logWarning("Local checkpointing is NOT safe to use with dynamic allocation, " +
        "which removes executors along with their cached blocks. If you must use both " +
        "features, you are advised to set `spark.dynamicAllocation.cachedExecutorIdleTimeout` " +
        "to a high value. E.g. If you plan to use the RDD for 1 hour, set the timeout to " +
        "at least 1 hour.")
    }

    // Note: At this point we do not actually know whether the user will call persist() on
    // this RDD later, so we must explicitly call it here ourselves to ensure the cached
    // blocks are registered for cleanup later in the SparkContext.
    //
    // If, however, the user has already called persist() on this RDD, then we must adapt
    // the storage level he/she specified to one that is appropriate for local checkpointing
    // (i.e. uses disk) to guarantee correctness.

    if (storageLevel == StorageLevel.NONE) {
      persist(LocalRDDCheckpointData.DEFAULT_STORAGE_LEVEL)
    } else {
      persist(LocalRDDCheckpointData.transformStorageLevel(storageLevel), allowOverride = true)
    }

    // If this RDD is already checkpointed and materialized, its lineage is already truncated.
    // We must not override our `checkpointData` in this case because it is needed to recover
    // the checkpointed data. If it is overridden, next time materializing on this RDD will
    // cause error.
    if (isCheckpointedAndMaterialized) {
      logWarning("Not marking RDD for local checkpoint because it was already " +
        "checkpointed and materialized")
    } else {
      // Lineage is not truncated yet, so just override any existing checkpoint data with ours
      checkpointData match {
        case Some(_: ReliableRDDCheckpointData[_]) => logWarning(
          "RDD was already marked for reliable checkpointing: overriding with local checkpoint.") //说明RDD已经是在HDFS上存在checkpoint了
        case _ =>
      }
      checkpointData = Some(new LocalRDDCheckpointData(this))
    }
    this
  }

  /**
   * Return whether this RDD is checkpointed and materialized, either reliably or locally.
   * true表示已经完成了checkpoint操作了
   */
  def isCheckpointed: Boolean = checkpointData.exists(_.isCheckpointed)

  /**
   * Return whether this RDD is checkpointed and materialized, either reliably or locally.
   * This is introduced as an alias for `isCheckpointed` to clarify the semantics of the
   * return value. Exposed for testing.
   * true表示已经完成了checkpoint操作了
   */
  private[spark] def isCheckpointedAndMaterialized: Boolean = isCheckpointed

  /**
   * Return whether this RDD is marked for local checkpointing.
   * Exposed for testing.仅仅为了测试使用LocalRDDCheckpointData
   */
  private[rdd] def isLocallyCheckpointed: Boolean = {
    checkpointData match {
      case Some(_: LocalRDDCheckpointData[T]) => true
      case _ => false
    }
  }

  /**
   * Gets the name of the directory to which this RDD was checkpointed.
   * This is not defined if the RDD is checkpointed locally.
   * 获取已经完成checkpoint的目录
   */
  def getCheckpointFile: Option[String] = {
    checkpointData match {
      case Some(reliable: ReliableRDDCheckpointData[T]) => reliable.getCheckpointDir
      case _ => None
    }
  }

  // =======================================================================
  // Other internal methods and fields
  // =======================================================================

  private var storageLevel: StorageLevel = StorageLevel.NONE//NONE说明该RDD没有被存储

  /** User code that created this RDD (e.g. `textFile`, `parallelize`). */
  @transient private[spark] val creationSite = sc.getCallSite()

  /**
   * The scope associated with the operation that created this RDD.
   *
   * This is more flexible than the call site and can be defined hierarchically. For more
   * detail, see the documentation of {{RDDOperationScope}}. This scope is not defined if the
   * user instantiates this RDD himself without using any Spark operations.
   */
  @transient private[spark] val scope: Option[RDDOperationScope] = {
    Option(sc.getLocalProperty(SparkContext.RDD_SCOPE_KEY)).map(RDDOperationScope.fromJson)
  }

  private[spark] def getCreationSite: String = Option(creationSite).map(_.shortForm).getOrElse("")

  private[spark] def elementClassTag: ClassTag[T] = classTag[T] //RDD对应的泛型对应的类

  private[spark] var checkpointData: Option[RDDCheckpointData[T]] = None //对RDD的最终结果在hdfs上有一个备份,这样如果该hdfs上存在内容,则说明已经不用再跑rdd前面的逻辑了,直接加载数据即可

  /** Returns the first parent RDD 
   * 返回依赖的父类中第一个RDD对象  
   **/
  protected[spark] def firstParent[U: ClassTag]: RDD[U] = {
    dependencies.head.rdd.asInstanceOf[RDD[U]]
  }

  /** Returns the jth parent RDD: e.g. rdd.parent[T](0) is equivalent to rdd.firstParent[T] 
   *  返回依赖的父RDD中第j个RDD对象
   **/
  protected[spark] def parent[U: ClassTag](j: Int) = {
    dependencies(j).rdd.asInstanceOf[RDD[U]]
  }

  /** The [[org.apache.spark.SparkContext]] that this RDD was created on. */
  def context: SparkContext = sc

  /**
   * Private API for changing an RDD's ClassTag.
   * Used for internal Java-Scala API compatibility.
   */
  private[spark] def retag(cls: Class[T]): RDD[T] = {
    val classTag: ClassTag[T] = ClassTag.apply(cls)
    this.retag(classTag)
  }

  /**
   * Private API for changing an RDD's ClassTag.
   * Used for internal Java-Scala API compatibility.
   */
  private[spark] def retag(implicit classTag: ClassTag[T]): RDD[T] = {
    this.mapPartitions(identity, preservesPartitioning = true)(classTag)
  }

  // Avoid handling doCheckpoint multiple times to prevent excessive recursion
  @transient private var doCheckpointCalled = false

  /**
   * Performs the checkpointing of this RDD by saving this. It is called after a job using this RDD
   * has completed (therefore the RDD has been materialized and potentially stored in memory).
   * doCheckpoint() is called recursively on the parent RDDs.
   * 真正去做checkPoint操作
   */
  private[spark] def doCheckpoint(): Unit = {
    RDDOperationScope.withScope(sc, "checkpoint", allowNesting = false, ignoreParent = true) {
      if (!doCheckpointCalled) {
        doCheckpointCalled = true
        if (checkpointData.isDefined) {
          checkpointData.get.checkpoint()
        } else {
          dependencies.foreach(_.rdd.doCheckpoint())
        }
      }
    }
  }

  /**
   * Changes the dependencies of this RDD from its original parents to a new RDD (`newRDD`)
   * created from the checkpoint file, and forget its old dependencies and partitions.
   * 当完成checkpoint之后,调用该方法,清理父RDD依赖的信息,因为这些信息已经不重要了
   */
  private[spark] def markCheckpointed(): Unit = {
    clearDependencies() //清理父依赖的RDD关系,因为已经有checkpoint了
    partitions_ = null //因为该RDD已经在HDFS上存储结果集了,因此将父RDD的关联都删除.已经不再需要父RDD的存在了
    deps = null    // Forget the constructor argument for dependencies too
  }

  /**
   * Clears the dependencies of this RDD. This method must ensure that all references
   * to the original parent RDDs is removed to enable the parent RDDs to be garbage
   * collected. Subclasses of RDD may override this method for implementing their own cleaning
   * logic. See [[org.apache.spark.rdd.UnionRDD]] for an example.
   * 清理父依赖的RDD关系,因为已经有checkpoint了
   */
  protected def clearDependencies() {
    dependencies_ = null
  }

  /** A description of this RDD and its recursive dependencies for debugging.
    * 递归描述一个RDD的依赖内容
    **/
  def toDebugString: String = {
    // Get a debug description of an rdd without its children
    def debugSelf(rdd: RDD[_]): Seq[String] = {
      import Utils.bytesToString

      val persistence = if (storageLevel != StorageLevel.NONE) storageLevel.description else ""
      val storageInfo = rdd.context.getRDDStorageInfo.filter(_.id == rdd.id).map(info =>
        "    CachedPartitions: %d; MemorySize: %s; ExternalBlockStoreSize: %s; DiskSize: %s".format(
          info.numCachedPartitions, bytesToString(info.memSize),
          bytesToString(info.externalBlockStoreSize), bytesToString(info.diskSize)))

      s"$rdd [$persistence]" +: storageInfo
    }

    // Apply a different rule to the last child
    def debugChildren(rdd: RDD[_], prefix: String): Seq[String] = {
      val len = rdd.dependencies.length
      len match {
        case 0 => Seq.empty
        case 1 =>
          val d = rdd.dependencies.head
          debugString(d.rdd, prefix, d.isInstanceOf[ShuffleDependency[_, _, _]], true)
        case _ =>
          val frontDeps = rdd.dependencies.take(len - 1)
          val frontDepStrings = frontDeps.flatMap(
            d => debugString(d.rdd, prefix, d.isInstanceOf[ShuffleDependency[_, _, _]]))

          val lastDep = rdd.dependencies.last
          val lastDepStrings =
            debugString(lastDep.rdd, prefix, lastDep.isInstanceOf[ShuffleDependency[_, _, _]], true)

          (frontDepStrings ++ lastDepStrings)
      }
    }
    // The first RDD in the dependency stack has no parents, so no need for a +-
    def firstDebugString(rdd: RDD[_]): Seq[String] = {
      val partitionStr = "(" + rdd.partitions.length + ")"
      val leftOffset = (partitionStr.length - 1) / 2
      val nextPrefix = (" " * leftOffset) + "|" + (" " * (partitionStr.length - leftOffset))

      debugSelf(rdd).zipWithIndex.map{
        case (desc: String, 0) => s"$partitionStr $desc"
        case (desc: String, _) => s"$nextPrefix $desc"
      } ++ debugChildren(rdd, nextPrefix)
    }
    def shuffleDebugString(rdd: RDD[_], prefix: String = "", isLastChild: Boolean): Seq[String] = {
      val partitionStr = "(" + rdd.partitions.length + ")"
      val leftOffset = (partitionStr.length - 1) / 2
      val thisPrefix = prefix.replaceAll("\\|\\s+$", "")
      val nextPrefix = (
        thisPrefix
        + (if (isLastChild) "  " else "| ")
        + (" " * leftOffset) + "|" + (" " * (partitionStr.length - leftOffset)))

      debugSelf(rdd).zipWithIndex.map{
        case (desc: String, 0) => s"$thisPrefix+-$partitionStr $desc"
        case (desc: String, _) => s"$nextPrefix$desc"
      } ++ debugChildren(rdd, nextPrefix)
    }
    def debugString(
        rdd: RDD[_],
        prefix: String = "",
        isShuffle: Boolean = true,
        isLastChild: Boolean = false): Seq[String] = {
      if (isShuffle) {
        shuffleDebugString(rdd, prefix, isLastChild)
      } else {
        debugSelf(rdd).map(prefix + _) ++ debugChildren(rdd, prefix)
      }
    }
    firstDebugString(this).mkString("\n")
  }

  override def toString: String = "%s%s[%d] at %s".format(
    Option(name).map(_ + " ").getOrElse(""), getClass.getSimpleName, id, getCreationSite)

  def toJavaRDD() : JavaRDD[T] = {
    new JavaRDD(this)(elementClassTag)
  }
}


/**
 * Defines implicit functions that provide extra functionalities on RDDs of specific types.
 *
 * For example, [[RDD.rddToPairRDDFunctions]] converts an RDD into a [[PairRDDFunctions]] for
 * key-value-pair RDDs, and enabling extra functionalities such as [[PairRDDFunctions.reduceByKey]].
 */
object RDD {

  /**
  隐式转换
   */
  // The following implicit functions were in SparkContext before 1.3 and users had to
  // `import SparkContext._` to enable them. Now we move them here to make the compiler find
  // them automatically. However, we still keep the old functions in SparkContext for backward
  // compatibility and forward to the following functions directly.
  //1.可以将RDD[(K, V)]转换成PairRDDFunctions(rdd),即对KEY-value的RDD增加一些新的功能函数,该函数由PairRDDFunctions类提供
  implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
    new PairRDDFunctions(rdd)
  }

  //2.可以将RDD[(K, V)]转换成OrderedRDDFunctions(rdd),条件是Key是支持排序的Key,即对KEY-value的RDD增加一些新的功能函数,该函数由OrderedRDDFunctions类提供
  implicit def rddToOrderedRDDFunctions[K : Ordering : ClassTag, V: ClassTag](rdd: RDD[(K, V)])
  : OrderedRDDFunctions[K, V, (K, V)] = {
    new OrderedRDDFunctions[K, V, (K, V)](rdd)
  }

  //3.可以将RDD[(K, V)]转换成SequenceFileRDDFunctions(rdd),条件是Key和value都是支持hadoop的Writable接口的,即对KEY-value的RDD增加一些新的功能函数,该函数由SequenceFileRDDFunctions类提供
  implicit def rddToSequenceFileRDDFunctions[K, V](rdd: RDD[(K, V)])
      (implicit kt: ClassTag[K], vt: ClassTag[V],
                keyWritableFactory: WritableFactory[K],
                valueWritableFactory: WritableFactory[V])
    : SequenceFileRDDFunctions[K, V] = {
    implicit val keyConverter = keyWritableFactory.convert
    implicit val valueConverter = valueWritableFactory.convert
    new SequenceFileRDDFunctions(rdd,
      keyWritableFactory.writableClass(kt), valueWritableFactory.writableClass(vt))
  }

  //4.可以将double的RDD,RDD[Double]转换成DoubleRDDFunctions,为double运算提供了新的函数
  implicit def doubleRDDToDoubleRDDFunctions(rdd: RDD[Double]): DoubleRDDFunctions = {
    new DoubleRDDFunctions(rdd)
  }

  //5.可以将RDD[T]元素是Numeric数字类型的都转换成DoubleRDDFunctions,为数字运算提供了新的函数,实现是将每一个数字map成double类型
  implicit def numericRDDToDoubleRDDFunctions[T](rdd: RDD[T])(implicit num: Numeric[T])
    : DoubleRDDFunctions = {
    new DoubleRDDFunctions(rdd.map(x => num.toDouble(x)))
  }


  //6.可以为每一个RDD都转换成AsyncRDDActions,为RDD本身提供了异步的功能函数
  implicit def rddToAsyncRDDActions[T: ClassTag](rdd: RDD[T]): AsyncRDDActions[T] = {
    new AsyncRDDActions(rdd)
  }
}
