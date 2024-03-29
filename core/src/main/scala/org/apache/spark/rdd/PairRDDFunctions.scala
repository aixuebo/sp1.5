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

import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util.{Date, HashMap => JHashMap}

import scala.collection.{Map, mutable}
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.DynamicVariable

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf, OutputFormat}
import org.apache.hadoop.mapreduce.{Job => NewAPIHadoopJob, OutputFormat => NewOutputFormat,
  RecordWriter => NewRecordWriter}

import org.apache.spark._
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.annotation.Experimental
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.{DataWriteMethod, OutputMetrics}
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.partial.{BoundedDouble, PartialResult}
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.{SerializableConfiguration, Utils}
import org.apache.spark.util.collection.CompactBuffer
import org.apache.spark.util.random.StratifiedSamplingUtils

/**
 * Extra functions available on RDDs of (key, value) pairs through an implicit conversion.
 * 
 * 
 * 1.reduceByKey 其实真正执行的逻辑就是combineByKey,只是特别的是reduce进行merge合并操作的时候,产生的结果依然是K-V中的V
 * 2.combineByKey 是原始输入K-V,返回值是K-U,会变换返回值---该方法仅是对同一分区内的数据进行合并
 * 3.lookup(key: K): Seq[V] 返回该key对应的所有value值的集合,返回值是ArrayBuffer[V]
 * 4.mapValues[U](f: V => U): RDD[(K, U)],将RDD[K-V]转换成RDD[k,f[V=>U]] = RDD[k,U]操作,相当于对value进行Map操作
 * 5.saveAsHadoopFile 将RDD的内容保存在HDFS上
 * 6.collectAsMap(): Map[K, V] 将RDD处理的所有结果,调用collect方法,返回的是K-V键值对的信息,将其存储在Map中返回
 * 7. flatMapValues[U](f: V => TraversableOnce[U]): RDD[(K, U)],针对RDD[(k,v)] 将其转换成RDD[(K, U)]
 * 过程:
 * a.针对每一个v调用函数f,将其一个v转化成迭代器U,即一个v可以转换成一组数据
 * b.针对迭代器U,调用map方法,转换成k-U
 * 总结,一个v转换成一组U,从而等式为一个key-value,转换成一组K-U的过程
 * 8.join[W](other: RDD[(K, W)]): RDD[(K, (V, W))] 
 * 将两个RDD,组装成新的RDD.返回一个新的RDD,仅将两个RDD都包含的key进行组装成新的RDD
 * 返回值RDD[(K, (V, W))],表示是相同的key做为key,value是元组,表示第一个RDD的value和第二个RDD的value组成的元组
 * 
 * 
 */
class PairRDDFunctions[K, V](self: RDD[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
  extends Logging
  with SparkHadoopMapReduceUtil
  with Serializable
{
  /**
   * Generic function to combine the elements for each key using a custom set of aggregation
   * functions. Turns an RDD[(K, V)] into a result of type RDD[(K, C)], for a "combined type" C
   * Note that V and C can be different -- for example, one might group an RDD of type
   * (Int, Int) into an RDD of type (Int, Seq[Int]). Users provide three functions:
   *
   * - `createCombiner`, which turns a V into a C (e.g., creates a one-element list)
   * - `mergeValue`, to merge a V into a C (e.g., adds it to the end of a list)
   * - `mergeCombiners`, to combine two C's into a single one.
   *
   * In addition, users can control the partitioning of the output RDD, and whether to perform
   * map-side aggregation (if a mapper can produce multiple items with the same key).
   *
   * 对key-value的数据进行聚合
   * 规则是
   * 1.循环每一个key-value,对value进行处理,转换成C对象,将key和c存储到map中
   *   过程:先从map中查找key,获取对应的C,如果C没有,则说明第一次遇见该key,则将value转换成C,即createCombiner函数
   *        如果从map中查获到key对应的c,则调用mergeValue函数,让C和新的V进行运算,生成C
   * 2.合并过程,让每一个key-c进行合并,最终生成key-C对象
   *
   * 即对key相同的数据进行合并,产生一个新的对象C,最终返回key-c,每一个partition仅有一个key-c对象
   *
   * 该方法的意义就仅是对同一个分区内的数据进行merge,即相同的key进行合并操作
   */
  def combineByKey[C](createCombiner: V => C,//可以value转换成C的函数
      mergeValue: (C, V) => C,//对每一个C与V交互生成C的函数
      mergeCombiners: (C, C) => C,//将多个C进行合并的函数
      partitioner: Partitioner,
      mapSideCombine: Boolean = true,//是否map端合并操作
      serializer: Serializer = null): RDD[(K, C)] = self.withScope {
    require(mergeCombiners != null, "mergeCombiners must be defined") // required as of Spark 0.9.0
    if (keyClass.isArray) {//校验key是数组的时候,该如何处理
      if (mapSideCombine) {
        throw new SparkException("Cannot use map-side combining with array keys.")
      }
      if (partitioner.isInstanceOf[HashPartitioner]) {
        throw new SparkException("Default partitioner cannot partition array keys.")
      }
    }

    //创建聚合类
    val aggregator = new Aggregator[K, V, C](
      self.context.clean(createCombiner),
      self.context.clean(mergeValue),
      self.context.clean(mergeCombiners))


    if (self.partitioner == Some(partitioner)) {//该阶段不需要shuffle处理
      self.mapPartitions(iter => {
        val context = TaskContext.get()
        //注意aggregator.combineValuesByKey方法会立刻参与计算,他不是懒加载的
        new InterruptibleIterator(context, aggregator.combineValuesByKey(iter, context))//对每一个partition进行合并,生成key-c对象
      }, preservesPartitioning = true)
    } else {
      new ShuffledRDD[K, V, C](self, partitioner)
        .setSerializer(serializer)
        .setAggregator(aggregator)
        .setMapSideCombine(mapSideCombine)
    }
  }

  /**
   * Simplified version of combineByKey that hash-partitions the output RDD.
   */
  def combineByKey[C](createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      numPartitions: Int): RDD[(K, C)] = self.withScope {
    combineByKey(createCombiner, mergeValue, mergeCombiners, new HashPartitioner(numPartitions))
  }

  /**
   * Aggregate the values of each key, using given combine functions and a neutral "zero value".
   * This function can return a different result type, U, than the type of the values in this RDD,
   * V. Thus, we need one operation for merging a V into a U and one operation for merging two U's,
   * as in scala.TraversableOnce. The former operation is used for merging values within a
   * partition, and the latter is used for merging values between partitions. To avoid memory
   * allocation, both of these functions are allowed to modify and return their first argument
   * instead of creating a new U.
   * 与combineByKey方法类似,只是第一次key出现的时候,value不需要转换成C,而是直接调用seqOp即可,其中seqOp的U就是初始化值zeroValue
   */
  def aggregateByKey[U: ClassTag](zeroValue: U, partitioner: Partitioner)(seqOp: (U, V) => U,
      combOp: (U, U) => U): RDD[(K, U)] = self.withScope {
    // Serialize the zero value to a byte array so that we can get a new clone of it on each key
    val zeroBuffer = SparkEnv.get.serializer.newInstance().serialize(zeroValue)//初始化赋值
    val zeroArray = new Array[Byte](zeroBuffer.limit)
    zeroBuffer.get(zeroArray)

    lazy val cachedSerializer = SparkEnv.get.serializer.newInstance()
    val createZero = () => cachedSerializer.deserialize[U](ByteBuffer.wrap(zeroArray)) //初始化的数据如何反序列化

    // We will clean the combiner closure later in `combineByKey`
    val cleanedSeqOp = self.context.clean(seqOp)
    combineByKey[U]((v: V) => cleanedSeqOp(createZero(), v), cleanedSeqOp, combOp, partitioner)
  }

  /**
   * Aggregate the values of each key, using given combine functions and a neutral "zero value".
   * This function can return a different result type, U, than the type of the values in this RDD,
   * V. Thus, we need one operation for merging a V into a U and one operation for merging two U's,
   * as in scala.TraversableOnce. The former operation is used for merging values within a
   * partition, and the latter is used for merging values between partitions. To avoid memory
   * allocation, both of these functions are allowed to modify and return their first argument
   * instead of creating a new U.
   */
  def aggregateByKey[U: ClassTag](zeroValue: U, numPartitions: Int)(seqOp: (U, V) => U,
      combOp: (U, U) => U): RDD[(K, U)] = self.withScope {
    aggregateByKey(zeroValue, new HashPartitioner(numPartitions))(seqOp, combOp)
  }

  /**
   * Aggregate the values of each key, using given combine functions and a neutral "zero value".
   * This function can return a different result type, U, than the type of the values in this RDD,
   * V. Thus, we need one operation for merging a V into a U and one operation for merging two U's,
   * as in scala.TraversableOnce. The former operation is used for merging values within a
   * partition, and the latter is used for merging values between partitions. To avoid memory
   * allocation, both of these functions are allowed to modify and return their first argument
   * instead of creating a new U.
   */
  def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,
      combOp: (U, U) => U): RDD[(K, U)] = self.withScope {
    aggregateByKey(zeroValue, defaultPartitioner(self))(seqOp, combOp)
  }

  /**
   * Merge the values for each key using an associative function and a neutral "zero value" which
   * may be added to the result an arbitrary number of times, and must not change the result
   * (e.g., Nil for list concatenation, 0 for addition, or 1 for multiplication.).
   * 与reduceByKey一样,都是对V进行处理,返回值还是V,只是区别在于.每一个key的第一个value,要与初始值一起参与F运算,而reduceByKey第一次出现的V就直接返回V
   */
  def foldByKey(
      zeroValue: V,
      partitioner: Partitioner)(func: (V, V) => V): RDD[(K, V)] = self.withScope {
    // Serialize the zero value to a byte array so that we can get a new clone of it on each key
    val zeroBuffer = SparkEnv.get.serializer.newInstance().serialize(zeroValue) //将zeroValue对象写入流中,并且返回
    val zeroArray = new Array[Byte](zeroBuffer.limit)
    zeroBuffer.get(zeroArray)

    // When deserializing, use a lazy val to create just one instance of the serializer per task
    lazy val cachedSerializer = SparkEnv.get.serializer.newInstance()
    val createZero = () => cachedSerializer.deserialize[V](ByteBuffer.wrap(zeroArray))

    val cleanedFunc = self.context.clean(func)
    combineByKey[V]((v: V) => cleanedFunc(createZero(), v), cleanedFunc, cleanedFunc, partitioner)
  }

  /**
   * Merge the values for each key using an associative function and a neutral "zero value" which
   * may be added to the result an arbitrary number of times, and must not change the result
   * (e.g., Nil for list concatenation, 0 for addition, or 1 for multiplication.).
   */
  def foldByKey(zeroValue: V, numPartitions: Int)(func: (V, V) => V): RDD[(K, V)] = self.withScope {
    foldByKey(zeroValue, new HashPartitioner(numPartitions))(func)
  }

  /**
   * Merge the values for each key using an associative function and a neutral "zero value" which
   * may be added to the result an arbitrary number of times, and must not change the result
   * (e.g., Nil for list concatenation, 0 for addition, or 1 for multiplication.).
   */
  def foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)] = self.withScope {
    foldByKey(zeroValue, defaultPartitioner(self))(func)
  }

  /**
   * Return a subset of this RDD sampled by key (via stratified sampling).
   *
   * Create a sample of this RDD using variable sampling rates for different keys as specified by
   * `fractions`, a key to sampling rate map, via simple random sampling with one pass over the
   * RDD, to produce a sample of size that's approximately equal to the sum of
   * math.ceil(numItems * samplingRate) over all key values.
   *
   * @param withReplacement whether to sample with or without replacement
   * @param fractions map of specific keys to sampling rates
   * @param seed seed for the random number generator
   * @return RDD containing the sampled subset
   */
  def sampleByKey(withReplacement: Boolean,
      fractions: Map[K, Double],
      seed: Long = Utils.random.nextLong): RDD[(K, V)] = self.withScope {

    require(fractions.values.forall(v => v >= 0.0), "Negative sampling rates.")

    val samplingFunc = if (withReplacement) {
      StratifiedSamplingUtils.getPoissonSamplingFunction(self, fractions, false, seed)
    } else {
      StratifiedSamplingUtils.getBernoulliSamplingFunction(self, fractions, false, seed)
    }
    self.mapPartitionsWithIndex(samplingFunc, preservesPartitioning = true)
  }

  /**
   * ::Experimental::
   * Return a subset of this RDD sampled by key (via stratified sampling) containing exactly
   * math.ceil(numItems * samplingRate) for each stratum (group of pairs with the same key).
   *
   * This method differs from [[sampleByKey]] in that we make additional passes over the RDD to
   * create a sample size that's exactly equal to the sum of math.ceil(numItems * samplingRate)
   * over all key values with a 99.99% confidence. When sampling without replacement, we need one
   * additional pass over the RDD to guarantee sample size; when sampling with replacement, we need
   * two additional passes.
   *
   * @param withReplacement whether to sample with or without replacement
   * @param fractions map of specific keys to sampling rates
   * @param seed seed for the random number generator
   * @return RDD containing the sampled subset
   */
  @Experimental
  def sampleByKeyExact(
      withReplacement: Boolean,
      fractions: Map[K, Double],
      seed: Long = Utils.random.nextLong): RDD[(K, V)] = self.withScope {

    require(fractions.values.forall(v => v >= 0.0), "Negative sampling rates.")

    val samplingFunc = if (withReplacement) {
      StratifiedSamplingUtils.getPoissonSamplingFunction(self, fractions, true, seed)
    } else {
      StratifiedSamplingUtils.getBernoulliSamplingFunction(self, fractions, true, seed)
    }
    self.mapPartitionsWithIndex(samplingFunc, preservesPartitioning = true)
  }

  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce.
   *
  顾名思义，reduceByKey就是对元素为KV对的RDD中Key相同的元素的Value进行reduce，因此，Key相同的多个元素的值被reduce为一个值，然后与原RDD中的Key组成一个新的KV对。

举例:

scala> val a = sc.parallelize(List((1,2),(3,4),(3,6)))
scala> a.reduceByKey((x,y) => x + y).collect
res7: Array[(Int, Int)] = Array((1,2), (3,10))
上述例子中，对Key相同的元素的值求和，因此Key为3的两个元素被转为了(3,10)。
   */
  def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDD[(K, V)] = self.withScope {
    combineByKey[V]((v: V) => v, func, func, partitioner)
  }

  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce. Output will be hash-partitioned with numPartitions partitions.
   * 返回值是RDD[(K, V)],其中key按照partition分组到任意一个Partitions中,函数func表示相同的key该如何处理,v和v表示连续两个相同的key对应的value
   */
  def reduceByKey(func: (V, V) => V, numPartitions: Int): RDD[(K, V)] = self.withScope {
    reduceByKey(new HashPartitioner(numPartitions), func)
  }

  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce. Output will be hash-partitioned with the existing partitioner/
   * parallelism level.
   *
   * 相当于group by key,然后同一个key下面的value进行func函数处理,最终生成RDD[K,V]
   */
  def reduceByKey(func: (V, V) => V): RDD[(K, V)] = self.withScope {
    reduceByKey(defaultPartitioner(self), func)
  }

  /**
   * Merge the values for each key using an associative reduce function, but return the results
   * immediately to the master as a Map. This will also perform the merging locally on each mapper
   * before sending results to a reducer, similarly to a "combiner" in MapReduce.
   * 在一个partition节点上,使用reduce函数,合并每一个key相同的value值,在每一个mapper上本地进行merge合并操作,然后在发送结果到reducer节点上,类似于mapreduce中的combiner操作
   * 参数:func,表示如果key存在,则将存在的value值以及新的value值分别为两个函数,调用f,返回新的值
   *
   * 在本地执行map--combine--reduce一整套算法,
   * 有内存问题,因为reducePartition方法将一个迭代器传递进来,在内部维护一个map,如果迭代器内容很多,导致map会很大
   */
  def reduceByKeyLocally(func: (V, V) => V): Map[K, V] = self.withScope {
    val cleanedF = self.sparkContext.clean(func)

    if (keyClass.isArray) {
      throw new SparkException("reduceByKeyLocally() does not support array keys")
    }

    //迭代每一个partition的k-v键值对.返回新的key-value值,key还是原来的key,value经过func计算的value值
    //函数参数是迭代器,
    val reducePartition = (iter: Iterator[(K, V)]) => {
      val map = new JHashMap[K, V]
      
      /**
       * 如果key不存在,则将ket-value添加到Map中
       * 如果key存在,则将存在的value值以及新的value值分别为两个函数,调用f,返回新的值
       */
      iter.foreach { pair => //此时是懒加载方式
        val old = map.get(pair._1)//查找key对应的value
        map.put(pair._1, if (old == null) pair._2 else cleanedF(old, pair._2))//如果value已经存在,则要新老value进行运算
      }
      Iterator(map)//最终返回该partition上运算后的key-value
    } : Iterator[JHashMap[K, V]] //即对合并相同的key对应的value内容,参数func就是合并value的算法

      /**
       * 合并多个partition结果
       * 如果key不存在,则将ket-value添加到Map中
       * 如果key存在,则将存在的value值以及新的value值分别为两个函数,调用f,返回新的值
       */
    val mergeMaps = (m1: JHashMap[K, V], m2: JHashMap[K, V]) => {
      m2.foreach { pair => //懒加载模式merge若干个partition的结果集
        val old = m1.get(pair._1)
        m1.put(pair._1, if (old == null) pair._2 else cleanedF(old, pair._2))
      }
      m1
    } : JHashMap[K, V]

        /**
         * 1.先计算每一个partition,生成key-value
         * 2.合并每一个partition的值
         */
    self.mapPartitions(reducePartition).reduce(mergeMaps) //直接出发reduce的这个action,直接将结果输出到driver
  }

  /** Alias for reduceByKeyLocally */
  @deprecated("Use reduceByKeyLocally", "1.0.0")
  def reduceByKeyToDriver(func: (V, V) => V): Map[K, V] = self.withScope {
    reduceByKeyLocally(func)
  }

  /**
   * Count the number of elements for each key, collecting the results to a local Map.
   * 注意他是一个action行为,因为调用了collect,会将所有的而结果存储到本地的map中,每一个key出现的次数,因此内存要求较高,最好不经常使用
   * Note that this method should only be used if the resulting map is expected to be small, as 此时时候的时候仅仅是结果集很小的时候才允许使用
   * the whole thing is loaded into the driver's memory.
   * To handle very large results, consider using rdd.mapValues(_ => 1L).reduceByKey(_ + _), which
   * returns an RDD[T, Long] instead of a map.
   *
   * 1.先将RDD[K,V] 转换成RDD[K,1]
   * 2.对相同的key的value进行_ + _c处理,返回RDD[K,V],其中key不变,value就是相同key出现的次数
   * 3.将结果集抓去到本地,转换成Map
   * 返回值[key,key相同的元素的次数]
   */
  def countByKey(): Map[K, Long] = self.withScope {
    self.mapValues(_ => 1L).reduceByKey(_ + _).collect().toMap
  }

  /**
   * :: Experimental ::
   * Approximate version of countByKey that can return a partial result if it does
   * not finish within a timeout.
   */
  @Experimental
  def countByKeyApprox(timeout: Long, confidence: Double = 0.95)
      : PartialResult[Map[K, BoundedDouble]] = self.withScope {
    self.map(_._1).countByValueApprox(timeout, confidence)
  }

  /**
   * :: Experimental ::
   *
   * Return approximate number of distinct values for each key in this RDD.
   * 返回RDD中每一个key对应的value的不重复的次数的近似值
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="http://dx.doi.org/10.1145/2452376.2452456">here</a>.
   *
   * The relative accuracy is approximately `1.054 / sqrt(2^p)`. Setting a nonzero `sp > p`
   * would trigger sparse representation of registers, which may reduce the memory consumption
   * and increase accuracy when the cardinality is small.
   *
   * @param p The precision value for the normal set.
   *          `p` must be a value between 4 and `sp` if `sp` is not zero (32 max).
   * @param sp The precision value for the sparse set, between 0 and 32.
   *           If `sp` equals 0, the sparse representation is skipped.
   * @param partitioner Partitioner to use for the resulting RDD.
   */
  @Experimental
  def countApproxDistinctByKey(
      p: Int,
      sp: Int,
      partitioner: Partitioner): RDD[(K, Long)] = self.withScope {
    require(p >= 4, s"p ($p) must be >= 4") //必须大于4
    require(sp <= 32, s"sp ($sp) must be <= 32") //必须小于32
    require(sp == 0 || p <= sp, s"p ($p) cannot be greater than sp ($sp)") //sp必须大于4

    val createHLL = (v: V) => {
      val hll = new HyperLogLogPlus(p, sp)
      hll.offer(v)
      hll
    }
    val mergeValueHLL = (hll: HyperLogLogPlus, v: V) => {
      hll.offer(v)
      hll
    }
    val mergeHLL = (h1: HyperLogLogPlus, h2: HyperLogLogPlus) => {
      h1.addAll(h2)
      h1
    }

    combineByKey(createHLL, mergeValueHLL, mergeHLL, partitioner).mapValues(_.cardinality())
  }

  /**
   * Return approximate number of distinct values for each key in this RDD.
   *
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="http://dx.doi.org/10.1145/2452376.2452456">here</a>.
   *
   * @param relativeSD Relative accuracy. Smaller values create counters that require more space.
   *                   It must be greater than 0.000017.
   * @param partitioner partitioner of the resulting RDD
   */
  def countApproxDistinctByKey(
      relativeSD: Double,
      partitioner: Partitioner): RDD[(K, Long)] = self.withScope {
    require(relativeSD > 0.000017, s"accuracy ($relativeSD) must be greater than 0.000017")
    val p = math.ceil(2.0 * math.log(1.054 / relativeSD) / math.log(2)).toInt
    assert(p <= 32)
    countApproxDistinctByKey(if (p < 4) 4 else p, 0, partitioner)
  }

  /**
   * Return approximate number of distinct values for each key in this RDD.
   *
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="http://dx.doi.org/10.1145/2452376.2452456">here</a>.
   *
   * @param relativeSD Relative accuracy. Smaller values create counters that require more space.
   *                   It must be greater than 0.000017.
   * @param numPartitions number of partitions of the resulting RDD
   */
  def countApproxDistinctByKey(
      relativeSD: Double,
      numPartitions: Int): RDD[(K, Long)] = self.withScope {
    countApproxDistinctByKey(relativeSD, new HashPartitioner(numPartitions))
  }

  /**
   * Return approximate number of distinct values for each key in this RDD.
   *
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="http://dx.doi.org/10.1145/2452376.2452456">here</a>.
   *
   * @param relativeSD Relative accuracy. Smaller values create counters that require more space.
   *                   It must be greater than 0.000017.
   */
  def countApproxDistinctByKey(relativeSD: Double = 0.05): RDD[(K, Long)] = self.withScope {
    countApproxDistinctByKey(relativeSD, defaultPartitioner(self))
  }

  /**
   * Group the values for each key in the RDD into a single sequence. Allows controlling the
   * partitioning of the resulting key-value pair RDD by passing a Partitioner.
   * The ordering of elements within each group is not guaranteed, and may even differ
   * each time the resulting RDD is evaluated.
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using [[PairRDDFunctions.aggregateByKey]]
   * or [[PairRDDFunctions.reduceByKey]] will provide much better performance.
   *
   * Note: As currently implemented, groupByKey must be able to hold all the key-value pairs for any
   * key in memory. If a key has too many values, it can result in an [[OutOfMemoryError]].
   * 按照key进行分组,value是相同key组成的集合迭代器
   */
  def groupByKey(partitioner: Partitioner): RDD[(K, Iterable[V])] = self.withScope {
    // groupByKey shouldn't use map side combine because map side combine does not
    // reduce the amount of data shuffled and requires all map side data be inserted
    // into a hash table, leading to more objects in the old gen.
    val createCombiner = (v: V) => CompactBuffer(v) //第一次出现key的时候,创建一个队列
    val mergeValue = (buf: CompactBuffer[V], v: V) => buf += v //将value添加到队列中
    val mergeCombiners = (c1: CompactBuffer[V], c2: CompactBuffer[V]) => c1 ++= c2 //合并key相同的队列
    val bufs = combineByKey[CompactBuffer[V]](
      createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine = false)
    bufs.asInstanceOf[RDD[(K, Iterable[V])]]
  }

  /**
   * Group the values for each key in the RDD into a single sequence. Hash-partitions the
   * resulting RDD with into `numPartitions` partitions. The ordering of elements within
   * each group is not guaranteed, and may even differ each time the resulting RDD is evaluated.
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using [[PairRDDFunctions.aggregateByKey]]
   * or [[PairRDDFunctions.reduceByKey]] will provide much better performance.
   *
   * Note: As currently implemented, groupByKey must be able to hold all the key-value pairs for any
   * key in memory. If a key has too many values, it can result in an [[OutOfMemoryError]].
   */
  def groupByKey(numPartitions: Int): RDD[(K, Iterable[V])] = self.withScope {
    groupByKey(new HashPartitioner(numPartitions))
  }

  /**
   * Return a copy of the RDD partitioned using the specified partitioner.
   * 重新对RDD进行partition分组
   */
  def partitionBy(partitioner: Partitioner): RDD[(K, V)] = self.withScope {
    if (keyClass.isArray && partitioner.isInstanceOf[HashPartitioner]) {
      throw new SparkException("Default partitioner cannot partition array keys.")
    }
    if (self.partitioner == Some(partitioner)) {
      self
    } else {
      new ShuffledRDD[K, V, V](self, partitioner)
    }
  }

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Uses the given Partitioner to partition the output RDD.
   * 将两个RDD,组装成新的RDD.返回一个新的RDD,仅将两个RDD都包含的key进行组装成新的RDD
   * 返回值RDD[(K, (V, W))],表示是相同的key做为key,value是元组,表示第一个RDD的value和第二个RDD的value组成的元组
   */
  def join[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, W))] = self.withScope {
    //this.cogroup返回RDD[(K, (Iterable[V], Iterable[W]))]
    this.cogroup(other, partitioner).flatMapValues( pair =>
      for (v <- pair._1.iterator; w <- pair._2.iterator) yield (v, w) //两层循环,分别笛卡尔乘积打印v和w
    )
  }

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Uses the given Partitioner to
   * partition the output RDD.
   *
   */
  def leftOuterJoin[W](
      other: RDD[(K, W)],
      partitioner: Partitioner): RDD[(K, (V, Option[W]))] = self.withScope { //左连接,因此W可能是没有内容,因此是Option对象
    this.cogroup(other, partitioner).flatMapValues { pair =>
      if (pair._2.isEmpty) {//如果就没有右边的数据,因此直接输出即可
        pair._1.iterator.map(v => (v, None))
      } else {//说明如果有右边的对象,则笛卡尔乘积关联
        for (v <- pair._1.iterator; w <- pair._2.iterator) yield (v, Some(w)) //一般发生左关联的时候,都是左边1个记录,右边多条记录情况,很少发生笛卡尔乘积
      }
    }
  }

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Uses the given Partitioner to
   * partition the output RDD.
   */
  def rightOuterJoin[W](other: RDD[(K, W)], partitioner: Partitioner)
      : RDD[(K, (Option[V], W))] = self.withScope {//右连接,因此V可能是没有内容,因此是Option对象
    this.cogroup(other, partitioner).flatMapValues { pair =>
      if (pair._1.isEmpty) {//右边是空,则直接输出
        pair._2.iterator.map(w => (None, w))
      } else {
        for (v <- pair._1.iterator; w <- pair._2.iterator) yield (Some(v), w) //一般发生右关联的时候,都是右边1个记录,左边多条记录情况,很少发生笛卡尔乘积
      }
    }
  }

  /**
   * Perform a full outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (Some(v), Some(w))) for w in `other`, or
   * the pair (k, (Some(v), None)) if no elements in `other` have key k. Similarly, for each
   * element (k, w) in `other`, the resulting RDD will either contain all pairs
   * (k, (Some(v), Some(w))) for v in `this`, or the pair (k, (None, Some(w))) if no elements
   * in `this` have key k. Uses the given Partitioner to partition the output RDD.
   */
  def fullOuterJoin[W](other: RDD[(K, W)], partitioner: Partitioner)
      : RDD[(K, (Option[V], Option[W]))] = self.withScope {//全表连接,只要出现过一次key,则V和W有一个是空,或者都不是空,至于哪个是空是不确定的,因此都是Option对象
    this.cogroup(other, partitioner).flatMapValues {
      case (vs, Seq()) => vs.iterator.map(v => (Some(v), None))//1对空
      case (Seq(), ws) => ws.iterator.map(w => (None, Some(w)))//空对1
      case (vs, ws) => for (v <- vs.iterator; w <- ws.iterator) yield (Some(v), Some(w))//多对多
    }
  }

  /**
   * Simplified version of combineByKey that hash-partitions the resulting RDD using the
   * existing partitioner/parallelism level.
   */
  def combineByKey[C](createCombiner: V => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C)
    : RDD[(K, C)] = self.withScope {
    combineByKey(createCombiner, mergeValue, mergeCombiners, defaultPartitioner(self))
  }

  /**
   * Group the values for each key in the RDD into a single sequence. Hash-partitions the
   * resulting RDD with the existing partitioner/parallelism level. The ordering of elements
   * within each group is not guaranteed, and may even differ each time the resulting RDD is
   * evaluated.
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using [[PairRDDFunctions.aggregateByKey]]
   * or [[PairRDDFunctions.reduceByKey]] will provide much better performance.
   */
  def groupByKey(): RDD[(K, Iterable[V])] = self.withScope {
    groupByKey(defaultPartitioner(self))
  }

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Performs a hash join across the cluster.
   * 将两个RDD,组装成新的RDD.返回一个新的RDD,仅将两个RDD都包含的key进行组装成新的RDD
   * 返回值RDD[(K, (V, W))],表示是相同的key做为key,value是元组,表示第一个RDD的value和第二个RDD的value组成的元组
   */
  def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))] = self.withScope {
    join(other, defaultPartitioner(self, other))
  }

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Performs a hash join across the cluster.
   * 将两个RDD,组装成新的RDD.返回一个新的RDD,仅将两个RDD都包含的key进行组装成新的RDD
   * 返回值RDD[(K, (V, W))],表示是相同的key做为key,value是元组,表示第一个RDD的value和第二个RDD的value组成的元组
   */
  def join[W](other: RDD[(K, W)], numPartitions: Int): RDD[(K, (V, W))] = self.withScope {
    join(other, new HashPartitioner(numPartitions))
  }

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Hash-partitions the output
   * using the existing partitioner/parallelism level.
   */
  def leftOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))] = self.withScope {
    leftOuterJoin(other, defaultPartitioner(self, other))
  }

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Hash-partitions the output
   * into `numPartitions` partitions.
   */
  def leftOuterJoin[W](
      other: RDD[(K, W)],
      numPartitions: Int): RDD[(K, (V, Option[W]))] = self.withScope {
    leftOuterJoin(other, new HashPartitioner(numPartitions))
  }

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Hash-partitions the resulting
   * RDD using the existing partitioner/parallelism level.
   */
  def rightOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (Option[V], W))] = self.withScope {
    rightOuterJoin(other, defaultPartitioner(self, other))
  }

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Hash-partitions the resulting
   * RDD into the given number of partitions.
   */
  def rightOuterJoin[W](
      other: RDD[(K, W)],
      numPartitions: Int): RDD[(K, (Option[V], W))] = self.withScope {
    rightOuterJoin(other, new HashPartitioner(numPartitions))
  }

  /**
   * Perform a full outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (Some(v), Some(w))) for w in `other`, or
   * the pair (k, (Some(v), None)) if no elements in `other` have key k. Similarly, for each
   * element (k, w) in `other`, the resulting RDD will either contain all pairs
   * (k, (Some(v), Some(w))) for v in `this`, or the pair (k, (None, Some(w))) if no elements
   * in `this` have key k. Hash-partitions the resulting RDD using the existing partitioner/
   * parallelism level.
   */
  def fullOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (Option[V], Option[W]))] = self.withScope {
    fullOuterJoin(other, defaultPartitioner(self, other))
  }

  /**
   * Perform a full outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (Some(v), Some(w))) for w in `other`, or
   * the pair (k, (Some(v), None)) if no elements in `other` have key k. Similarly, for each
   * element (k, w) in `other`, the resulting RDD will either contain all pairs
   * (k, (Some(v), Some(w))) for v in `this`, or the pair (k, (None, Some(w))) if no elements
   * in `this` have key k. Hash-partitions the resulting RDD into the given number of partitions.
   */
  def fullOuterJoin[W](
      other: RDD[(K, W)],
      numPartitions: Int): RDD[(K, (Option[V], Option[W]))] = self.withScope {
    fullOuterJoin(other, new HashPartitioner(numPartitions))
  }

  /**
   * Return the key-value pairs in this RDD to the master as a Map.
   *
   * Warning: this doesn't return a multimap (so if you have multiple values to the same key, only
   *          one value per key is preserved in the map returned)
   * 将RDD处理的所有结果,调用collect方法,返回的是K-V键值对的信息,将其存储在Map中返回
   */
  def collectAsMap(): Map[K, V] = self.withScope {
    val data = self.collect()
    val map = new mutable.HashMap[K, V]
    map.sizeHint(data.length)
    data.foreach { pair => map.put(pair._1, pair._2) }
    map
  }

  /**
   * Pass each value in the key-value pair RDD through a map function without changing the keys;
   * this also retains the original RDD's partitioning.
   * 将RDD[K-V]转换成RDD[k,f[V=>U]] = RDD[k,U]操作
   * 即对key-value中的value进行map方法映射,转换成其他对象,最终将RDD[k,v]转换成RDD[k,u]
   */
  def mapValues[U](f: V => U): RDD[(K, U)] = self.withScope {
    val cleanF = self.context.clean(f)
    new MapPartitionsRDD[(K, U), (K, V)](self,
      (context, pid, iter) => iter.map { case (k, v) => (k, cleanF(v)) },
      preservesPartitioning = true)
  }

  /**
   * Pass each value in the key-value pair RDD through a flatMap function without changing the
   * keys; this also retains the original RDD's partitioning.
   * 针对RDD[(k,v)] 将其转换成RDD[(K, U)]
   * 过程:
   * a.针对每一个v调用函数f,将其一个v转化成迭代器U,即一个v可以转换成一组数据
   * b.针对迭代器U,调用map方法,转换成k-U
   * 总结,一个v转换成一组U,从而等式为一个key-value,转换成一组K-U的过程
   */
  def flatMapValues[U](f: V => TraversableOnce[U]): RDD[(K, U)] = self.withScope {
    val cleanF = self.context.clean(f)
    new MapPartitionsRDD[(K, U), (K, V)](self,
      (context, pid, iter) => iter.flatMap { case (k, v) =>
        cleanF(v).map(x => (k, x))
      },
      preservesPartitioning = true)
  }

  /**
   * For each key k in `this` or `other1` or `other2` or `other3`,
   * return a resulting RDD that contains a tuple with the list of values
   * for that key in `this`, `other1`, `other2` and `other3`.
   * RDD与三个RDD进行group by key,按照key去关联
   * 返回值是RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))],即key与三个RDD对应的迭代器组成的元组作为value返回
   */
  def cogroup[W1, W2, W3](other1: RDD[(K, W1)],
      other2: RDD[(K, W2)],
      other3: RDD[(K, W3)],
      partitioner: Partitioner)
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = self.withScope {
    if (partitioner.isInstanceOf[HashPartitioner] && keyClass.isArray) {
      throw new SparkException("Default partitioner cannot partition array keys.")
    }
    val cg = new CoGroupedRDD[K](Seq(self, other1, other2, other3), partitioner)
    cg.mapValues { case Array(vs, w1s, w2s, w3s) =>
       (vs.asInstanceOf[Iterable[V]],
         w1s.asInstanceOf[Iterable[W1]],
         w2s.asInstanceOf[Iterable[W2]],
         w3s.asInstanceOf[Iterable[W3]])
    }
  }

  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   */
  def cogroup[W](other: RDD[(K, W)], partitioner: Partitioner)
      : RDD[(K, (Iterable[V], Iterable[W]))] = self.withScope { //两个RDD做join,相同的key,生产两个不同的迭代器,迭代器是不耗费内存的
    if (partitioner.isInstanceOf[HashPartitioner] && keyClass.isArray) {
      throw new SparkException("Default partitioner cannot partition array keys.")
    }
    val cg = new CoGroupedRDD[K](Seq(self, other), partitioner)
    cg.mapValues { case Array(vs, w1s) =>
      (vs.asInstanceOf[Iterable[V]], w1s.asInstanceOf[Iterable[W]])
    }
  }

  /**
   * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
   * tuple with the list of values for that key in `this`, `other1` and `other2`.
   */
  def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)], partitioner: Partitioner)
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = self.withScope {
    if (partitioner.isInstanceOf[HashPartitioner] && keyClass.isArray) {
      throw new SparkException("Default partitioner cannot partition array keys.")
    }
    val cg = new CoGroupedRDD[K](Seq(self, other1, other2), partitioner)
    cg.mapValues { case Array(vs, w1s, w2s) =>
      (vs.asInstanceOf[Iterable[V]],
        w1s.asInstanceOf[Iterable[W1]],
        w2s.asInstanceOf[Iterable[W2]])
    }
  }

  /**
   * For each key k in `this` or `other1` or `other2` or `other3`,
   * return a resulting RDD that contains a tuple with the list of values
   * for that key in `this`, `other1`, `other2` and `other3`.
   */
  def cogroup[W1, W2, W3](other1: RDD[(K, W1)], other2: RDD[(K, W2)], other3: RDD[(K, W3)])
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = self.withScope {
    cogroup(other1, other2, other3, defaultPartitioner(self, other1, other2, other3))
  }

  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   */
  def cogroup[W](other: RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))] = self.withScope {
    cogroup(other, defaultPartitioner(self, other))
  }

  /**
   * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
   * tuple with the list of values for that key in `this`, `other1` and `other2`.
   */
  def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)])
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = self.withScope {
    cogroup(other1, other2, defaultPartitioner(self, other1, other2))
  }

  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   */
  def cogroup[W](
      other: RDD[(K, W)],
      numPartitions: Int): RDD[(K, (Iterable[V], Iterable[W]))] = self.withScope {
    cogroup(other, new HashPartitioner(numPartitions))
  }

  /**
   * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
   * tuple with the list of values for that key in `this`, `other1` and `other2`.
   */
  def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)], numPartitions: Int)
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = self.withScope {
    cogroup(other1, other2, new HashPartitioner(numPartitions))
  }

  /**
   * For each key k in `this` or `other1` or `other2` or `other3`,
   * return a resulting RDD that contains a tuple with the list of values
   * for that key in `this`, `other1`, `other2` and `other3`.
   */
  def cogroup[W1, W2, W3](other1: RDD[(K, W1)],
      other2: RDD[(K, W2)],
      other3: RDD[(K, W3)],
      numPartitions: Int)
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = self.withScope {
    cogroup(other1, other2, other3, new HashPartitioner(numPartitions))
  }

  /** Alias for cogroup. cogroup与groupWith是同一个函数,只是groupWith是别名而已 */
  def groupWith[W](other: RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))] = self.withScope {
    cogroup(other, defaultPartitioner(self, other))
  }

  /** Alias for cogroup. */
  def groupWith[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)])
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = self.withScope {
    cogroup(other1, other2, defaultPartitioner(self, other1, other2))
  }

  /** Alias for cogroup. */
  def groupWith[W1, W2, W3](other1: RDD[(K, W1)], other2: RDD[(K, W2)], other3: RDD[(K, W3)])
      : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = self.withScope {
    cogroup(other1, other2, other3, defaultPartitioner(self, other1, other2, other3))
  }

  /**
   * Return an RDD with the pairs from `this` whose keys are not in `other`.
   *
   * Uses `this` partitioner/partition size, because even if `other` is huge, the resulting
   * RDD will be <= us.
   * 对两个rdd进行交互,获取RDD1存在,RDD2不存在数据,即差异的元素
   */
  def subtractByKey[W: ClassTag](other: RDD[(K, W)]): RDD[(K, V)] = self.withScope {
    subtractByKey(other, self.partitioner.getOrElse(new HashPartitioner(self.partitions.length)))
  }

  /** Return an RDD with the pairs from `this` whose keys are not in `other`.
    * 对两个rdd进行交互,获取RDD1存在,RDD2不存在数据,即差异的元素
    **/
  def subtractByKey[W: ClassTag](
      other: RDD[(K, W)],
      numPartitions: Int): RDD[(K, V)] = self.withScope {
    subtractByKey(other, new HashPartitioner(numPartitions))
  }

  /** Return an RDD with the pairs from `this` whose keys are not in `other`.
    * 对两个rdd进行交互,获取RDD1存在,RDD2不存在数据,即差异的元素
    **/
  def subtractByKey[W: ClassTag](other: RDD[(K, W)], p: Partitioner): RDD[(K, V)] = self.withScope {
    new SubtractedRDD[K, V, W](self, other, p)
  }

  /**
   * Return the list of values in the RDD for key `key`. This operation is done efficiently if the
   * RDD has a known partitioner by only searching the partition that the key maps to.
   * 返回该key对应的所有value值的集合
   * 如果这个RDD是有partition分区的,该操作非常有效率的做完,因为仅需要搜索该partition分区就可以了
   * 
   * 返回值是ArrayBuffer[V]
   */
  def lookup(key: K): Seq[V] = self.withScope {
    self.partitioner match {
      case Some(p) =>
        val index = p.getPartition(key)//key分配在第几个partition上
        
        //迭代key-value,找到key与参数key相同的,将value追加到ArrayBuffer中返回即可
        val process = (it: Iterator[(K, V)]) => {
          val buf = new ArrayBuffer[V]
          for (pair <- it if pair._1 == key) {//找到与key相同的,就添加到队列中
            buf += pair._2
          }
          buf
        } : Seq[V]
        val res = self.context.runJob(self, process, Array(index))//处理index个partition对象
        res(0)
      case None =>
        self.filter(_._1 == key).map(_._2).collect() //如果没有partition,则需要遍历整个RDD,寻找与key相同的value值
        //1.过滤与key相同的key-value集合
        //2.将转换成value集合
    }
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a Hadoop `OutputFormat` class
   * supporting the key and value types K and V in this RDD.
   * 输出RDD的内容到Hadoop系统中,存储在path上,存储的类型就是OutputFormat[K, V]对应的key与value类型
   */
  def saveAsHadoopFile[F <: OutputFormat[K, V]](
      path: String)(implicit fm: ClassTag[F]): Unit = self.withScope {
    saveAsHadoopFile(path, keyClass, valueClass, fm.runtimeClass.asInstanceOf[Class[F]])
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a Hadoop `OutputFormat` class
   * supporting the key and value types K and V in this RDD. Compress the result with the
   * supplied codec.
   * 输出RDD的内容到Hadoop系统中,存储在path上,存储的类型就是OutputFormat[K, V]对应的key与value类型
   * 该函数支持对存储的结果进行编码处理
   */
  def saveAsHadoopFile[F <: OutputFormat[K, V]](
      path: String,
      codec: Class[_ <: CompressionCodec])(implicit fm: ClassTag[F]): Unit = self.withScope {
    val runtimeClass = fm.runtimeClass
    saveAsHadoopFile(path, keyClass, valueClass, runtimeClass.asInstanceOf[Class[F]], codec)
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a new Hadoop API `OutputFormat`
   * (mapreduce.OutputFormat) object supporting the key and value types K and V in this RDD.
   */
  def saveAsNewAPIHadoopFile[F <: NewOutputFormat[K, V]](
      path: String)(implicit fm: ClassTag[F]): Unit = self.withScope {
    saveAsNewAPIHadoopFile(path, keyClass, valueClass, fm.runtimeClass.asInstanceOf[Class[F]])
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a new Hadoop API `OutputFormat`
   * (mapreduce.OutputFormat) object supporting the key and value types K and V in this RDD.
   */
  def saveAsNewAPIHadoopFile(
      path: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: NewOutputFormat[_, _]],
      conf: Configuration = self.context.hadoopConfiguration): Unit = self.withScope {
    // Rename this as hadoopConf internally to avoid shadowing (see SPARK-2038).
    val hadoopConf = conf
    val job = new NewAPIHadoopJob(hadoopConf)
    job.setOutputKeyClass(keyClass)
    job.setOutputValueClass(valueClass)
    job.setOutputFormatClass(outputFormatClass)
    job.getConfiguration.set("mapred.output.dir", path)
    saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a Hadoop `OutputFormat` class
   * supporting the key and value types K and V in this RDD. Compress with the supplied codec.
   * 输出RDD的内容到Hadoop系统中,存储在path上,存储的类型就是OutputFormat[K, V]对应的key与value类型
   * 该函数支持对存储的结果进行编码处理
   */
  def saveAsHadoopFile(
      path: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: OutputFormat[_, _]],
      codec: Class[_ <: CompressionCodec]): Unit = self.withScope {
    saveAsHadoopFile(path, keyClass, valueClass, outputFormatClass,
      new JobConf(self.context.hadoopConfiguration), Some(codec))
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a Hadoop `OutputFormat` class
   * supporting the key and value types K and V in this RDD.
   * 输出RDD的内容到Hadoop系统中,存储在path上,存储的类型就是OutputFormat[K, V]对应的key与value类型
   * 该函数支持对存储的结果进行编码处理
   */
  def saveAsHadoopFile(
      path: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: OutputFormat[_, _]],
      conf: JobConf = new JobConf(self.context.hadoopConfiguration),
      codec: Option[Class[_ <: CompressionCodec]] = None): Unit = self.withScope {
    // Rename this as hadoopConf internally to avoid shadowing (see SPARK-2038).
    val hadoopConf = conf
    hadoopConf.setOutputKeyClass(keyClass)
    hadoopConf.setOutputValueClass(valueClass)
    // Doesn't work in Scala 2.9 due to what may be a generics bug
    // TODO: Should we uncomment this for Scala 2.10?
    // conf.setOutputFormat(outputFormatClass)
    hadoopConf.set("mapred.output.format.class", outputFormatClass.getName)
    for (c <- codec) {
      hadoopConf.setCompressMapOutput(true)
      hadoopConf.set("mapred.output.compress", "true")
      hadoopConf.setMapOutputCompressorClass(c)
      hadoopConf.set("mapred.output.compression.codec", c.getCanonicalName)
      hadoopConf.set("mapred.output.compression.type", CompressionType.BLOCK.toString)
    }

    // Use configured output committer if already set
    if (conf.getOutputCommitter == null) {
      hadoopConf.setOutputCommitter(classOf[FileOutputCommitter])
    }

    FileOutputFormat.setOutputPath(hadoopConf,
      SparkHadoopWriter.createPathFromString(path, hadoopConf))
    saveAsHadoopDataset(hadoopConf)
  }

  /**
   * Output the RDD to any Hadoop-supported storage system with new Hadoop API, using a Hadoop
   * Configuration object for that storage system. The Conf should set an OutputFormat and any
   * output paths required (e.g. a table name to write to) in the same way as it would be
   * configured for a Hadoop MapReduce job.
   */
  def saveAsNewAPIHadoopDataset(conf: Configuration): Unit = self.withScope {
    // Rename this as hadoopConf internally to avoid shadowing (see SPARK-2038).
    val hadoopConf = conf
    val job = new NewAPIHadoopJob(hadoopConf)
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    val jobtrackerID = formatter.format(new Date())
    val stageId = self.id
    val wrappedConf = new SerializableConfiguration(job.getConfiguration)
    val outfmt = job.getOutputFormatClass
    val jobFormat = outfmt.newInstance

    if (isOutputSpecValidationEnabled) {
      // FileOutputFormat ignores the filesystem parameter
      jobFormat.checkOutputSpecs(job)
    }

    val writeShard = (context: TaskContext, iter: Iterator[(K, V)]) => {
      val config = wrappedConf.value
      /* "reduce task" <split #> <attempt # = spark task #> */
      val attemptId = newTaskAttemptID(jobtrackerID, stageId, isMap = false, context.partitionId,
        context.attemptNumber)//我们是知道partitionID的,因此可以知道该文件的全路径
      val hadoopContext = newTaskAttemptContext(config, attemptId)
      val format = outfmt.newInstance
      format match {
        case c: Configurable => c.setConf(config)
        case _ => ()
      }
      val committer = format.getOutputCommitter(hadoopContext)
      committer.setupTask(hadoopContext)

      val (outputMetrics, bytesWrittenCallback) = initHadoopOutputMetrics(context)

      val writer = format.getRecordWriter(hadoopContext).asInstanceOf[NewRecordWriter[K, V]]
      require(writer != null, "Unable to obtain RecordWriter")
      var recordsWritten = 0L
      Utils.tryWithSafeFinally {
        while (iter.hasNext) {
          val pair = iter.next()
          writer.write(pair._1, pair._2)

          // Update bytes written metric every few records
          maybeUpdateOutputMetrics(bytesWrittenCallback, outputMetrics, recordsWritten)
          recordsWritten += 1
        }
      } {
        writer.close(hadoopContext)
      }
      committer.commitTask(hadoopContext)
      bytesWrittenCallback.foreach { fn => outputMetrics.setBytesWritten(fn()) }
      outputMetrics.setRecordsWritten(recordsWritten)
      1
    } : Int

    val jobAttemptId = newTaskAttemptID(jobtrackerID, stageId, isMap = true, 0, 0)
    val jobTaskContext = newTaskAttemptContext(wrappedConf.value, jobAttemptId)
    val jobCommitter = jobFormat.getOutputCommitter(jobTaskContext)
    jobCommitter.setupJob(jobTaskContext)
    self.context.runJob(self, writeShard)
    jobCommitter.commitJob(jobTaskContext)
  }

  /**
   * Output the RDD to any Hadoop-supported storage system, using a Hadoop JobConf object for
   * that storage system. The JobConf should set an OutputFormat and any output paths required
   * (e.g. a table name to write to) in the same way as it would be configured for a Hadoop
   * MapReduce job.
   * 真正去执行hadoop存储,所有的环境都已经存储在conf环境配置中了
   */
  def saveAsHadoopDataset(conf: JobConf): Unit = self.withScope {
    // Rename this as hadoopConf internally to avoid shadowing (see SPARK-2038).
    val hadoopConf = conf
    val wrappedConf = new SerializableConfiguration(hadoopConf)
    val outputFormatInstance = hadoopConf.getOutputFormat
    val keyClass = hadoopConf.getOutputKeyClass
    val valueClass = hadoopConf.getOutputValueClass
    if (outputFormatInstance == null) {
      throw new SparkException("Output format class not set")
    }
    if (keyClass == null) {
      throw new SparkException("Output key class not set")
    }
    if (valueClass == null) {
      throw new SparkException("Output value class not set")
    }
    SparkHadoopUtil.get.addCredentials(hadoopConf)

    logDebug("Saving as hadoop file of type (" + keyClass.getSimpleName + ", " +
      valueClass.getSimpleName + ")")

    if (isOutputSpecValidationEnabled) {
      // FileOutputFormat ignores the filesystem parameter
      val ignoredFs = FileSystem.get(hadoopConf)
      hadoopConf.getOutputFormat.checkOutputSpecs(ignoredFs, hadoopConf)
    }

    val writer = new SparkHadoopWriter(hadoopConf)
    writer.preSetup()

    val writeToFile = (context: TaskContext, iter: Iterator[(K, V)]) => {
      val config = wrappedConf.value
      // Hadoop wants a 32-bit task attempt ID, so if ours is bigger than Int.MaxValue, roll it
      // around by taking a mod. We expect that no task will be attempted 2 billion times.
      val taskAttemptId = (context.taskAttemptId % Int.MaxValue).toInt

      val (outputMetrics, bytesWrittenCallback) = initHadoopOutputMetrics(context)

      writer.setup(context.stageId, context.partitionId, taskAttemptId)
      writer.open() //根据该任务ID和文件路径,找到对应的存储文件全路径,创建该文件
      var recordsWritten = 0L //总记录数

      Utils.tryWithSafeFinally {
        while (iter.hasNext) {
          val record = iter.next()
          writer.write(record._1.asInstanceOf[AnyRef], record._2.asInstanceOf[AnyRef])

          // Update bytes written metric every few records
          maybeUpdateOutputMetrics(bytesWrittenCallback, outputMetrics, recordsWritten)
          recordsWritten += 1
        }
      } {
        writer.close()
      }
      writer.commit()
      bytesWrittenCallback.foreach { fn => outputMetrics.setBytesWritten(fn()) }
      outputMetrics.setRecordsWritten(recordsWritten)
    }

    self.context.runJob(self, writeToFile)
    writer.commitJob()
  }

  private def initHadoopOutputMetrics(context: TaskContext): (OutputMetrics, Option[() => Long]) = {
    val bytesWrittenCallback = SparkHadoopUtil.get.getFSBytesWrittenOnThreadCallback()
    val outputMetrics = new OutputMetrics(DataWriteMethod.Hadoop)
    if (bytesWrittenCallback.isDefined) {
      context.taskMetrics.outputMetrics = Some(outputMetrics)
    }
    (outputMetrics, bytesWrittenCallback)
  }

  private def maybeUpdateOutputMetrics(bytesWrittenCallback: Option[() => Long],
      outputMetrics: OutputMetrics, recordsWritten: Long): Unit = {
    if (recordsWritten % PairRDDFunctions.RECORDS_BETWEEN_BYTES_WRITTEN_METRIC_UPDATES == 0) {
      bytesWrittenCallback.foreach { fn => outputMetrics.setBytesWritten(fn()) }
      outputMetrics.setRecordsWritten(recordsWritten)
    }
  }

  /**
   * Return an RDD with the keys of each tuple.
   * 返回所有的key组成的RDD
   */
  def keys: RDD[K] = self.map(_._1)

  /**
   * Return an RDD with the values of each tuple.
   * 返回所有的value组成的RDD
   */
  def values: RDD[V] = self.map(_._2)

  //key对应的class
  private[spark] def keyClass: Class[_] = kt.runtimeClass

  //VALUE对应的class
  private[spark] def valueClass: Class[_] = vt.runtimeClass

  //key对应的排序对象
  private[spark] def keyOrdering: Option[Ordering[K]] = Option(ord)

  // Note: this needs to be a function instead of a 'val' so that the disableOutputSpecValidation
  // setting can take effect:
  private def isOutputSpecValidationEnabled: Boolean = {
    val validationDisabled = PairRDDFunctions.disableOutputSpecValidation.value
    val enabledInConf = self.conf.getBoolean("spark.hadoop.validateOutputSpecs", true)
    enabledInConf && !validationDisabled
  }
}

private[spark] object PairRDDFunctions {
  val RECORDS_BETWEEN_BYTES_WRITTEN_METRIC_UPDATES = 256

  /**
   * Allows for the `spark.hadoop.validateOutputSpecs` checks to be disabled on a case-by-case
   * basis; see SPARK-4835 for more details.
   */
  val disableOutputSpecValidation: DynamicVariable[Boolean] = new DynamicVariable[Boolean](false)
}
