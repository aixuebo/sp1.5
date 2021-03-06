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

import java.io.{IOException, ObjectOutputStream}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.existentials
import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.util.Utils

/**
 * Class that captures a coalesced RDD by essentially keeping track of parent partitions
 * @param index of this coalesced partition
 * @param rdd which it belongs to
 * @param parentsIndices list of indices in the parent that have been coalesced into this partition
 * @param preferredLocation the preferred location for this partition
 * 合并RDD
 * 代表合并后的某一个分区
 */
private[spark] case class CoalescedRDDPartition(
    index: Int,//该合并的新分区的序号
    @transient rdd: RDD[_],//要合并的父RDD
    parentsIndices: Array[Int],//该分区要合并父RDD的哪些分区
    @transient preferredLocation: Option[String] = None) extends Partition {//该分区首选的host,即该分区在哪个host上执行最好

  //其中_表示Array[Int]中的int
  var parents: Seq[Partition] = parentsIndices.map(rdd.partitions(_)) //获取rdd的第index个Partition,组成的集合,这个集合的partition的index来自于parentsIndices数组

  //序列化
  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent partition at the time of task serialization
    parents = parentsIndices.map(rdd.partitions(_))
    oos.defaultWriteObject()
  }

  /**
   * Computes the fraction of the parents' partitions containing preferredLocation within
   * their getPreferredLocs.
   * @return locality of this coalesced partition between 0 and 1
   *  0-1之间,表示首选的host节点在所有partition的比例
   */
  def localFraction: Double = {
    //计算首选路径在RDD的partition中的比例
    //count函数计算结果为true的数量
    val loc = parents.count { p =>
      val parentPreferredLocations = rdd.context.getPreferredLocs(rdd, p.index).map(_.host) //获取每一个partition的地址集合对应的host
      preferredLocation.exists(parentPreferredLocations.contains)//判断首选的节点host是否在集合里面
    }
    if (parents.size == 0) 0.0 else (loc.toDouble / parents.size.toDouble) //    推荐节点包含数据块的数量/总数据块数量
  }
}

/**
 * Represents a coalesced RDD that has fewer partitions than its parent RDD
 * 代表一个合并RDD,合并后的partition比合并前的要少很多,
 * This class uses the PartitionCoalescer class to find a good partitioning of the parent RDD
 * so that each new partition has roughly the same number of parent partitions and that
 * the preferred location of each new partition overlaps with as many preferred locations of its
 * parent partitions
 * 这个案例被用于 PartitionCoalescer类去找到父RDD中最好的分区过程,新的分区会大体上拥有相同数量的父分区,并且会优先在父分区中最适合的host上创建新的分区
 * @param prev RDD to be coalesced
 * @param maxPartitions number of desired partitions in the coalesced RDD (must be positive)
 * @param balanceSlack used to trade-off balance and locality. 1.0 is all locality, 0 is all balance
 */
private[spark] class CoalescedRDD[T: ClassTag](
    @transient var prev: RDD[T],//需要被合并的RDD
    maxPartitions: Int,//需要合并成多少个partition
    balanceSlack: Double = 0.10)//平衡参数,1更接近本地,0更接近平衡--所谓平衡是老partition分配到新的parititon要平衡,即老的有10个,新的有5个,则保证每一个新的分区都包含2个老的分区.则使用balanceSlack为0
  extends RDD[T](prev.context, Nil) {  // Nil since we implement getDependencies

  require(maxPartitions > 0 || maxPartitions == prev.partitions.length,
    s"Number of partitions ($maxPartitions) must be positive.")

  //重新规划分区,产生新的分区集合
  override def getPartitions: Array[Partition] = {
    val pc = new PartitionCoalescer(maxPartitions, prev, balanceSlack)//为老RDD重新规划成一个新的RDD

    pc.run().zipWithIndex.map {
      case (pg, i) => //i表示第几个分区
        val ids = pg.arr.map(_.index).toArray //获取该新partition对应的老partition的集合
        new CoalescedRDDPartition(i, prev, ids, pg.prefLoc)//创建一个新的分区
    }
  }

  /**
   * 计算父RDD中一组partition集合
   */
  override def compute(partition: Partition, context: TaskContext): Iterator[T] = {
    //每一个partition对应父rdd的多个partition
    partition.asInstanceOf[CoalescedRDDPartition].parents.iterator.flatMap { parentPartition => //循环每一个partition,
      firstParent[T].iterator(parentPartition, context) //读取每一个partition数据,返回迭代器
    }
  }

  override def getDependencies: Seq[Dependency[_]] = {
    Seq(new NarrowDependency(prev) { //NarrowDependency 表示一个子类对应多个父类的RDD分区
      def getParents(id: Int): Seq[Int] =
        partitions(id).asInstanceOf[CoalescedRDDPartition].parentsIndices //说明该partition依赖父RDD中一组partition
    })
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }

  /**
   * Returns the preferred machine for the partition. If split is of type CoalescedRDDPartition,
   * then the preferred machine will be one which most parent splits prefer too.
   * @param partition
   * @return the machine most preferred by split
   * 获取该partition在哪个节点上运行
   */
  override def getPreferredLocations(partition: Partition): Seq[String] = {
    partition.asInstanceOf[CoalescedRDDPartition].preferredLocation.toSeq
  }
}

/**
 * Coalesce the partitions of a parent RDD (`prev`) into fewer partitions, so that each partition of
 * this RDD computes one or more of the parent ones. It will produce exactly `maxPartitions` if the
 * parent had more than maxPartitions, or fewer if the parent had fewer.
 * 合并父RDD的partitions,产生新的分区partition,因此每一个新的partition要去计算多个老的partition,
 *
 * This transformation is useful when an RDD with many partitions gets filtered into a smaller one,
 * or to avoid having a large number of small tasks when processing a directory with many files.
 *
 * If there is no locality information (no preferredLocations) in the parent, then the coalescing
 * is very simple: chunk parents that are close in the Array in chunks.
 * If there is locality information, it proceeds to pack them with the following four goals:
 *
 * (1) Balance the groups so they roughly have the same number of parent partitions 每一个group有尽可能相同数量的父partition
 * (2) Achieve locality per partition, i.e. find one machine which most parent partitions prefer 找到一个机器host,让大多数partition都在该机器上
 * (3) Be efficient, i.e. O(n) algorithm for n parent partitions (problem is likely NP-hard)
 * (4) Balance preferred machines, i.e. avoid as much as possible picking the same preferred machine 分散平衡,避免所有的partition都在同一个机器上
 *
 * Furthermore, it is assumed that the parent RDD may have many partitions, e.g. 100 000.
 * We assume the final number of desired partitions is small, e.g. less than 1000.
 * 因此 这有一个假设,父RDD有很多的partition,最终要被减少为很少的paritition的新的RDD
 *
 * The algorithm tries to assign unique preferred machines to each partition. If the number of
 * desired partitions is greater than the number of preferred machines (can happen), it needs to
 * start picking duplicate preferred machines. This is determined using coupon collector estimation
 * (2n log(n)). The load balancing is done using power-of-two randomized bins-balls with one twist:
 * it tries to also achieve locality. This is done by allowing a slack (balanceSlack) between two
 * bins. If two bins are within the slack in terms of balance, the algorithm will assign partitions
 * according to locality. (contact alig for questions)
 *
 */
private class PartitionCoalescer(maxPartitions: Int, prev: RDD[_], balanceSlack: Double) {

  //按照该分区内父partition的数量做比较
  def compare(o1: PartitionGroup, o2: PartitionGroup): Boolean = o1.size < o2.size
  def compare(o1: Option[PartitionGroup], o2: Option[PartitionGroup]): Boolean =
    if (o1 == None) false else if (o2 == None) true else compare(o1.get, o2.get)

  val rnd = new scala.util.Random(7919) // keep this class deterministic

  // each element of groupArr represents one coalesced partition
  //新的分区集合
  val groupArr = ArrayBuffer[PartitionGroup]()

  // hash used to check whether some machine is already in groupArr
  //每一个host对应在哪些分区组中
  val groupHash = mutable.Map[String, ArrayBuffer[PartitionGroup]]()

  // hash used for the first maxPartitions (to avoid duplicates)
  //说明该partition已经有了partition组了,一旦为父partition分配了新的partition组,则存放在这里面,防止同一个父partition添加到两个组里面
  val initialHash = mutable.Set[Partition]()

  // determines the tradeoff between load-balancing the partitions sizes and their locality
  // e.g. balanceSlack=0.10 means that it allows up to 10% imbalance in favor of locality
  //平滑因子
  val slack = (balanceSlack * prev.partitions.length).toInt

  var noLocality = true  // if true if no preferredLocations exists for parent RDD ,true表示在父RDD中没有设置预先考虑在哪个节点的存在partition,

  // gets the *current* preferred locations from the DAGScheduler (as opposed to the static ones)
  //获取该partition的host集合,必须至少有一个(此处有问题,什么逻辑保证必须有一个的)----逻辑没问题,因为执行这块代码的时候,肯定有推荐节点的情况,即noLocality=false
  def currPrefLocs(part: Partition): Seq[String] = {
    prev.context.getPreferredLocs(prev, part.index).map(tl => tl.host)
  }

  // this class just keeps iterating and rotating infinitely over the partitions of the RDD
  //这个类能够让你保持循环和无限的旋转这个RDD的每一个partition
  // next() returns the next preferred machine that a partition is replicated on
  //next()方法返回下一个机器的首选host,这个host是一个partition被复制的首选节点
  // the rotator first goes through the first replica copy of each partition, then second, third
  //该旋转体首先通过每一个分区的第一个备份  第二个备份 第三个备份
  // the iterators return type is a tuple: (replicaString, partition) 该迭代器返回的对象是一个元组
  //参数是父RDD
  class LocationIterator(prev: RDD[_]) extends Iterator[(String, Partition)] {

    var it: Iterator[(String, Partition)] = resetIterator()

    override val isEmpty = !it.hasNext //true表示空了,不能迭代元素了

    // initializes/resets to start iterating from the beginning
    def resetIterator(): Iterator[(String, Partition)] = {//key是partition所在host,value是partition对象
      /**
       * 0 1 2三个元素组成集合
       * 每一个元素都循环全部partition,如果该数据块对应的host集合大于(0,1,2)之一,则转换成(host,p)组成的元组,其中host来自于集合的下标(0,1,2),否则是None
       * 因此最终得到(host,partition)元组集合
       *
       * scala demo
val d = List("host1","host2","host3")
val iterators = (0 to 2).map( x =>
d.iterator.flatMap(p => {Some( (p, 1) )} )
)
var it:Iterator[(String, Int)] = iterators.reduceLeft((x, y) => x ++ y)
it.foreach(print(_))

可以将flatMap换成map,测试是不通过的,原因就是iterators产生的是Some<String,Int>的迭代器

       即返回值是每一个partition第一个备份host和备份块  然后是第二个备份host和备份块 然后是第三个备份host和备份块
       */
      val iterators = (0 to 2).map( x => //map的结果是(String,Parttaion)的迭代器
        prev.partitions.iterator.flatMap(p => {
          //首先是x=0,表示优先获取第0个推荐的位置,返回<String,Partition>,此时String表示host,即第0个host,就是优先推荐的host
          //第二轮循环的时候变成1,就变成非优先推荐的host了,有可能partiton都没有第二个推荐的host
          if (currPrefLocs(p).size > x) Some((currPrefLocs(p)(x), p)) else None //注意,此时为什么是flatMap,而不是map呢,是因为里面套用的是Some,因此使用循环的时候必须将some去掉,因此用的是flatMap
        } )
      )
      iterators.reduceLeft((x, y) => x ++ y) //x是一个(host,partition)元组,y也是(host,partition)元组,因此最终得到的就是元组集合
    }

    // hasNext() is false iff there are no preferredLocations for any of the partitions of the RDD
    override def hasNext: Boolean = { !isEmpty } //false表示isEmpty为true,即it中没有数据

    // return the next preferredLocation of some partition of the RDD
    override def next(): (String, Partition) = {
      if (it.hasNext) {
        it.next()
      } else {
        it = resetIterator() // ran out of preferred locations, reset and rotate to the beginning
        it.next()
      }
    }
  }

  /**
   * Sorts and gets the least element of the list associated with key in groupHash
   * The returned PartitionGroup is the least loaded of all groups that represent the machine "key"
   * @param key string representing a partitioned group on preferred machine key
   * @return Option of PartitionGroup that has least elements for key
   */
  def getLeastGroupHash(key: String): Option[PartitionGroup] = {
    groupHash.get(key).map(_.sortWith(compare).head)//获取该host对应的分区组中,分区最少的一个就是该host对应的分区
  }

  //添加这个partition到这个组里面
  def addPartToPGroup(part: Partition, pgroup: PartitionGroup): Boolean = {
    if (!initialHash.contains(part)) {
      pgroup.arr += part           // already assign this element 添加一个partition
      initialHash += part // needed to avoid assigning partitions to multiple buckets 说明该partition已经有了partition组了
      true
    } else { false }
  }

  /**
   * Initializes targetLen partition groups and assigns a preferredLocation
   * This uses coupon collector to estimate how many preferredLocations it must rotate through
   * until it has seen most of the preferred locations (2 * n log(n))
   * @param targetLen 创建新的RDD有多少个分区,每一个分区分配一个PartitionGroup对象
   */
  def setupGroups(targetLen: Int) {
    val rotIt = new LocationIterator(prev)

    // deal with empty case, just create targetLen partition groups with no preferred location
    //为了处理空的情况,仅仅创建若干个分区即可,不需要host
    if (!rotIt.hasNext) {
      (1 to targetLen).foreach(x => groupArr += PartitionGroup()) //为每一个分区创建一个PartitionGroup对象
      return
    }

    noLocality = false

    // number of iterations needed to be certain that we've seen most preferred locations
   //需要多少次迭代,我们可以确定可能好到最可能的首选地址
    //math.log 表示e 2.7的运算
    val expectedCoupons2 = 2 * (math.log(targetLen)*targetLen + targetLen + 0.5).toInt //期望次数
    var numCreated = 0 //已经分配多少个partition了
    var tries = 0 //已经尝试的次数,不能比expectedCoupons2期望的大

    // rotate through until either targetLen unique/distinct preferred locations have been created
    // OR we've rotated expectedCoupons2, in which case we have likely seen all preferred locations,
    // i.e. likely targetLen >> number of preferred locations (more buckets than there are machines)
    //此时初步分配组,因此一个host只能分一个组
    while (numCreated < targetLen && tries < expectedCoupons2) {//只要没有分配完partition,并且尝试次数还没有结束,就一直循环
      tries += 1 //尝试次数+1
      val (nxt_replica, nxt_part) = rotIt.next()//返回partition所在host和partition本身
      if (!groupHash.contains(nxt_replica)) {//查看该host是否已经分配了分区组,如果没分配则进行分配
        val pgroup = PartitionGroup(nxt_replica)//在该host上设立分区组
        groupArr += pgroup//添加一个新的组
        addPartToPGroup(nxt_part, pgroup) //添加这个partition到这个组里面
        groupHash.put(nxt_replica, ArrayBuffer(pgroup)) // list in case we have multiple 让host上产生一个该用户组
        numCreated += 1 //已经分配了一个partition
      }
    }

    //说明分组还没有分成功---一个host可以分配多个分组
    while (numCreated < targetLen) {  // if we don't have enough partition groups, create duplicates
      var (nxt_replica, nxt_part) = rotIt.next() //继续循环,不保证该partition已经分配成功了
      val pgroup = PartitionGroup(nxt_replica)
      groupArr += pgroup
      groupHash.getOrElseUpdate(nxt_replica, ArrayBuffer()) += pgroup //向该host上可以添加多个partition组
      var tries = 0
      while (!addPartToPGroup(nxt_part, pgroup) && tries < targetLen) { // ensure at least one part 确保一个partition,因为可能该partition已经分配成功了
        nxt_part = rotIt.next()._2 //不断循环
        tries += 1
      }
      numCreated += 1
    }

  }

  /**
   * Takes a parent RDD partition and decides which of the partition groups to put it in
   * 拿到父RDD的某一个partition,去决定存储到哪个新的partition组里面
   * Takes locality into account, but also uses power of 2 choices to load balance
   * It strikes a balance between the two use the balanceSlack variable
   * @param p partition (ball to be thrown)
   * @return partition group (bin to be put in)
   */
  def pickBin(p: Partition): PartitionGroup = {
    val pref = currPrefLocs(p).map(getLeastGroupHash(_)).sortWith(compare) // least loaded pref locs 拿到该partition的host集合 --- 找到该host存储多个组内元素最少的一个组---按照partition数量排序
    val prefPart = if (pref == Nil) None else pref.head //找到最小数量的partition组 或者None(表示在该数据块对应的这些host上,没有分组的host)

    val r1 = rnd.nextInt(groupArr.size) //在groupArr.size以内随机产生一个数字
    val r2 = rnd.nextInt(groupArr.size)
    val minPowerOfTwo = if (groupArr(r1).size < groupArr(r2).size) groupArr(r1) else groupArr(r2) //随机找两个分组,获取两者中小的分组
    if (prefPart.isEmpty) { //说明该partition所在节点上是没有分组存在的
      // if no preferred locations, just use basic power of two 返回随机的最小的分组即可
      return minPowerOfTwo
    }

    //以下说明该partition所在节点上是有分组存在的
    val prefPartActual = prefPart.get //确定的分组host

    if (minPowerOfTwo.size + slack <= prefPartActual.size) { // more imbalance than the slack allows,说明确定的分组host上已经存在的partition已经超过伐值了,还是选择给数量较小的分组存储
      minPowerOfTwo  // prefer balance over locality 为就负载均衡 返回较小的
    } else {
      prefPartActual // prefer locality over balance 返回当前本地节点的
    }
  }

  def throwBalls() {
    if (noLocality) {  // no preferredLocations in parent RDD, no randomization needed 在父RDD中没有设置预先考虑在哪个节点的存在partition,
      if (maxPartitions > groupArr.size) { // just return prev.partitions
        for ((p, i) <- prev.partitions.zipWithIndex) {//基本不会再这里面执行,因为这里面说明新的分区比老得还多,那么就把老的分区依次填充新分区的相同位置即可
          groupArr(i).arr += p //一比一对应关系添加即可
        }
      } else { // no locality available, then simply split partitions based on positions in array
        //很简单的方式,将按照每隔多少个父partition,分配给一个新的partition目标设置
        for (i <- 0 until maxPartitions) {
          val rangeStart = ((i.toLong * prev.partitions.length) / maxPartitions).toInt
          val rangeEnd = (((i.toLong + 1) * prev.partitions.length) / maxPartitions).toInt
          (rangeStart until rangeEnd).foreach{ j => groupArr(i).arr += prev.partitions(j) }
        }
      }
    } else {//考虑父RDD的每一个分片的位置情况
      for (p <- prev.partitions if (!initialHash.contains(p))) { // throw every partition into group 循环每一个父RDD的分区,为该分区没有分组的进行分组
        //pickBin表示为该partition找到符合的组
        pickBin(p).arr += p //将没有分配的partition进行分配
      }
    }
  }

  //获取全部新的partition集合
  def getPartitions: Array[PartitionGroup] = groupArr.filter( pg => pg.size > 0).toArray

  /**
   * Runs the packing algorithm and returns an array of PartitionGroups that if possible are
   * load balanced and grouped by locality
   * @return array of partition groups
   */
  def run(): Array[PartitionGroup] = {
    setupGroups(math.min(prev.partitions.length, maxPartitions))   // setup the groups (bins) 最终产生多少个partition分区
    throwBalls() // assign partitions (balls) to each group (bins)
    getPartitions
  }
}

//新的partition包含哪些老的partition集合
private case class PartitionGroup(prefLoc: Option[String] = None) {//新的partition建议在哪个host上执行
  var arr = mutable.ArrayBuffer[Partition]()//包含的老的partition集合
  def size: Int = arr.size
}

private object PartitionGroup {
  def apply(prefLoc: String): PartitionGroup = {
    require(prefLoc != "", "Preferred location must not be empty") //建议在哪个host上执行新的partition,这个参数必须存在
    PartitionGroup(Some(prefLoc))
  }
}
