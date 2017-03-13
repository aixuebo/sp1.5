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

package org.apache.spark.sql.columnar.compression

import java.nio.ByteBuffer

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.runtimeMirror
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{MutableRow, SpecificMutableRow}
import org.apache.spark.sql.columnar._
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

//全体通过PassThrough的类型是0   RunLengthEncoding类型是1   DictionaryEncoding类型是2  BooleanBitSet类型是3  IntDelta类型是4   LongDelta类型是5

//全体通过
private[sql] case object PassThrough extends CompressionScheme {
  override val typeId = 0

  override def supports(columnType: ColumnType[_]): Boolean = true //全部属性都支持

  override def encoder[T <: AtomicType](columnType: NativeColumnType[T]): Encoder[T] = {
    new this.Encoder[T](columnType)
  }

  override def decoder[T <: AtomicType](
      buffer: ByteBuffer, columnType: NativeColumnType[T]): Decoder[T] = {
    new this.Decoder(buffer, columnType)
  }

  class Encoder[T <: AtomicType](columnType: NativeColumnType[T]) extends compression.Encoder[T] {
    override def uncompressedSize: Int = 0

    override def compressedSize: Int = 0

    override def compress(from: ByteBuffer, to: ByteBuffer): ByteBuffer = {
      // Writes compression type ID and copies raw contents
      to.putInt(PassThrough.typeId).put(from).rewind()
      to
    }
  }

  class Decoder[T <: AtomicType](buffer: ByteBuffer, columnType: NativeColumnType[T])
    extends compression.Decoder[T] {

    override def next(row: MutableRow, ordinal: Int): Unit = {
      columnType.extract(buffer, row, ordinal)
    }

    override def hasNext: Boolean = buffer.hasRemaining
  }
}

//对不断连续重复出现的数据,只是记录一次该值,以及int类型该值出现多少次即可起到压缩的作用
private[sql] case object RunLengthEncoding extends CompressionScheme {
  override val typeId = 1

  override def encoder[T <: AtomicType](columnType: NativeColumnType[T]): Encoder[T] = {
    new this.Encoder[T](columnType)
  }

  //对buffer的数据进行解压缩.即反序列化,buffer里面存储的每一个数据类型是T
  override def decoder[T <: AtomicType](
      buffer: ByteBuffer, columnType: NativeColumnType[T]): Decoder[T] = {
    new this.Decoder(buffer, columnType)
  }

  override def supports(columnType: ColumnType[_]): Boolean = columnType match {
    case INT | LONG | SHORT | BYTE | STRING | BOOLEAN => true
    case _ => false
  }

  class Encoder[T <: AtomicType](columnType: NativeColumnType[T]) extends compression.Encoder[T] {
    private var _uncompressedSize = 0 //未压缩前的字节大小
    private var _compressedSize = 0 //压缩后的字节大小

    // Using `MutableRow` to store the last value to avoid boxing/unboxing cost.
    private val lastValue = new SpecificMutableRow(Seq(columnType.dataType)) //上一次存储的该属性值,因为该row就一列,因此获取该值的时候是第0个位置
    private var lastRun = 0 //该值连续的出现了多少次

    override def uncompressedSize: Int = _uncompressedSize

    override def compressedSize: Int = _compressedSize

    //收集压缩统计信息
    override def gatherCompressibilityStats(row: InternalRow, ordinal: Int): Unit = {
      val value = columnType.getField(row, ordinal) //获取该row对应的ordinal元素值
      val actualSize = columnType.actualSize(row, ordinal) //计算该元素值真实的字节大小
      _uncompressedSize += actualSize //增加未压缩前的字节大小

      if (lastValue.isNullAt(0)) {//上一个值是null
        columnType.copyField(row, ordinal, lastValue, 0) //因为是新的值.因此要重新从row的第ordinal位置把值获取出来,赋予lastValue这个row的第0个位置
        lastRun = 1
        _compressedSize += actualSize + 4
      } else {//上一个值是非null
        if (columnType.getField(lastValue, 0) == value) { //获取上一个值,跟此时值对比.是否相同
          lastRun += 1 //记录有多少个先通过的值出现
        } else { //说明值不相同
          _compressedSize += actualSize + 4 //压缩后的大小就是真实值+4(表示该值出现了多少次)
          columnType.copyField(row, ordinal, lastValue, 0) //因为是新的值.因此要重新从row的第ordinal位置把值获取出来,赋予lastValue这个row的第0个位置
          lastRun = 1 //因为新的值已经出现因此了,则立刻设置为1
        }
      }
    }

    //循环from,然后将数据写入到to中,每一个不同的属性值,存储到to的格式是数据内容+4个字节的出现次数
    override def compress(from: ByteBuffer, to: ByteBuffer): ByteBuffer = {
      to.putInt(RunLengthEncoding.typeId)

      if (from.hasRemaining) {
        val currentValue = new SpecificMutableRow(Seq(columnType.dataType)) //根据当前类的属性类型,创建一个可变的row对象,用于设置当前最后一个数据的内容
        var currentRun = 1 //连续重复出现多少次
        val value = new SpecificMutableRow(Seq(columnType.dataType)) //根据当前类的属性类型,创建一个可变的row对象,用于不断的循环从from中获取数据

        columnType.extract(from, currentValue, 0) //从from中抽取一个数据,存储到currentValue这个row行第0列中

        while (from.hasRemaining) {//不断抽取from的数据
          columnType.extract(from, value, 0) //不断的从from中抽取数据,存储到value这row的第0个列中

          if (value.get(0, columnType.dataType) == currentValue.get(0, columnType.dataType)) {//说明value和当前last最后一个数据内容currentValue的内容一样
            currentRun += 1 //连续重复出现多少次 +1
          } else {//说明此时不一样
            // Writes current run
            columnType.append(currentValue, 0, to)//将last的数据内容添加到to中
            to.putInt(currentRun) //添加出现次数

            // Resets current run
            columnType.copyField(value, 0, currentValue, 0) //重新设置last的数据内容currentValue为此时的value
            currentRun = 1 //出现次数设置为1
          }
        }

        //最后插入到to中数据内容和出现次数
        // Writes the last run
        columnType.append(currentValue, 0, to)
        to.putInt(currentRun)
      }

      to.rewind()
      to
    }
  }

  class Decoder[T <: AtomicType](buffer: ByteBuffer, columnType: NativeColumnType[T])
    extends compression.Decoder[T] {

    private var run = 0 //当前值重复出现多少次
    private var valueCount = 0 //已经消费了多少次
    private var currentValue: T#InternalType = _ //当前的数据值

    //为row的第ordinal属性设置值
    override def next(row: MutableRow, ordinal: Int): Unit = {
      if (valueCount == run) {//说明已经消费完了,要抽取新的数据了
        currentValue = columnType.extract(buffer) //从buffer中获取数据值
        run = buffer.getInt() //重复出现多少次
        valueCount = 1
      } else {//说明数据没消费完.累加1
        valueCount += 1
      }

      columnType.setField(row, ordinal, currentValue) //将当前currentValue值设置到row的第ordinal属性中
    }

    override def hasNext: Boolean = valueCount < run || buffer.hasRemaining //还有没消费完的重复数据  or  buffer还有数据都表示还有数据存在
  }
}

//value值作为key,为每一个value赋予唯一的一个ID,因此存储的时候存储ID以及映射关系即可压缩
private[sql] case object DictionaryEncoding extends CompressionScheme {
  override val typeId = 2

  // 32K unique values allowed
  val MAX_DICT_SIZE = Short.MaxValue //map目录中key允许的最大不同值,即不同的属性值最多不允许超过该值

  override def supports(columnType: ColumnType[_]): Boolean = columnType match {
    case INT | LONG | STRING => true
    case _ => false
  }

  override def encoder[T <: AtomicType](columnType: NativeColumnType[T]): Encoder[T] = {
    new this.Encoder[T](columnType)
  }

  override def decoder[T <: AtomicType](buffer: ByteBuffer, columnType: NativeColumnType[T])
  : Decoder[T] = {
    new this.Decoder(buffer, columnType)
  }

  class Encoder[T <: AtomicType](columnType: NativeColumnType[T]) extends compression.Encoder[T] {
    // Size of the input, uncompressed, in bytes. Note that we only count until the dictionary
    // overflows.
    private var _uncompressedSize = 0 //为压缩的字节数

    // If the number of distinct elements is too large, we discard the use of dictionary encoding
    // and set the overflow flag to true.
    //如果不同元素的数量太大了,我们放弃使用这个方式,并且设置溢出为true
    private var overflow = false //是否溢出,默认是false,true表示不能使用该算法了

    // Total number of elements.一共多少个属性元素
    private var count = 0

    // The reverse mapping of _dictionary, i.e. mapping encoded integer to the value itself.
    //存储所有的不同的属性值
    private var values = new mutable.ArrayBuffer[T#InternalType](1024)

    // The dictionary that maps a value to the encoded short integer.
    //每一个属性值对应的一个编号
    private val dictionary = mutable.HashMap.empty[Any, Short]

    // Size of the serialized dictionary in bytes. Initialized to 4 since we need at least an `Int`
    // to store dictionary element count.
    private var dictionarySize = 4 //存储真实的value的所有字节数

    override def gatherCompressibilityStats(row: InternalRow, ordinal: Int): Unit = {
      val value = columnType.getField(row, ordinal) //获取该row的第ordinal属性对应的属性值

      if (!overflow) {
        val actualSize = columnType.actualSize(row, ordinal) //计算该值所占用的字节
        count += 1
        _uncompressedSize += actualSize //未压缩的字节

        if (!dictionary.contains(value)) {//查看该map中是否存在该value,说明此时不存在
          if (dictionary.size < MAX_DICT_SIZE) {//还可以继续产生数据
            val clone = columnType.clone(value) //复制该value值
            values += clone //添加所有不同的value到ArrayBuffer集合中
            dictionarySize += actualSize //存储真实的value的所有字节数
            dictionary(clone) = dictionary.size.toShort //设置属性值的编号就是此时最大的size
          } else {//说明map不同的属性值数量超过限制了
            overflow = true
            //清理内存
            values.clear()
            dictionary.clear()
          }
        }
      }
    }

    override def compress(from: ByteBuffer, to: ByteBuffer): ByteBuffer = {
      if (overflow) {//说明不能使用该算法进行压缩
        throw new IllegalStateException(
          "Dictionary encoding should not be used because of dictionary overflow.")
      }

      to.putInt(DictionaryEncoding.typeId)
        .putInt(dictionary.size) //输入前两个int,分别表示存储的数据类型以及有多少个不同的属性值,存储多少个的目的是为了从数据buffer中还原每一个value,并且存储到映射关系中

      //存储所有的value值,每一个value其实是有一个id的,就是序号即可,因此可以忽略
      var i = 0
      while (i < values.length) {
        columnType.append(values(i), to)
        i += 1
      }

      while (from.hasRemaining) {//不断的抽取from中的数据
        to.putShort(dictionary(columnType.extract(from)))//存储to中的是该属性值对应的序号
      }

      to.rewind()
      to
    }

    override def uncompressedSize: Int = _uncompressedSize //未压缩前数据字节数

    override def compressedSize: Int = if (overflow) Int.MaxValue else dictionarySize + count * 2 //如果没有异常的话,则为不同value的字节总数+ 一共多少个元素,每一个元素使用short2个字节存储.因此是count*2
  }

  class Decoder[T <: AtomicType](buffer: ByteBuffer, columnType: NativeColumnType[T])
    extends compression.Decoder[T] {

    //因为是val,因此第一次调用的时候就会发生,后续都不会在调用了
    private val dictionary: Array[Any] = {
      val elementNum = buffer.getInt() //从buffer中获取对应的value的个数
      Array.fill[Any](elementNum)(columnType.extract(buffer).asInstanceOf[Any])//循环elementNum次,从中还原value信息
    }

    //为row的第ordinal个元素值,值为从dictionary中获取第value序号的位置值
    override def next(row: MutableRow, ordinal: Int): Unit = {
      columnType.setField(row, ordinal, dictionary(buffer.getShort()).asInstanceOf[T#InternalType])
    }

    override def hasNext: Boolean = buffer.hasRemaining
  }
}

//压缩boolean类型的值---将64个值转换成一个long值从而进行压缩
private[sql] case object BooleanBitSet extends CompressionScheme {
  override val typeId = 3

  val BITS_PER_LONG = 64 //一个long需要的bit位

  override def decoder[T <: AtomicType](buffer: ByteBuffer, columnType: NativeColumnType[T])
    : compression.Decoder[T] = {
    new this.Decoder(buffer).asInstanceOf[compression.Decoder[T]]
  }

  override def encoder[T <: AtomicType](columnType: NativeColumnType[T]): compression.Encoder[T] = {
    (new this.Encoder).asInstanceOf[compression.Encoder[T]]
  }

  override def supports(columnType: ColumnType[_]): Boolean = columnType == BOOLEAN //存储boolean类型的值

  class Encoder extends compression.Encoder[BooleanType.type] {
    private var _uncompressedSize = 0

    override def gatherCompressibilityStats(row: InternalRow, ordinal: Int): Unit = {
      _uncompressedSize += BOOLEAN.defaultSize //计算未压缩的字节数,因为每一个位置都是boolean,因此字节大小相同
    }

    override def compress(from: ByteBuffer, to: ByteBuffer): ByteBuffer = {
      to.putInt(BooleanBitSet.typeId)
        // Total element count (1 byte per Boolean value)
        .putInt(from.remaining) //输出有多少个boolean值

      while (from.remaining >= BITS_PER_LONG) { //说明总数比BITS_PER_LONG大
        var word = 0: Long //强制为long类型
        var i = 0

        while (i < BITS_PER_LONG) {//循环前BITS_PER_LONG个元素,即每隔64个value值进行输出一个long值
          if (BOOLEAN.extract(from)) {//抽取每一个boolean
            word |= (1: Long) << i //转换成一个long值
          }
          i += 1
        }

        to.putLong(word) //存储long值
      }

      //输出剩余的值.该值的数量<64个,则产生一个最终的long
      if (from.hasRemaining) {//如果还有数据
        var word = 0: Long
        var i = 0

        while (from.hasRemaining) {
          if (BOOLEAN.extract(from)) {//不断提取数据
            word |= (1: Long) << i
          }
          i += 1
        }

        to.putLong(word)
      }

      to.rewind()
      to
    }

    override def uncompressedSize: Int = _uncompressedSize

    //压缩后的大小为总字节/64
    override def compressedSize: Int = {
      val extra = if (_uncompressedSize % BITS_PER_LONG == 0) 0 else 1 //是否整除64.如果不是则多了一个long值,因此是1
      (_uncompressedSize / BITS_PER_LONG + extra) * 8 + 4 //每一个long是8个字节,因此*8,后面的4表示一个int值,表示输出有多少个boolean值
    }
  }

  class Decoder(buffer: ByteBuffer) extends compression.Decoder[BooleanType.type] {

    private val count = buffer.getInt() //获取有多少个long值

    private var currentWord = 0: Long //一个具体的long值

    private var visited: Int = 0 //在一个long值内访问到第一个位置了

    override def next(row: MutableRow, ordinal: Int): Unit = {
      val bit = visited % BITS_PER_LONG //是否访问完一个long的位数了

      visited += 1
      if (bit == 0) {
        currentWord = buffer.getLong() //不断获取下一个long值
      }

      row.setBoolean(ordinal, ((currentWord >> bit) & 1) != 0) //获取对应的boolean值
    }

    override def hasNext: Boolean = visited < count
  }
}

//参考后面的LongDelta类
private[sql] case object IntDelta extends CompressionScheme {
  override def typeId: Int = 4

  override def decoder[T <: AtomicType](buffer: ByteBuffer, columnType: NativeColumnType[T])
    : compression.Decoder[T] = {
    new Decoder(buffer, INT).asInstanceOf[compression.Decoder[T]]
  }

  override def encoder[T <: AtomicType](columnType: NativeColumnType[T]): compression.Encoder[T] = {
    (new Encoder).asInstanceOf[compression.Encoder[T]]
  }

  override def supports(columnType: ColumnType[_]): Boolean = columnType == INT

  class Encoder extends compression.Encoder[IntegerType.type] {
    protected var _compressedSize: Int = 0
    protected var _uncompressedSize: Int = 0

    override def compressedSize: Int = _compressedSize
    override def uncompressedSize: Int = _uncompressedSize

    private var prevValue: Int = _

    override def gatherCompressibilityStats(row: InternalRow, ordinal: Int): Unit = {
      val value = row.getInt(ordinal)
      val delta = value - prevValue

      _compressedSize += 1

      // If this is the first integer to be compressed, or the delta is out of byte range, then give
      // up compressing this integer.
      if (_uncompressedSize == 0 || delta <= Byte.MinValue || delta > Byte.MaxValue) {
        _compressedSize += INT.defaultSize
      }

      _uncompressedSize += INT.defaultSize
      prevValue = value
    }

    override def compress(from: ByteBuffer, to: ByteBuffer): ByteBuffer = {
      to.putInt(typeId)

      if (from.hasRemaining) {
        var prev = from.getInt()
        to.put(Byte.MinValue)
        to.putInt(prev)

        while (from.hasRemaining) {
          val current = from.getInt()
          val delta = current - prev
          prev = current

          if (Byte.MinValue < delta && delta <= Byte.MaxValue) {
            to.put(delta.toByte)
          } else {
            to.put(Byte.MinValue)
            to.putInt(current)
          }
        }
      }

      to.rewind().asInstanceOf[ByteBuffer]
    }
  }

  class Decoder(buffer: ByteBuffer, columnType: NativeColumnType[IntegerType.type])
    extends compression.Decoder[IntegerType.type] {

    private var prev: Int = _

    override def hasNext: Boolean = buffer.hasRemaining

    override def next(row: MutableRow, ordinal: Int): Unit = {
      val delta = buffer.get()
      prev = if (delta > Byte.MinValue) prev + delta else buffer.getInt()
      row.setInt(ordinal, prev)
    }
  }
}

//计算两个long类型的差,如果在byte里面,则用一个字节存储,如果超过byte范围,则存储long本身
private[sql] case object LongDelta extends CompressionScheme {
  override def typeId: Int = 5

  override def decoder[T <: AtomicType](buffer: ByteBuffer, columnType: NativeColumnType[T])
    : compression.Decoder[T] = {
    new Decoder(buffer, LONG).asInstanceOf[compression.Decoder[T]]
  }

  override def encoder[T <: AtomicType](columnType: NativeColumnType[T]): compression.Encoder[T] = {
    (new Encoder).asInstanceOf[compression.Encoder[T]]
  }

  override def supports(columnType: ColumnType[_]): Boolean = columnType == LONG //只支持long类型

  class Encoder extends compression.Encoder[LongType.type] {
    protected var _compressedSize: Int = 0
    protected var _uncompressedSize: Int = 0

    override def compressedSize: Int = _compressedSize
    override def uncompressedSize: Int = _uncompressedSize

    private var prevValue: Long = _ //上一个long值

    override def gatherCompressibilityStats(row: InternalRow, ordinal: Int): Unit = {
      val value = row.getLong(ordinal)//获取该row行对应的ordinal位置元素值为long
      val delta = value - prevValue //获取差值

      _compressedSize += 1 //压缩值只是增加1,该值是存储差的位置

      // If this is the first long integer to be compressed, or the delta is out of byte range, then
      // give up compressing this long integer.
      //如果第一次long值被压缩, or  差值delta超出byte范围了,则设置压缩大小就是long本身的大小
      if (_uncompressedSize == 0 || delta <= Byte.MinValue || delta > Byte.MaxValue) {
        _compressedSize += LONG.defaultSize //压缩值追加本身,在范围内是不需要追加本身的,因此不需要_compressedSize改变
      }

      _uncompressedSize += LONG.defaultSize //未压缩的值
      prevValue = value
    }

    override def compress(from: ByteBuffer, to: ByteBuffer): ByteBuffer = {
      to.putInt(typeId) //写入数据类型

      if (from.hasRemaining) {
        var prev = from.getLong() //设置第一个值
        to.put(Byte.MinValue) //第一个值的偏移量为最小值
        to.putLong(prev) //设置第一个值

        while (from.hasRemaining) {
          val current = from.getLong() //获取下一个值
          val delta = current - prev //计算差
          prev = current //切换当前值为prev

          if (Byte.MinValue < delta && delta <= Byte.MaxValue) {//范围内,设置一个字节的delta
            //设置1个字节
            to.put(delta.toByte)
          } else {//超出范围了,设置当前值本身
            //设置9个字节
            to.put(Byte.MinValue)
            to.putLong(current)
          }
        }
      }

      to.rewind().asInstanceOf[ByteBuffer]
    }
  }

  class Decoder(buffer: ByteBuffer, columnType: NativeColumnType[LongType.type])
    extends compression.Decoder[LongType.type] {

    private var prev: Long = _

    override def hasNext: Boolean = buffer.hasRemaining

    override def next(row: MutableRow, ordinal: Int): Unit = {
      val delta = buffer.get() //获取变化差
      prev = if (delta > Byte.MinValue) prev + delta else buffer.getLong() //前面的值+变化差  或者 当前值
      row.setLong(ordinal, prev) //设置long值为row的第ordinal个属性
    }
  }
}
