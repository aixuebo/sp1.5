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

package org.apache.spark.storage

import java.io.{IOException, File, FileOutputStream, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel.MapMode

import org.apache.spark.Logging
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.Utils

/**
 * Stores BlockManager blocks on disk.
 * 将数据块信息存储到磁盘上
 */
private[spark] class DiskStore(blockManager: BlockManager, diskManager: DiskBlockManager)
  extends BlockStore(blockManager) with Logging {

  //小文件则直接读取到内存中
  val minMemoryMapBytes = blockManager.conf.getSizeAsBytes("spark.storage.memoryMapThreshold", "2m")

  //获取文件大小
  override def getSize(blockId: BlockId): Long = {
    diskManager.getFile(blockId.name).length//找到该数据块的对应的文件,获取文件大小
  }

  //将_bytes内容写入BlockId对应的文件中
  override def putBytes(blockId: BlockId, _bytes: ByteBuffer, level: StorageLevel): PutResult = {
    // So that we do not modify the input offsets !
    // duplicate does not copy buffer, so inexpensive
    val bytes = _bytes.duplicate()//不是真正的copy,但是也算是复制一份
    logDebug(s"Attempting to put block $blockId")
    val startTime = System.currentTimeMillis
    val file = diskManager.getFile(blockId) //获取该数据块要存储的File对象
    val channel = new FileOutputStream(file).getChannel //对File对象进行写入数据
    Utils.tryWithSafeFinally {
      while (bytes.remaining > 0) {
        channel.write(bytes)
      }
    } {
      channel.close()
    }
    val finishTime = System.currentTimeMillis
    logDebug("Block %s stored as %s file on disk in %d ms".format(
      file.getName, Utils.bytesToString(bytes.limit), finishTime - startTime))
    PutResult(bytes.limit(), Right(bytes.duplicate()))
  }

  override def putArray(
      blockId: BlockId,
      values: Array[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {
    putIterator(blockId, values.toIterator, level, returnValues)
  }

  //将一组数据写入到BlockId对应的文件中
  override def putIterator(
      blockId: BlockId,
      values: Iterator[Any],
      level: StorageLevel,
      returnValues: Boolean) //是否需要返回存储的内容buffer
      : PutResult = {

    logDebug(s"Attempting to write values for block $blockId")
    val startTime = System.currentTimeMillis
    val file = diskManager.getFile(blockId)//获取该数据块要存储在什么位置上
    val outputStream = new FileOutputStream(file)//打开该文件的输出流
    try {
      Utils.tryWithSafeFinally {
        blockManager.dataSerializeStream(blockId, outputStream, values)//将value写入到输出流中,使用默认的序列化对象写入
      } {
        // Close outputStream here because it should be closed before file is deleted.
        outputStream.close()
      }
    } catch {
      //如果出现异常,则将文件删除,并且抛异常
      case e: Throwable =>
        if (file.exists()) {
          file.delete()
        }
        throw e
    }

    //说明文件已经写入成功,计算文件写入多少个字节
    val length = file.length

    //计算写入过程消耗的时间
    val timeTaken = System.currentTimeMillis - startTime

    //记录日志,存储数据块XXX,一共存储多少个字节,花费多少时间
    logDebug("Block %s stored as %s file on disk in %d ms".format(
      file.getName, Utils.bytesToString(length), timeTaken))

    if (returnValues) {//创建返回值对象
      // Return a byte buffer for the contents of the file
      val buffer = getBytes(blockId).get //获取该存储文件的buffer对象
      PutResult(length, Right(buffer))//Right(buffer)表示buffer对象存储的文件内容
    } else {
      PutResult(length, null)
    }
  }

  //返回file中指定的一段内容,返回的buffer对象就是可以读取的信息对象,即已经将文件的内容存储到buffer返回值中了
  private def getBytes(file: File, offset: Long, length: Long): Option[ByteBuffer] = {
    val channel = new RandomAccessFile(file, "r").getChannel
    Utils.tryWithSafeFinally {
      // For small files, directly read rather than memory map 如果是小文件,直接读取到内存中
      if (length < minMemoryMapBytes) {
        val buf = ByteBuffer.allocate(length.toInt)
        channel.position(offset)//移动渠道到读取的开始位置
        while (buf.remaining() != 0) {//如果buffer缓冲区还有地方存储数据
          if (channel.read(buf) == -1) {//渠道信息读取到buffer中
            throw new IOException("Reached EOF before filling buffer\n" +
              s"offset=$offset\nfile=${file.getAbsolutePath}\nbuf.remaining=${buf.remaining}")
          }
        }
        buf.flip()//写模式切换到读模式
        Some(buf)//返回可以读的buffer对象
      } else {//readOnly的方式内存映射该文件的一段内容
        Some(channel.map(MapMode.READ_ONLY, offset, length))
      }
    } {
      channel.close()
    }
  }

  //读取blockId对应的整个文件
  override def getBytes(blockId: BlockId): Option[ByteBuffer] = {
    val file = diskManager.getFile(blockId.name)
    getBytes(file, 0, file.length)
  }

  //读取文件的一个小片段
  def getBytes(segment: FileSegment): Option[ByteBuffer] = {
    getBytes(segment.file, segment.offset, segment.length)
  }

  //将blockId对应的文件内容bytes,进行反序列化成一组数据
  override def getValues(blockId: BlockId): Option[Iterator[Any]] = {
    getBytes(blockId).map(buffer => blockManager.dataDeserialize(blockId, buffer))
  }

  /**
   * A version of getValues that allows a custom serializer. This is used as part of the
   * shuffle short-circuit code.
   * 将blockId对应的文件内容bytes,进行反序列化成一组数据,使用自定义的序列化工具
   */
  def getValues(blockId: BlockId, serializer: Serializer): Option[Iterator[Any]] = {
    // TODO: Should bypass getBytes and use a stream based implementation, so that
    // we won't use a lot of memory during e.g. external sort merge.
    getBytes(blockId).map(bytes => blockManager.dataDeserialize(blockId, bytes, serializer))
  }

  //删除BlockId对应的物理文件
  override def remove(blockId: BlockId): Boolean = {
    val file = diskManager.getFile(blockId.name)//找到该数据块对应的文件
    // If consolidation mode is used With HashShuffleMananger, the physical filename for the block
    // is different from blockId.name. So the file returns here will not be exist, thus we avoid to
    // delete the whole consolidated file by mistake.
    //删除该文件
    if (file.exists()) {
      file.delete()
    } else {
      false
    }
  }

  //判断BlockId数据块是否存在
  override def contains(blockId: BlockId): Boolean = {
    val file = diskManager.getFile(blockId.name)
    file.exists()
  }
}
