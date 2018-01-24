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

import java.nio.ByteBuffer

/**
 * Result of adding a block into a BlockStore. This case class contains a few things:
 * 结果是添加数据块到存储器之后的结果,主要有三个时期
 *   (1) The estimated size of the put,估算put存放的数据块总大小
 *   (2) The values put if the caller asked for them to be returned (e.g. for chaining
 *       replication), and 返回的集体内容,是具体文件内容 或者是迭代器内容
 *   (3) A list of blocks dropped as a result of this put. This is always empty for DiskStore.
 */
private[spark] case class PutResult(
    size: Long,//文件大小(反序列化后的大小)
    data: Either[Iterator[_], ByteBuffer],//文件的具体内容,要么是迭代器,要么是具体的内容组成的ByteBuffer数组
    droppedBlocks: Seq[(BlockId, BlockStatus)] = Seq.empty)//数据块ID和该数据块状态组成的集合,这些是在内存管理存储的时候,内存不足时,删除的一些数据块映射集合---该属性只用于内存存储对象中
