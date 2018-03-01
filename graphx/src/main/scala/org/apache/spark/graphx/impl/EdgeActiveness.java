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

package org.apache.spark.graphx.impl;

/**
 * Criteria for filtering edges based on activeness. For internal use only.
 * 边的顶点活跃情况状态
 */
public enum EdgeActiveness {
  /** Neither the source vertex nor the destination vertex need be active.*/
  Neither,//表示都不需要和活跃有关系   比如计算出入度的时候,每一个节点计算一次就可以,不需要再迭代操作了
  /** The source vertex must be active. */
  SrcOnly,//只有src是活跃的
  /** The destination vertex must be active. */
  DstOnly,//只有dst是活跃的
  /** Both vertices must be active. */
  Both,//src和dsc必须都是活跃的
  /** At least one vertex must be active. */
  Either//src和dsc至少有一个是活跃的
}
