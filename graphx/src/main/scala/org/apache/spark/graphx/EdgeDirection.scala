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

package org.apache.spark.graphx

/**
 * The direction of a directed edge relative to a vertex.
 */
class EdgeDirection private (private val name: String) extends Serializable {
  /**
   * Reverse the direction of an edge.  An in becomes out,
   * out becomes in and both and either remain the same.
   */
  def reverse: EdgeDirection = this match {
    case EdgeDirection.In => EdgeDirection.Out
    case EdgeDirection.Out => EdgeDirection.In
    case EdgeDirection.Either => EdgeDirection.Either
    case EdgeDirection.Both => EdgeDirection.Both
  }

  override def toString: String = "EdgeDirection." + name

  override def equals(o: Any): Boolean = o match {
    case other: EdgeDirection => other.name == name
    case _ => false
  }

  override def hashCode: Int = name.hashCode
}


/**
 * A set of [[EdgeDirection]]s.
 * 表示边两个顶点的关系
 */
object EdgeDirection {
  /** Edges arriving at a vertex.
    * a--->b,说明a是出out,b是进in
    **/
  final val In: EdgeDirection = new EdgeDirection("In") //指代B---head边

  /** Edges originating from a vertex. */
  final val Out: EdgeDirection = new EdgeDirection("Out")//指代A--tail边

  /** Edges originating from *or* arriving at a vertex of interest.
    * 表示入度+出度
    **/
  final val Either: EdgeDirection = new EdgeDirection("Either")

  /** Edges originating from *and* arriving at a vertex of interest.
    * 表示自己又是入度 又是出度
    **/
  final val Both: EdgeDirection = new EdgeDirection("Both")
}
