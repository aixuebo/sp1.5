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

package org.apache.spark.sql.catalyst.rules

import scala.collection.JavaConverters._

import com.google.common.util.concurrent.AtomicLongMap

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.util.sideBySide

object RuleExecutor {
  //每一个规则执行的时间,即Map是String long类型的
  protected val timeMap = AtomicLongMap.create[String]()

  /** Resets statistics about time spent running specific rules */
  def resetTime(): Unit = timeMap.clear()

  /** Dump statistics about time spent running specific rules. */
  def dumpTimeSpent(): String = {
    val map = timeMap.asMap().asScala
    val maxSize = map.keys.map(_.toString.length).max //获取key的String最大的长度,目的是打印的结果有格式化效果,都对齐打印
    map.toSeq.sortBy(_._2).reverseMap { case (k, v) =>
      s"${k.padTo(maxSize, " ").mkString} $v"
    }.mkString("\n") //按照时间倒排序,获取key和value,然后打印数据
  }
}

abstract class RuleExecutor[TreeType <: TreeNode[_]] extends Logging {

  /**
   * An execution strategy for rules that indicates the maximum number of executions. If the
   * execution reaches fix point (i.e. converge) before maxIterations, it will stop.
   * 一个规则的执行策略,指示最大的执行次数
   * 执行规则
   */
  abstract class Strategy { def maxIterations: Int }

  /** A strategy that only runs once.
    * 最大值为1,只允许一次
    **/
  case object Once extends Strategy { val maxIterations = 1 }

  /** A strategy that runs until fix point or maxIterations times, whichever comes first.
    * 设置最大值---设置一个固定值
    **/
  case class FixedPoint(maxIterations: Int) extends Strategy

  /** A batch of rules.
    * 一个批次里面有很多规则,但是他们都是同一个策略
    * 包含批处理的名字 以及策略,以及处理规则
    **/
  protected case class Batch(name: String, strategy: Strategy, rules: Rule[TreeType]*)

  /** Defines a sequence of rule batches, to be overridden by the implementation.
    * 记录一个批次集合
    **/
  protected val batches: Seq[Batch]


  /**
   * Executes the batches of rules defined by the subclass. The batches are executed serially
   * using the defined execution strategy.
   * 这行每一个批次下的规则集合
   * Within each batch, rules are also executed serially.
   * 每一个批次内,规则是连续的被执行的
   *
   * 对计划进行更改,按照一定规则进行更改计划
   */
  def execute(plan: TreeType): TreeType = {
    var curPlan = plan //开始传入进来的逻辑计划

    batches.foreach { batch => //循环所有批次
      val batchStartPlan = curPlan //该批次开始时候的计划快照----目的是与最终批次处理后生成的计划做对比,看是否更改了计划
      var iteration = 1 //迭代次数
      var lastPlan = curPlan //每一个规则后,最新的计划
      var continue = true

      // Run until fix point (or the max number of iterations as specified in the strategy.
      while (continue) {
        //处理该批次的所有规则,按照顺序依次处理--最终产生最新的逻辑计划
        curPlan = batch.rules.foldLeft(curPlan) { //循环一个批次里面的所有规则,让该计划通过每一个规则,转换成新的计划
          case (plan, rule) => //合并后的新的逻辑计划 以及 每一个规则
            val startTime = System.nanoTime()
            val result = rule(plan) //对该计划使用该规则
            val runTime = System.nanoTime() - startTime //计算规则的运行时间
            RuleExecutor.timeMap.addAndGet(rule.ruleName, runTime) //记录该规则的运行时间

            if (!result.fastEquals(plan)) {//说明计划有变化,即规则产生的效果
              logTrace(
                s"""
                  |=== Applying Rule ${rule.ruleName} ===
                  |${sideBySide(plan.treeString, result.treeString).mkString("\n")}
                """.stripMargin)
            }

            result
        }
        iteration += 1
        if (iteration > batch.strategy.maxIterations) { //停止迭代,因为迭代次数已经达到上限
          // Only log if this is a rule that is supposed to run more than once.
          if (iteration != 2) {
            logInfo(s"Max iterations (${iteration - 1}) reached for batch ${batch.name}")
          }
          continue = false
        }

        if (curPlan.fastEquals(lastPlan)) {//说明经过该批次的规则后,计划没有变化,一旦计划没有变化,则不需要再次迭代了,即使没有达到最大循环次数也不迭代了
          logTrace(
            s"Fixed point reached for batch ${batch.name} after ${iteration - 1} iterations.")
          continue = false
        }
        lastPlan = curPlan
      }

      if (!batchStartPlan.fastEquals(curPlan)) {//说明这个批次的规则下,是将原始计划有改变的,因此打印日志
        logDebug(
          s"""
          |=== Result of Batch ${batch.name} ===
          |${sideBySide(plan.treeString, curPlan.treeString).mkString("\n")}
        """.stripMargin)
      } else {
        logTrace(s"Batch ${batch.name} has no effect.") //说明该批次 第一次迭代都没有成功修改逻辑计划
      }

      //继续循环下一个批次
    }

    curPlan
  }
}
