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

package org.apache.spark.sql.catalyst.trees

import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.types.DataType

/** Used by [[TreeNode.getNodeNumbered]] when traversing the tree for a given number */
private class MutableInt(var i: Int)

case class Origin(
  line: Option[Int] = None,//表示出问题的sql是第几行
  startPosition: Option[Int] = None)//表示出问题的sql是该行第几个位置

/**
 * Provides a location for TreeNodes to ask about the context of their origin.  For example, which
 * line of code is currently being parsed.
 */
object CurrentOrigin {
  private val value = new ThreadLocal[Origin]() {
    override def initialValue: Origin = Origin()
  }

  def get: Origin = value.get()
  def set(o: Origin): Unit = value.set(o)

  def reset(): Unit = value.set(Origin())

  def setPosition(line: Int, start: Int): Unit = {
    value.set(
      value.get.copy(line = Some(line), startPosition = Some(start)))
  }
//先设置Origin,然后执行f函数,执行后还原新的Origin
  def withOrigin[A](o: Origin)(f: => A): A = {
    set(o)
    val ret = try f finally { reset() }
    reset()
    ret
  }
}

//组件语法树的一个节点,参数是表示必须是TreeNode的子类
abstract class TreeNode[BaseType <: TreeNode[BaseType]] extends Product {
  self: BaseType =>

  val origin: Origin = CurrentOrigin.get

  /**
   * Returns a Seq of the children of this node.
   * Children should not change. Immutability required for containsChild optimization
   * 该节点拥有的子节点集合
   */
  def children: Seq[BaseType]

  lazy val containsChild: Set[TreeNode[_]] = children.toSet

  /**
   * Faster version of equality which short-circuits when two treeNodes are the same instance.
   * We don't just override Object.equals, as doing so prevents the scala compiler from
   * generating case class `equals` methods
   * 比较两个对象是否相同
   */
  def fastEquals(other: TreeNode[_]): Boolean = {
    this.eq(other) || this == other
  }

  /**
   * Find the first [[TreeNode]] that satisfies the condition specified by `f`.
   * The condition is recursively applied to this node and all of its children (pre-order).
   * 找第一个符合条件函数的对象,先对比自己,然后对比子类,子类对比也是一个子类递归到底的方式进行对比的
   * f的参数就是本类自己
   *
   * 从本节点开始，先序遍历整棵树，返回第一个符合f命题的节点
   */
  def find(f: BaseType => Boolean): Option[BaseType] = f(this) match {
    case true => Some(this)
    case false => children.foldLeft(None: Option[BaseType]) { (l, r) => l.orElse(r.find(f)) } //从子类第一个开始找,默认值是Option[BaseType]类型的None,调用下一个r也进行find查找f函数,一旦返回true,则在下一个校验的时候,l就是true,而不是none,因此就返回找到的l了
  }

  /**
   * Runs the given function on this node and then recursively on [[children]].
   * @param f the function to be applied to each node in the tree.
   * 从自己开始执行表达式,依次延伸到孙子上
   */
  def foreach(f: BaseType => Unit): Unit = {
    f(this)
    children.foreach(_.foreach(f))
  }

  /**
   * Runs the given function recursively on [[children]] then on this node.
   * @param f the function to be applied to each node in the tree.
   * 从孙子的表达式开始执行f函数
   */
  def foreachUp(f: BaseType => Unit): Unit = {
    children.foreach(_.foreachUp(f))
    f(this)
  }

  /**
   * Returns a Seq containing the result of applying the given function to each
   * node in this tree in a preorder traversal.
   * @param f the function to be applied.
   * 自己和子类都调用f函数.每一次调用的返回值汇总返回
   */
  def map[A](f: BaseType => A): Seq[A] = {
    val ret = new collection.mutable.ArrayBuffer[A]()
    foreach(ret += f(_))
    ret
  }

  /**
   * Returns a Seq by applying a function to all nodes in this tree and using the elements of the
   * resulting collections.
   * 与map函数不同的是,参数的返回值是一个集合,而map的返回值是一个对象.
   */
  def flatMap[A](f: BaseType => TraversableOnce[A]): Seq[A] = {
    val ret = new collection.mutable.ArrayBuffer[A]()
    foreach(ret ++= f(_))//因为f返回值是一个集合,因此将集合添加到ret中,因此就是一个整体的集合
    ret
  }

  /**
   * Returns a Seq containing the result of applying a partial function to all elements in this
   * tree on which the function is defined.
   * 偏函数接收BaseType类型参数,转换成B对象
   * 即收集所有符合偏函数转换的数据
   *
   * 即 map的偏函数版
   */
  def collect[B](pf: PartialFunction[BaseType, B]): Seq[B] = {
    val ret = new collection.mutable.ArrayBuffer[B]()
    val lifted = pf.lift
    //偏函数的结果进行foreach,添加到ret中
    foreach(node => lifted(node).foreach(ret.+=))//本类和子类都调用偏函数,转换成B.然后添加到ret集合里面
    ret
  }

  /**
   * Finds and returns the first [[TreeNode]] of the tree for which the given partial function
   * is defined (pre-order), and applies the partial function to it.
   * 偏函数接收BaseType类型参数,转换成B对象
   * 先执行本类,然后执行子类,直到第一个结果返回为止
   */
  def collectFirst[B](pf: PartialFunction[BaseType, B]): Option[B] = {
    val lifted = pf.lift //将偏函数转换成正常函数
    lifted(this).orElse {
      children.foldLeft(None: Option[B]) { (l, r) => l.orElse(r.collectFirst(pf)) }
    }//对本类进行转换成B类型,如果转换失败,转换子类,直到第一个转换B成功的为止,然后返回成功的B
  }

  /**
   * Returns a copy of this node where `f` has been applied to all the nodes children.
   * f函数应用于所有的子对象,转换成新的对象
   */
  def mapChildren(f: BaseType => BaseType): BaseType = {
    var changed = false
    val newArgs = productIterator.map {
      case arg: TreeNode[_] if containsChild(arg) => //获取指定的子元素
        val newChild = f(arg.asInstanceOf[BaseType]) //判断转换后是否有更改
        if (newChild fastEquals arg) {
          arg
        } else {
          changed = true
          newChild
        }
      case nonChild: AnyRef => nonChild
      case null => null
    }.toArray
    if (changed) makeCopy(newArgs) else this
  }

  /**
   * Returns a copy of this node with the children replaced.
   * TODO: Validate somewhere (in debug mode?) that children are ordered correctly.
   */
  def withNewChildren(newChildren: Seq[BaseType]): BaseType = {
    assert(newChildren.size == children.size, "Incorrect number of children")
    var changed = false
    val remainingNewChildren = newChildren.toBuffer
    val remainingOldChildren = children.toBuffer
    val newArgs = productIterator.map {
      // Handle Seq[TreeNode] in TreeNode parameters.
      case s: Seq[_] => s.map {
        case arg: TreeNode[_] if containsChild(arg) =>
          val newChild = remainingNewChildren.remove(0)
          val oldChild = remainingOldChildren.remove(0)
          if (newChild fastEquals oldChild) {
            oldChild
          } else {
            changed = true
            newChild
          }
        case nonChild: AnyRef => nonChild
        case null => null
      }
      case arg: TreeNode[_] if containsChild(arg) =>
        val newChild = remainingNewChildren.remove(0)
        val oldChild = remainingOldChildren.remove(0)
        if (newChild fastEquals oldChild) {
          oldChild
        } else {
          changed = true
          newChild
        }
      case nonChild: AnyRef => nonChild
      case null => null
    }.toArray

    if (changed) makeCopy(newArgs) else this
  }

  /**
   * Returns a copy of this node where `rule` has been recursively applied to the tree.
   * 返回该node的一个copy,基于规则递归应用到所有的node上,然后结果进行copy
   * When `rule` does not apply to a given node it is left unchanged.
   * 当规则没有适应给定的节点,则该节点保持不变
   * Users should not expect a specific directionality. If a specific directionality is needed,
   * transformDown or transformUp should be used.
   * 转换的方向用于不应该去期待,如果需要期待方向行的话,则选择transformDown或者transformUp方法
   *
   * @param rule the function use to transform this nodes children 规则的偏函数去转换一些节点
   * 转换,基于给定的偏函数,符合偏函数规则的node节点,都要进行规则转换,转换成信的node
   * 比如
tree.transform {
case Add(Literal(c1), Literal(c2)) => Literal(c1+c2)
} 将复杂的加法,转换成简单的加法

   */
  def transform(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    transformDown(rule)
  }

  /**
   * Returns a copy of this node where `rule` has been recursively applied to it and all of its
   * children (pre-order). When `rule` does not apply to a given node it is left unchanged.
   * @param rule the function used to transform this nodes children
   */
  def transformDown(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    val afterRule = CurrentOrigin.withOrigin(origin) {
      rule.applyOrElse(this, identity[BaseType]) //表示将该节点应用该偏函数,只有满足偏函数的才会被转换
    }

    // Check if unchanged and then possibly return old copy to avoid gc churn.
    if (this fastEquals afterRule) { //说明没有转换成功,因此继续递归
      transformChildren(rule, (t, r) => t.transformDown(r)) //t表示每一个子对象,即每一个子对象都调用transformDown
    } else {
      afterRule.transformChildren(rule, (t, r) => t.transformDown(r))//使用新的node继续递归
    }
  }

  /**
   * Returns a copy of this node where `rule` has been recursively applied to all the children of
   * this node.  When `rule` does not apply to a given node it is left unchanged.
   * 让该规则去替换每一个子节点,如果匹配不成功,则不更改
   * @param rule the function used to transform this nodes children 该规则去替换该节点的所有子节点
   */
  protected def transformChildren(
      rule: PartialFunction[BaseType, BaseType],//偏函数,可以从一个BaseType转换成另外一个BaseType,即符合规则的节点node要去变更
      nextOperation: (BaseType, PartialFunction[BaseType, BaseType]) => BaseType): BaseType = { //参数表示递归调用偏函数,第一个参数表示子节点,第二个参数表示子节点要进行偏函数调用,如果匹配规则,则就可以规则替换了
    var changed = false
    //表示对子节点进行按照规则转换
    val newArgs = productIterator.map {//循环每一个子节点
      case arg: TreeNode[_] if containsChild(arg) => //每一个子对象
        val newChild = nextOperation(arg.asInstanceOf[BaseType], rule) //将arg这个子节点以及偏函数规则作为参数,进行递归调用,产生新的节点
        if (!(newChild fastEquals arg)) {//说明节点按照规则被替换了
          changed = true
          newChild
        } else {
          arg
        }
      case Some(arg: TreeNode[_]) if containsChild(arg) => //说明子节点是Option的
        val newChild = nextOperation(arg.asInstanceOf[BaseType], rule)
        if (!(newChild fastEquals arg)) {
          changed = true
          Some(newChild)
        } else {
          Some(arg)
        }
      case m: Map[_, _] => m
      case d: DataType => d // Avoid unpacking Structs
      case args: Traversable[_] => args.map {//说明该子节点是一个迭代器,可能迭代器里面的元素是一个node
        case arg: TreeNode[_] if containsChild(arg) =>
          val newChild = nextOperation(arg.asInstanceOf[BaseType], rule)//对每一个node再一次进行规则匹配
          if (!(newChild fastEquals arg)) {
            changed = true
            newChild
          } else {
            arg
          }
        case other => other
      }
      case nonChild: AnyRef => nonChild
      case null => null
    }.toArray
    if (changed) makeCopy(newArgs) else this //如果有节点被变化了,则将新的子节点集合重新生成新的对象
  }

  /**
   * Returns a copy of this node where `rule` has been recursively applied first to all of its
   * children and then itself (post-order). When `rule` does not apply to a given node, it is left
   * unchanged.
   * @param rule the function use to transform this nodes children
   */
  def transformUp(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    val afterRuleOnChildren = transformChildren(rule, (t, r) => t.transformUp(r))
    if (this fastEquals afterRuleOnChildren) {
      CurrentOrigin.withOrigin(origin) {
        rule.applyOrElse(this, identity[BaseType])
      }
    } else {
      CurrentOrigin.withOrigin(origin) {
        rule.applyOrElse(afterRuleOnChildren, identity[BaseType])
      }
    }
  }

  /**
   * Args to the constructor that should be copied, but not transformed.
   * These are appended to the transformed args automatically by makeCopy
   * @return
   */
  protected def otherCopyArgs: Seq[AnyRef] = Nil

  /**
   * Creates a copy of this type of tree node after a transformation.
   * Must be overridden by child classes that have constructor arguments
   * that are not present in the productIterator.
   * @param newArgs the new product arguments.
   * 返回一个新的构造函数实例对象
   */
  def makeCopy(newArgs: Array[AnyRef]): BaseType = attachTree(this, "makeCopy") {
    val ctors = getClass.getConstructors.filter(_.getParameterTypes.size != 0) //返回有参数的构造函数
    if (ctors.isEmpty) {
      sys.error(s"No valid constructor for $nodeName")
    }
    val defaultCtor = ctors.maxBy(_.getParameterTypes.size)//获取最大的一个参数个数的构造函数

    try {
      CurrentOrigin.withOrigin(origin) {
        // Skip no-arg constructors that are just there for kryo.
        if (otherCopyArgs.isEmpty) {
          defaultCtor.newInstance(newArgs: _*).asInstanceOf[BaseType]
        } else {
          defaultCtor.newInstance((newArgs ++ otherCopyArgs).toArray: _*).asInstanceOf[BaseType]
        }
      }
    } catch {
      case e: java.lang.IllegalArgumentException =>
        throw new TreeNodeException(
          this,
          s"""
             |Failed to copy node.
             |Is otherCopyArgs specified correctly for $nodeName.
             |Exception message: ${e.getMessage}
             |ctor: $defaultCtor?
             |args: ${newArgs.mkString(", ")}
           """.stripMargin)
    }
  }

  /** Returns the name of this type of TreeNode.  Defaults to the class name. */
  def nodeName: String = getClass.getSimpleName

  /**
   * The arguments that should be included in the arg string.  Defaults to the `productIterator`.
   */
  protected def stringArgs: Iterator[Any] = productIterator

  /** Returns a string representing the arguments to this node, minus any children
    * 返回一个字符串代表该节点下所有的子节点
    **/
  def argString: String = productIterator.flatMap {//循环每一个子节点
    case tn: TreeNode[_] if containsChild(tn) => Nil
    case tn: TreeNode[_] if tn.toString contains "\n" => s"(${tn.simpleString})" :: Nil
    case seq: Seq[BaseType] if seq.toSet.subsetOf(children.toSet) => Nil
    case seq: Seq[_] => seq.mkString("[", ",", "]") :: Nil
    case set: Set[_] => set.mkString("{", ",", "}") :: Nil
    case other => other :: Nil
  }.mkString(", ")

  /** String representation of this node without any children */
  def simpleString: String = s"$nodeName $argString".trim

  override def toString: String = treeString

  /** Returns a string representation of the nodes in this tree */
  def treeString: String = generateTreeString(0, new StringBuilder).toString

  /**
   * Returns a string representation of the nodes in this tree, where each operator is numbered.
   * The numbers can be used with [[trees.TreeNode.apply apply]] to easily access specific subtrees.
   */
  def numberedTreeString: String =
    treeString.split("\n").zipWithIndex.map { case (line, i) => f"$i%02d $line" }.mkString("\n")

  /**
   * Returns the tree node at the specified number.
   * Numbers for each node can be found in the [[numberedTreeString]].
   */
  def apply(number: Int): BaseType = getNodeNumbered(new MutableInt(number))

  protected def getNodeNumbered(number: MutableInt): BaseType = {
    if (number.i < 0) {
      null.asInstanceOf[BaseType]
    } else if (number.i == 0) {
      this
    } else {
      number.i -= 1
      children.map(_.getNodeNumbered(number)).find(_ != null).getOrElse(null.asInstanceOf[BaseType])
    }
  }

  /** Appends the string represent of this node and its children to the given StringBuilder. */
  protected def generateTreeString(depth: Int, builder: StringBuilder): StringBuilder = {
    builder.append(" " * depth)
    builder.append(simpleString)
    builder.append("\n")
    children.foreach(_.generateTreeString(depth + 1, builder))
    builder
  }

  /**
   * Returns a 'scala code' representation of this `TreeNode` and its children.  Intended for use
   * when debugging where the prettier toString function is obfuscating the actual structure. In the
   * case of 'pure' `TreeNodes` that only contain primitives and other TreeNodes, the result can be
   * pasted in the REPL to build an equivalent Tree.
   */
  def asCode: String = {
    val args = productIterator.map {
      case tn: TreeNode[_] => tn.asCode
      case s: String => "\"" + s + "\""
      case other => other.toString
    }
    s"$nodeName(${args.mkString(",")})"
  }
}
