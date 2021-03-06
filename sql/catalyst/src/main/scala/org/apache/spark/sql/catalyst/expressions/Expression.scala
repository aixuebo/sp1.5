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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.types._

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines the basic expression abstract classes in Catalyst.
////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * An expression in Catalyst.
 *
 * If an expression wants to be exposed in the function registry (so users can call it with
 * "name(arguments...)", the concrete implementation must be a case class whose constructor
 * arguments are all Expressions types. See [[Substring]] for an example.
 *
 * There are a few important traits:
 *
 * - [[Nondeterministic]]: an expression that is not deterministic.表达式的不确定性,如果一个表达式总是返回相同的结果,则说明是确定的,例如表达式的不确定性--比如randomExpressions表达式专门产生随机数,因此肯定是不确定的表达式,每一次执行的结果都是不一样的,即就是不确定性
 * - [[Unevaluable]]: an expression that is not supposed to be evaluated. 不能求值的表达式,比如 *
 * - [[CodegenFallback]]: an expression that does not have code gen implemented and falls back to
 *                        interpreted mode.
 *
 * - [[LeafExpression]]: an expression that has no child. 叶子表达式,表达式没有子表达式
 * - [[UnaryExpression]]: an expression that has one child. 一元表达式,即一个输入 一个输出
 * - [[BinaryExpression]]: an expression that has two children. 二元表达式,即两个输入 一个输出
 * - [[BinaryOperator]]: a special case of [[BinaryExpression]] that requires two children to have
 *                       the same output data type.一种特殊的二元表达式,即两个输入 一个输出,要求两个输入有相同的类型
 *
 */
abstract class Expression extends TreeNode[Expression] {

  /**
   * Returns true when an expression is a candidate for static evaluation before the query is
   * executed.
   * 在执行查询之前，如果表达式是静态求值的候选项，则返回true
   *
   * 有时候表达式的值是根据具体行数据决定的,此时该值都是false
如果在执行查询前该表达式的值是可以计算的,则返回true

   * The following conditions are used to determine suitability for constant folding:
   *  - A [[Coalesce]] is foldable if all of its children are foldable 合并---所有子表达式都是已经查询前确定了,那么该表达式一定也是确定的
   *  - A [[BinaryExpression]] is foldable if its both left and right child are foldable
   *  - A [[Not]], [[IsNull]], or [[IsNotNull]] is foldable if its child is foldable
   *  - A [[Literal]] is foldable 常量一定是确定的
   *  - A [[Cast]] or [[UnaryMinus]] is foldable if its child is foldable
   */
  def foldable: Boolean = false

  /**
   * Returns true when the current expression always return the same result for fixed inputs from
   * children.
   * 如果一个表达式总是返回相同的结果,则说明是确定的
   * Note that this means that an expression should be considered as non-deterministic if:
   * - if it relies on some mutable internal state, or 如果该表达式依赖内部的可变的状态,则说明是false
   * - if it relies on some implicit input that is not part of the children expression list.表达式依赖一些隐士的输入,他不是子表达式的一部分
   * - if it has non-deterministic child or children.他的子表达式就是不确定的
   *
   * An example would be `SparkPartitionID` that relies on the partition id returned by TaskContext.
   * By default leaf expressions are deterministic as Nil.forall(_.deterministic) returns true.
   */
  def deterministic: Boolean = children.forall(_.deterministic)

  def nullable: Boolean //判断value是否允许是null  true表示允许是null

  //表达式依赖的属性集合
  def references: AttributeSet = AttributeSet(children.flatMap(_.references.iterator))

  /** Returns the result of evaluating this expression on a given input Row
    * 将一行数据进行处理,比如二元表达式,就是两个表达式同时对该行数据进行处理,返回的值作为参数进行二元运算,最终返回一个结果就是返回值
    **/
  def eval(input: InternalRow = null): Any

  /**
   * Returns an [[GeneratedExpressionCode]], which contains Java source code that
   * can be used to generate the result of evaluating the expression on an input row.
   *
   * @param ctx a [[CodeGenContext]]
   * @return [[GeneratedExpressionCode]]
   */
  def gen(ctx: CodeGenContext): GeneratedExpressionCode = {
    val isNull = ctx.freshName("isNull")
    val primitive = ctx.freshName("primitive")
    val ve = GeneratedExpressionCode("", isNull, primitive)
    ve.code = genCode(ctx, ve)
    // Add `this` in the comment.
    ve.copy(s"/* $this */\n" + ve.code)
  }

  /**
   * Returns Java source code that can be compiled to evaluate this expression.
   * The default behavior is to call the eval method of the expression. Concrete expression
   * implementations should override this to do actual code generation.
   *
   * @param ctx a [[CodeGenContext]]
   * @param ev an [[GeneratedExpressionCode]] with unique terms.
   * @return Java source code
   */
  protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String

  /**
   * Returns `true` if this expression and all its children have been resolved to a specific schema
   * and input data types checking passed, and `false` if it still contains any unresolved
   * placeholders or has data types mismatch.
   * Implementations of expressions should override this if the resolution of this type of
   * expression involves more than just the resolution of its children and type checking.
   * true表示表达式以及所有的子表达式都已经校验完成
   */
  lazy val resolved: Boolean = childrenResolved && checkInputDataTypes().isSuccess //childrenResolved作用是递归操作

  /**
   * Returns the [[DataType]] of the result of evaluating this expression.  It is
   * invalid to query the dataType of an unresolved expression (i.e., when `resolved` == false).
   * 表达式的返回的结果类型
   */
  def dataType: DataType

  /**
   * Returns true if  all the children of this expression have been resolved to a specific schema
   * and false if any still contains any unresolved placeholders.
   * 子类已经校验完成
   */
  def childrenResolved: Boolean = children.forall(_.resolved) //所有的子表达式resolved都是true,则返回true

  /**
   * Returns true when two expressions will always compute the same result, even if they differ
   * cosmetically (i.e. capitalization of names in attributes may be different).
   * 两个表达式的语义是相同的.则返回true
   */
  def semanticEquals(other: Expression): Boolean = this.getClass == other.getClass && {
    def checkSemantic(elements1: Seq[Any], elements2: Seq[Any]): Boolean = {
      elements1.length == elements2.length && elements1.zip(elements2).forall {
        case (e1: Expression, e2: Expression) => e1 semanticEquals e2
        case (Some(e1: Expression), Some(e2: Expression)) => e1 semanticEquals e2
        case (t1: Traversable[_], t2: Traversable[_]) => checkSemantic(t1.toSeq, t2.toSeq)
        case (i1, i2) => i1 == i2
      }
    }
    val elements1 = this.productIterator.toSeq
    val elements2 = other.asInstanceOf[Product].productIterator.toSeq
    checkSemantic(elements1, elements2)
  }

  /**
   * Checks the input data types, returns `TypeCheckResult.success` if it's valid,
   * or returns a `TypeCheckResult` with an error message if invalid.
   * Note: it's not valid to call this method until `childrenResolved == true`.
   * 本类校验输入类型是否校验成功
   */
  def checkInputDataTypes(): TypeCheckResult = TypeCheckResult.TypeCheckSuccess

  /**
   * Returns a user-facing string representation of this expression's name.
   * This should usually match the name of the function in SQL.
   */
  def prettyName: String = getClass.getSimpleName.toLowerCase

  /**
   * Returns a user-facing string representation of this expression, i.e. does not have developer
   * centric debugging information like the expression id.
   * 一个简洁的面向用户的名字
   */
  def prettyString: String = {
    transform {
      case a: AttributeReference => PrettyAttribute(a.name)
      case u: UnresolvedAttribute => PrettyAttribute(u.name)
    }.toString
  }

  override def toString: String = prettyName + children.mkString("(", ",", ")")
}


/**
 * An expression that cannot be evaluated. Some expressions don't live past analysis or optimization
 * time (e.g. Star). This trait is used by those expressions.
 * 不能求值的表达式,比如 *,窗口函数
 */
trait Unevaluable extends Expression {

  //不能参与运算求具体的值
  final override def eval(input: InternalRow = null): Any =
    throw new UnsupportedOperationException(s"Cannot evaluate expression: $this")

  final override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String =
    throw new UnsupportedOperationException(s"Cannot evaluate expression: $this")
}


/**
 * An expression that is nondeterministic.
 * 表达式的不确定性--比如randomExpressions表达式专门产生随机数,因此肯定是不确定的表达式,每一次执行的结果都是不一样的,即就是不确定性
 */
trait Nondeterministic extends Expression {
  final override def deterministic: Boolean = false //不确定,因此是false
  final override def foldable: Boolean = false //不确定的一定不是静态的,因此是false

  private[this] var initialized = false //已经初始化完成

  final def setInitialValues(): Unit = {
    initInternal()
    initialized = true
  }

  protected def initInternal(): Unit //初始化该表达式

  //将一行数据进行处理,比如二元表达式,就是两个表达式同时对该行数据进行处理,返回的值作为参数进行二元运算,最终返回一个结果就是返回值
  final override def eval(input: InternalRow = null): Any = {
    require(initialized, "nondeterministic expression should be initialized before evaluate")
    evalInternal(input)
  }

  //将一行数据进行处理,比如二元表达式,就是两个表达式同时对该行数据进行处理,返回的值作为参数进行二元运算,最终返回一个结果就是返回值
  //因为不知道有多少个参数,因此不确定性的表达式自己内部逻辑全实现即可
  protected def evalInternal(input: InternalRow): Any
}


/**
 * A leaf expression, i.e. one without any child expressions.
 * 叶子表达式
 */
abstract class LeafExpression extends Expression {

  def children: Seq[Expression] = Nil //表达式没有子表达式
}


/**
 * An expression with one input and one output. The output is by default evaluated to null
 * if the input is evaluated to null.
 * 一元表达式,即一个输入 一个输出
 */
abstract class UnaryExpression extends Expression {

  def child: Expression //只有一个子表达式

  override def children: Seq[Expression] = child :: Nil

  override def foldable: Boolean = child.foldable //取决于子表达式是否是静态的
  override def nullable: Boolean = child.nullable //取决于子表达式是否是null

  /**
   * Default behavior of evaluation according to the default nullability of UnaryExpression.
   * If subclass of UnaryExpression override nullable, probably should also override this.
   */
  override def eval(input: InternalRow): Any = {
    val value = child.eval(input) //表达式对数据进行处理,返回结果
    if (value == null) {
      null
    } else {
      nullSafeEval(value) //将结果进一步进行一元表达式处理
    }
  }

  /**
   * Called by default [[eval]] implementation.  If subclass of UnaryExpression keep the default
   * nullability, they can override this method to save null-check code.  If we need full control
   * of evaluation process, we should override [[eval]].
   * 具体一元方式是子类实现
即一元表达式的具体子类只需要关注自己这一层接收的参数如何处理就可以。不需要关注参数怎么来的。
参数是具体的计算后的值。
   */
  protected def nullSafeEval(input: Any): Any =
    sys.error(s"UnaryExpressions must override either eval or nullSafeEval")

  /**
   * Called by unary expressions to generate a code block that returns null if its parent returns
   * null, and if not not null, use `f` to generate the expression.
   *
   * As an example, the following does a boolean inversion (i.e. NOT).
   * {{{
   *   defineCodeGen(ctx, ev, c => s"!($c)")
   * }}}
   *
   * @param f function that accepts a variable name and returns Java code to compute the output.
   */
  protected def defineCodeGen(
      ctx: CodeGenContext,
      ev: GeneratedExpressionCode,
      f: String => String): String = {
    nullSafeCodeGen(ctx, ev, eval => {
      s"${ev.primitive} = ${f(eval)};"
    })
  }

  /**
   * Called by unary expressions to generate a code block that returns null if its parent returns
   * null, and if not not null, use `f` to generate the expression.
   *
   * @param f function that accepts the non-null evaluation result name of child and returns Java
   *          code to compute the output.
   */
  protected def nullSafeCodeGen(
      ctx: CodeGenContext,
      ev: GeneratedExpressionCode,
      f: String => String): String = {
    val eval = child.gen(ctx)
    val resultCode = f(eval.primitive)
    eval.code + s"""
      boolean ${ev.isNull} = ${eval.isNull};
      ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
      if (!${ev.isNull}) {
        $resultCode
      }
    """
  }
}


/**
 * An expression with two inputs and one output. The output is by default evaluated to null
 * if any input is evaluated to null.
 * 二元表达式,即两个输入 一个输出
 * 比如 case class Contains(left: Expression, right: Expression) extends BinaryExpression with StringPredicate {
 */
abstract class BinaryExpression extends Expression {

  //有两个子表达式
  def left: Expression
  def right: Expression

  override def children: Seq[Expression] = Seq(left, right)

  override def foldable: Boolean = left.foldable && right.foldable //两个都得是静态的

  override def nullable: Boolean = left.nullable || right.nullable

  /**
   * Default behavior of evaluation according to the default nullability of BinaryExpression.
   * If subclass of BinaryExpression override nullable, probably should also override this.
   * 同一样数据InternalRow 分别参与两个表达式运算,得到的值后在进行综合运算
   * 注意:有任意一个是null参与的二元运算,结果都是null,不需要参与具体的计算了
   * Contains中两个参数必须存在,如果一个参数为null,我们就可以说他们一定返回null,除非特例,子类需要重新写eval方法
   */
  override def eval(input: InternalRow): Any = {
    val value1 = left.eval(input)
    if (value1 == null) {
      null
    } else {
      val value2 = right.eval(input)
      if (value2 == null) {
        null
      } else {
        nullSafeEval(value1, value2) //两个值都不是null,则参与二元运算
      }
    }
  }

  /**
   * Called by default [[eval]] implementation.  If subclass of BinaryExpression keep the default
   * nullability, they can override this method to save null-check code.  If we need full control
   * of evaluation process, we should override [[eval]].
   * 子类实现
   */
  protected def nullSafeEval(input1: Any, input2: Any): Any =
    sys.error(s"BinaryExpressions must override either eval or nullSafeEval")

  /**
   * Short hand for generating binary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f accepts two variable names and returns Java code to compute the output.
   */
  protected def defineCodeGen(
      ctx: CodeGenContext,
      ev: GeneratedExpressionCode,
      f: (String, String) => String): String = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      s"${ev.primitive} = ${f(eval1, eval2)};"
    })
  }

  /**
   * Short hand for generating binary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f function that accepts the 2 non-null evaluation result names of children
   *          and returns Java code to compute the output.
   */
  protected def nullSafeCodeGen(
      ctx: CodeGenContext,
      ev: GeneratedExpressionCode,
      f: (String, String) => String): String = {
    val eval1 = left.gen(ctx)
    val eval2 = right.gen(ctx)
    val resultCode = f(eval1.primitive, eval2.primitive)
    s"""
      ${eval1.code}
      boolean ${ev.isNull} = ${eval1.isNull};
      ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
      if (!${ev.isNull}) {
        ${eval2.code}
        if (!${eval2.isNull}) {
          $resultCode
        } else {
          ${ev.isNull} = true;
        }
      }
    """
  }
}


/**
 * A [[BinaryExpression]] that is an operator, with two properties:
 *
 * 1. The string representation is "x symbol y", rather than "funcName(x, y)".
 * 2. Two inputs are expected to the be same type. If the two inputs have different types,
 *    the analyzer will find the tightest common type and do the proper type casting.
 * 一种特殊的二元表达式,即两个输入 一个输出,要求两个输入有相同的类型
 */
abstract class BinaryOperator extends BinaryExpression with ExpectsInputTypes {

  /**
   * Expected input type from both left/right child expressions, similar to the
   * [[ImplicitCastInputTypes]] trait.
   * 输入的数据类型
   */
  def inputType: AbstractDataType

  def symbol: String //语法规则,比如+ - 等

  override def toString: String = s"($left $symbol $right)"

  override def inputTypes: Seq[AbstractDataType] = Seq(inputType, inputType) //二元的类型必须都相同

  //进行参数的有效性校验
  override def checkInputDataTypes(): TypeCheckResult = {
    // First check whether left and right have the same type, then check if the type is acceptable.
    if (left.dataType != right.dataType) {//左右类型必须相同
      TypeCheckResult.TypeCheckFailure(s"differing types in '$prettyString' " +
        s"(${left.dataType.simpleString} and ${right.dataType.simpleString}).")
    } else if (!inputType.acceptsType(left.dataType)) {//输入类型是可以转换成参数需要的类型的
      TypeCheckResult.TypeCheckFailure(s"'$prettyString' requires ${inputType.simpleString} type," +
        s" not ${left.dataType.simpleString}")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }
}

//将二元操作的两个表达式提取出来
private[sql] object BinaryOperator {
  def unapply(e: BinaryOperator): Option[(Expression, Expression)] = Some((e.left, e.right))
}

/**
 * An expression with three inputs and one output. The output is by default evaluated to null
 * if any input is evaluated to null.
 * 三个输入,一个输出
 * 比如 SubstringIndex(strExpr: Expression, delimExpr: Expression, countExpr: Expression) extends TernaryExpression with ImplicitCastInputTypes
 */
abstract class TernaryExpression extends Expression {

  override def foldable: Boolean = children.forall(_.foldable) //三个输入表达式所有的都得是静态的,才返回true

  override def nullable: Boolean = children.exists(_.nullable) //三个输入表达式有一个是null,则返回true

  /**
   * Default behavior of evaluation according to the default nullability of TernaryExpression.
   * If subclass of BinaryExpression override nullable, probably should also override this.
   * 将三个表达式都对同一个输入数据进行解析,然后执行nullSafeEval方法,返回新的值
   *
   * 注意:三个参数的值必须都是具体的值,一旦有一个是null,则返回的都是null
   */
  override def eval(input: InternalRow): Any = {
    val exprs = children
    val value1 = exprs(0).eval(input)
    if (value1 != null) {
      val value2 = exprs(1).eval(input)
      if (value2 != null) {
        val value3 = exprs(2).eval(input)
        if (value3 != null) {
          return nullSafeEval(value1, value2, value3)
        }
      }
    }
    null
  }

  /**
   * Called by default [[eval]] implementation.  If subclass of TernaryExpression keep the default
   * nullability, they can override this method to save null-check code.  If we need full control
   * of evaluation process, we should override [[eval]].
   * 子类具体实现
   */
  protected def nullSafeEval(input1: Any, input2: Any, input3: Any): Any =
    sys.error(s"BinaryExpressions must override either eval or nullSafeEval")

  /**
   * Short hand for generating binary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f accepts two variable names and returns Java code to compute the output.
   */
  protected def defineCodeGen(
    ctx: CodeGenContext,
    ev: GeneratedExpressionCode,
    f: (String, String, String) => String): String = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2, eval3) => {
      s"${ev.primitive} = ${f(eval1, eval2, eval3)};"
    })
  }

  /**
   * Short hand for generating binary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f function that accepts the 2 non-null evaluation result names of children
   *          and returns Java code to compute the output.
   */
  protected def nullSafeCodeGen(
    ctx: CodeGenContext,
    ev: GeneratedExpressionCode,
    f: (String, String, String) => String): String = {
    val evals = children.map(_.gen(ctx))
    val resultCode = f(evals(0).primitive, evals(1).primitive, evals(2).primitive)
    s"""
      ${evals(0).code}
      boolean ${ev.isNull} = true;
      ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
      if (!${evals(0).isNull}) {
        ${evals(1).code}
        if (!${evals(1).isNull}) {
          ${evals(2).code}
          if (!${evals(2).isNull}) {
            ${ev.isNull} = false;  // resultCode could change nullability
            $resultCode
          }
        }
      }
    """
  }
}
