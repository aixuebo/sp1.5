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
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types._

object NamedExpression {
  private val curId = new java.util.concurrent.atomic.AtomicLong()
  def newExprId: ExprId = ExprId(curId.getAndIncrement()) //JVM中唯一的全局ID
  def unapply(expr: NamedExpression): Option[(String, DataType)] = Some(expr.name, expr.dataType) //返回一个表达式的命名和返回类型
}

/**
 * A globally unique (within this JVM) id for a given named expression.
 * Used to identify which attribute output by a relation is being
 * referenced in a subsequent computation.
 * JVM中唯一的全局ID
 */
case class ExprId(id: Long)

/**
 * An [[Expression]] that is named.
 * 给一个表达式命名
 */
trait NamedExpression extends Expression {

  /** We should never fold named expressions in order to not remove the alias. */
  override def foldable: Boolean = false

  def name: String //表达式的命名
  def exprId: ExprId //全局唯一ID

  /**
   * Returns a dot separated fully qualified name for this attribute.  Given that there can be
   * multiple qualifiers, it is possible that there are other possible way to refer to this
   * attribute.
   * 返回使用.分隔的属性全名字
   */
  def qualifiedName: String = (qualifiers.headOption.toSeq :+ name).mkString(".")

  /**
   * All possible qualifiers for the expression.
   *
   * For now, since we do not allow using original table name to qualify a column name once the
   * table is aliased, this can only be:
   *
   * 1. Empty Seq: when an attribute doesn't have a qualifier,
   *    e.g. top level attributes aliased in the SELECT clause, or column from a LocalRelation.
   * 2. Single element: either the table name or the alias name of the table.
    * 只能是两种情况之一
    * 要么是没有内容,表示该字段属于根节点对应的表,比如select name from user
    * 要么是有一个值,表示别名,比如select a.name from user a
   */
  def qualifiers: Seq[String]

  def toAttribute: Attribute //将其转换成一个属性对象

  /** Returns the metadata when an expression is a reference to another expression with metadata. */
  def metadata: Metadata = Metadata.empty

  //为返回类型添加后缀,只有是Long类型的时候添加后缀L,其他类型不添加后缀
  protected def typeSuffix =
    if (resolved) {
      dataType match {
        case LongType => "L"
        case _ => ""
      }
    } else {
      ""
    }
}

//属性表达式,同时追加命名服务
abstract class Attribute extends LeafExpression with NamedExpression {

  override def references: AttributeSet = AttributeSet(this)

  def withNullability(newNullability: Boolean): Attribute //true表示该属性是可以是null的,用于left join的时候,右边的表字段的值是可以是null的
  def withQualifiers(newQualifiers: Seq[String]): Attribute //设置别名前缀,即该属性属于哪个别名表的属性
  def withName(newName: String): Attribute //属性名字

  override def toAttribute: Attribute = this //返回本身
  def newInstance(): Attribute //创建一个属性对象

}

/**
 * Used to assign a new name to a computation.
 * For example the SQL expression "1 + 1 AS a" could be represented as follows:
 *  Alias(Add(Literal(1), Literal(1)), "a")()
 *
 * Note that exprId and qualifiers are in a separate parameter list because
 * we only pattern match on child and name.
 *
 * @param child the computation being performed 执行的表达式
 * @param name the name to be associated with the result of computing [[child]]. 为表达式计算机后分配一个别名
 * @param exprId A globally unique id used to check if an [[AttributeReference]] refers to this
 *               alias. Auto-assigned if left blank.JVM内全局唯一ID
 * @param explicitMetadata Explicit metadata associated with this alias that overwrites child's.
 * 别名表达式  name是别名  表达式运算的结果就是别名对应的值
 */
case class Alias(child: Expression, name: String)(
    val exprId: ExprId = NamedExpression.newExprId,//获取唯一ID
    val qualifiers: Seq[String] = Nil,
    val explicitMetadata: Option[Metadata] = None)
  extends UnaryExpression with NamedExpression {

  // Alias(Generator, xx) need to be transformed into Generate(generator, ...)
  override lazy val resolved =
    childrenResolved && checkInputDataTypes().isSuccess && !child.isInstanceOf[Generator] //说明解析参数成功

  override def eval(input: InternalRow): Any = child.eval(input) //执行表达式

  /** Just a simple passthrough for code generation. */
  override def gen(ctx: CodeGenContext): GeneratedExpressionCode = child.gen(ctx)
  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = ""

  override def dataType: DataType = child.dataType //子表达式的返回类型
  override def nullable: Boolean = child.nullable
  override def metadata: Metadata = {
    explicitMetadata.getOrElse {
      child match {
        case named: NamedExpression => named.metadata
        case _ => Metadata.empty
      }
    }
  }

  override def toAttribute: Attribute = {
    if (resolved) {
      AttributeReference(name, child.dataType, child.nullable, metadata)(exprId, qualifiers) //定义一个name对应的field属性对象
    } else {
      UnresolvedAttribute(name)
    }
  }

  override def toString: String = s"$child AS $name#${exprId.id}$typeSuffix"

  override protected final def otherCopyArgs: Seq[AnyRef] = {
    exprId :: qualifiers :: explicitMetadata :: Nil
  }

  override def equals(other: Any): Boolean = other match {
    case a: Alias =>
      name == a.name && exprId == a.exprId && child == a.child && qualifiers == a.qualifiers &&
        explicitMetadata == a.explicitMetadata
    case _ => false
  }
}

/**
 * A reference to an attribute produced by another operator in the tree.
 *
 * @param name The name of this attribute, should only be used during analysis or for debugging.
 * @param dataType The [[DataType]] of this attribute.
 * @param nullable True if null is a valid value for this attribute.
 * @param metadata The metadata of this attribute.
 * @param exprId A globally unique id used to check if different AttributeReferences refer to the
 *               same attribute.
 * @param qualifiers a list of strings that can be used to referred to this attribute in a fully
 *                   qualified way. Consider the examples tableName.name, subQueryAlias.name.
 *                   tableName and subQueryAlias are possible qualifiers.
 * 代表一个属性StructField的详细信息
 *
 * 我猜想 select a.name b from biao a  或者 select str(a.name) b from biao a
 * name 是b,dataType是String,即第一部分参数都描述select这个字段的
 * 第二部分描述的是该字段锁对应的表达式,即a.name,或者str(a.name)
 */
case class AttributeReference(
    name: String,//属性名字
    dataType: DataType,//该属性对应的类型
    nullable: Boolean = true,//该属性是否为null
    override val metadata: Metadata = Metadata.empty)(
    val exprId: ExprId = NamedExpression.newExprId,
    val qualifiers: Seq[String] = Nil)
  extends Attribute with Unevaluable {

  /**
   * Returns true iff the expression id is the same for both attributes.
   * 引用是否相同
   */
  def sameRef(other: AttributeReference): Boolean = this.exprId == other.exprId

  override def equals(other: Any): Boolean = other match {
    case ar: AttributeReference => name == ar.name && exprId == ar.exprId && dataType == ar.dataType
    case _ => false
  }

  override def semanticEquals(other: Expression): Boolean = other match {
    case ar: AttributeReference => sameRef(ar)
    case _ => false
  }

  override def hashCode: Int = {
    // See http://stackoverflow.com/questions/113511/hash-code-implementation
    var h = 17
    h = h * 37 + exprId.hashCode()
    h = h * 37 + dataType.hashCode()
    h = h * 37 + metadata.hashCode()
    h
  }

  override def newInstance(): AttributeReference =
    AttributeReference(name, dataType, nullable, metadata)(qualifiers = qualifiers)

  /**
   * Returns a copy of this [[AttributeReference]] with changed nullability.
   */
  override def withNullability(newNullability: Boolean): AttributeReference = {
    if (nullable == newNullability) {
      this
    } else {
      AttributeReference(name, dataType, newNullability, metadata)(exprId, qualifiers)
    }
  }

  override def withName(newName: String): AttributeReference = {
    if (name == newName) {
      this
    } else {
      AttributeReference(newName, dataType, nullable)(exprId, qualifiers)
    }
  }

  /**
   * Returns a copy of this [[AttributeReference]] with new qualifiers.
   */
  override def withQualifiers(newQualifiers: Seq[String]): AttributeReference = {
    if (newQualifiers.toSet == qualifiers.toSet) {
      this
    } else {
      AttributeReference(name, dataType, nullable, metadata)(exprId, newQualifiers)
    }
  }

  override def toString: String = s"$name#${exprId.id}$typeSuffix"
}

/**
 * A place holder used when printing expressions without debugging information such as the
 * expression id or the unresolved indicator.
 */
case class PrettyAttribute(name: String) extends Attribute with Unevaluable {

  override def toString: String = name

  override def withNullability(newNullability: Boolean): Attribute =
    throw new UnsupportedOperationException
  override def newInstance(): Attribute = throw new UnsupportedOperationException
  override def withQualifiers(newQualifiers: Seq[String]): Attribute =
    throw new UnsupportedOperationException
  override def withName(newName: String): Attribute = throw new UnsupportedOperationException
  override def qualifiers: Seq[String] = throw new UnsupportedOperationException
  override def exprId: ExprId = throw new UnsupportedOperationException
  override def nullable: Boolean = throw new UnsupportedOperationException
  override def dataType: DataType = NullType
}

//虚拟列
object VirtualColumn {
  val groupingIdName: String = "grouping__id" //分组的虚拟列名字
  val groupingIdAttribute: UnresolvedAttribute = UnresolvedAttribute(groupingIdName)
}
