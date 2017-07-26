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

package org.apache.spark.sql.catalyst

import scala.language.implicitConversions

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * A very simple SQL parser.  Based loosely on:
 * https://github.com/stephentu/scala-sql-parser/blob/master/src/main/scala/parser.scala
 *
 * Limitations:
 *  - Only supports a very limited subset of SQL.
 *
 * This is currently included mostly for illustrative purposes.  Users wanting more complete support
 * for a SQL like language should checkout the HiveQL support in the sql/hive sub-project.
 * 默认的解析sql成LogicalPlan对象的实现类,主要入口函数是parse
 *
 总结:
 第一种类型sql
一、from 本身就是一个LogicalPlan
a.如果是join表,则最终是一个BinaryNode,而BinaryNode继承自LogicalPlan
输出的字段是根据join类型,产生每一个表的哪些字段作为输出

b.如果是一个子查询,则最终是一个Subquery,而该对象继承UnaryNode,继而继承自LogicalPlan
//alias 表示子查询别名,child表示子查询的完整逻辑
case class Subquery(alias: String, child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output.map(_.withQualifiers(alias :: Nil)) //子类的输出属性不会改变,但是名字会改变成别名
}
重新定义了输出,输出是子查询中的输出,只是需要对属性设置一下所属表,即所属表已经是子查询对应的别名了

c.直接查询一个表,则输出是UnresolvedRelation,他是继承自LeafNode,继而继承自LogicalPlan
持有查询的表名以及别名
case class UnresolvedRelation(
    tableIdentifier: Seq[String],//表名,可能是数据库.表名,因此是一个集合
    alias: Option[String] = None) //为表起的别名
    extends LeafNode {

  def tableName: String = tableIdentifier.mkString(".") //表名全路径

  override def output: Seq[Attribute] = Nil

  override lazy val resolved = false
}

二、where表达式 对from的LogicalPlan进一步包装成LogicalPlan
case class Filter(condition: Expression, child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}
其中参数是where表达式，以及from的LogicalPlan作为子LogicalPlan存在
因为只是进行where过滤,因此输出依然是from中的输出内容

三、group by操作,产生一个Aggregate对象,该对象也是一个LogicalPlan
case class Aggregate(
    groupingExpressions: Seq[Expression],//group by操作的表达式集合
    aggregateExpressions: Seq[NamedExpression],//select中所有的表达式集合
    child: LogicalPlan) //在什么结果集上进行group by聚合操作
  extends UnaryNode {
1.可以看到该对象包装了group的表达式、select上的表达式、child表示的就是where之后的LogicalPlan
2.输出就是select中需要的字段内容

四、select distinct 操作,产生Distinct对象,该对象也是一个LogicalPlan
case class Distinct(child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output //distinct不会改变子类的输出属性
}
1.如果有这个操作,则将group by的结果进一步处理,因此child就表示group by的结果集
2.输出不改变group by的输出字段内容,只是过滤重复的数据,因此输出不变化

五、having操作,产生Filter对象,该对象也是一个LogicalPlan
case class Filter(condition: Expression, child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}
其中参数是having表达式，以及group by的结果LogicalPlan作为子LogicalPlan存在
因为只是进行having过滤,因此输出依然是group by中的输出内容

六、order by操作,产生Sort对象,该对象也是一个LogicalPlan
case class Sort(
    order: Seq[SortOrder],//排序的表达式集合
    global: Boolean,//是否全局排序,即order by 和sort by 区别
    child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output //因为是排序,不改变属性.因此输出还是子类的输出
}
其中参数包含如何排序、全局排序还是局部排序、对group by ...having 后的结果集进行排序

七、limit操作,产生Limit对象,该对象也是一个LogicalPlan
case class Limit(limitExpr: Expression, child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output //limit不改变输出的字段,因此输出字段还是子类的字段集合
}
1.参数表示获取多少个数据,以及最终在什么结果集上进行获取
2.输出是不需要变化的,limit只是控制数据大小

--------------
第二种类型sql
insert OVERWRITE/INTO TABLE biao(或者join的结果集合) select逻辑计划
case class InsertIntoTable(
    table: LogicalPlan,//数据插入到哪个表中
    partition: Map[String, Option[String]],//分区信息
    child: LogicalPlan,//select查询出来的结果集
    overwrite: Boolean,//是否覆盖
    ifNotExists: Boolean) //表是否存在
  extends LogicalPlan {

  override def children: Seq[LogicalPlan] = child :: Nil
  override def output: Seq[Attribute] = Seq.empty //插入数据.不输出属性


  override lazy val resolved: Boolean = childrenResolved && child.output.zip(table.output).forall { //select的输出与 insert的输出 一对一关联
    case (childAttr, tableAttr) => //分别表示两个结果集
      DataType.equalsIgnoreCompatibleNullability(childAttr.dataType, tableAttr.dataType) //说明类型是可以相互转换的
  }
}
1.select 依然是跟第一种sql一样的解析
2.产生InsertIntoTable对象,该对象也是一个LogicalPlan
该对象包含以下属性内容:插入到哪个表、插入的分区信息、child为select节点的最终逻辑计划、是否覆盖、是否存在该表
3.输出没有,因为是插入操作,所以不会对外进行输出
4.override lazy val resolved
重新覆盖父类方法,因为要确保select中的输出与insert 表中的字段要一对一关联上,并且字段类型也要一致

--------------
高级sql
一、select union all select
case class Union(left: LogicalPlan, right: LogicalPlan) extends SetOperation(left, right) {
  override def statistics: Statistics = {
    val sizeInBytes = left.statistics.sizeInBytes + right.statistics.sizeInBytes
    Statistics(sizeInBytes = sizeInBytes)
  }
}
1.因为两个select分别对应一个LogicalPlan,因此该表达式最终产生Union操作,该对象也是一个LogicalPlan
2.输出字段为left表的输出即可,因为左右两边表输出的字段相同,因此考虑左边的输出属性即可
3.final override lazy val resolved方法,重新覆盖父类方法,因为要增加校验左边和右边的字段数量以及类型要一致

二、select INTERSECT select 获取两个集合的交集
1.因为两个select分别对应一个LogicalPlan,因此该表达式最终产生Intersect操作,该对象也是一个LogicalPlan
2.输出字段为left表的输出即可,因为左右两边表输出的字段相同,因此考虑左边的输出属性即可
3.final override lazy val resolved方法,重新覆盖父类方法,因为要增加校验左边和右边的字段数量以及类型要一致

三、select EXCEPT select 获取两个集合的差集
1.因为两个select分别对应一个LogicalPlan,因此该表达式最终产生EXCEPT操作,该对象也是一个LogicalPlan
2.输出字段为left表的输出即可,因为左右两边表输出的字段相同,因此考虑左边的输出属性即可
3.final override lazy val resolved方法,重新覆盖父类方法,因为要增加校验左边和右边的字段数量以及类型要一致

四、select UNION select 或者  select UNION DISTINCT select
1.因为是union操作,所以肯定与高级sql的1是一样的
2.在1的输出结果Union的基础上进一步过滤,增加了Distinct对象,用于过滤重复数据
case class Distinct(child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output //distinct不会改变子类的输出属性
}
 */
class SqlParser extends AbstractSparkSQLParser with DataTypeParser {

  //解析表达式
  def parseExpression(input: String): Expression = {
    // Initialize the Keywords.
    initLexical //语法解析器
    phrase(projection)(new lexical.Scanner(input)) match {//表示从input中去匹配projection正则表达式的内容
      case Success(plan, _) => plan
      case failureOrError => sys.error(failureOrError.toString)
    }
  }

  //解析databases.tablename表名字
  def parseTableIdentifier(input: String): TableIdentifier = {
    // Initialize the Keywords.
    initLexical
    phrase(tableIdentifier)(new lexical.Scanner(input)) match {//表示从input中去匹配tableIdentifier正则表达式的内容
      case Success(ident, _) => ident
      case failureOrError => sys.error(failureOrError.toString)
    }
  }

  // Keyword is a convention with AbstractSparkSQLParser, which will scan all of the `Keyword`
  // properties via reflection the class in runtime for constructing the SqlLexical object
  protected val ALL = Keyword("ALL")
  protected val AND = Keyword("AND")
  protected val APPROXIMATE = Keyword("APPROXIMATE")
  protected val AS = Keyword("AS")
  protected val ASC = Keyword("ASC")
  protected val BETWEEN = Keyword("BETWEEN")
  protected val BY = Keyword("BY")
  protected val CASE = Keyword("CASE")
  protected val CAST = Keyword("CAST")
  protected val DESC = Keyword("DESC")
  protected val DISTINCT = Keyword("DISTINCT")
  protected val ELSE = Keyword("ELSE")
  protected val END = Keyword("END")
  protected val EXCEPT = Keyword("EXCEPT")
  protected val FALSE = Keyword("FALSE")
  protected val FROM = Keyword("FROM")
  protected val FULL = Keyword("FULL")
  protected val GROUP = Keyword("GROUP")
  protected val HAVING = Keyword("HAVING")
  protected val IN = Keyword("IN")
  protected val INNER = Keyword("INNER")
  protected val INSERT = Keyword("INSERT")
  protected val INTERSECT = Keyword("INTERSECT")
  protected val INTERVAL = Keyword("INTERVAL")
  protected val INTO = Keyword("INTO")
  protected val IS = Keyword("IS")
  protected val JOIN = Keyword("JOIN")
  protected val LEFT = Keyword("LEFT")
  protected val LIKE = Keyword("LIKE")
  protected val LIMIT = Keyword("LIMIT")
  protected val NOT = Keyword("NOT")
  protected val NULL = Keyword("NULL")
  protected val ON = Keyword("ON")
  protected val OR = Keyword("OR")
  protected val ORDER = Keyword("ORDER")
  protected val SORT = Keyword("SORT")
  protected val OUTER = Keyword("OUTER")
  protected val OVERWRITE = Keyword("OVERWRITE")
  protected val REGEXP = Keyword("REGEXP")
  protected val RIGHT = Keyword("RIGHT")
  protected val RLIKE = Keyword("RLIKE")
  protected val SELECT = Keyword("SELECT")
  protected val SEMI = Keyword("SEMI")
  protected val TABLE = Keyword("TABLE")
  protected val THEN = Keyword("THEN")
  protected val TRUE = Keyword("TRUE")
  protected val UNION = Keyword("UNION")
  protected val WHEN = Keyword("WHEN")
  protected val WHERE = Keyword("WHERE")
  protected val WITH = Keyword("WITH")

  //入口,先优先解析这三类
  protected lazy val start: Parser[LogicalPlan] =
    start1 | insert | cte

  protected lazy val start1: Parser[LogicalPlan] =
    (select | ("(" ~> select <~ ")")) * //纯粹的一个完整的sql 或者子查询中完成的sql   *表示若干个
    ( UNION ~ ALL        ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Union(q1, q2) }//union操作  两个select进行union
    | INTERSECT          ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Intersect(q1, q2) }//两个集合做交集
    | EXCEPT             ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Except(q1, q2)} //获取两个集合的差集
    | UNION ~ DISTINCT.? ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Distinct(Union(q1, q2)) }//union操作后过滤重复---其中DISTINCT可以不存在
    )

  //将select sql 转换成一个逻辑执行计划
  protected lazy val select: Parser[LogicalPlan] =
    SELECT ~> DISTINCT.? ~ //select distinct(可能不存在)
      repsep(projection, ",") ~ //解析表达式  表达式 as 别名 集合,多个列使用逗号分割
      (FROM   ~> relations).? ~ //要查询的表
      (WHERE  ~> expression).? ~ //where条件
      (GROUP  ~  BY ~> rep1sep(expression, ",")).? ~ //group by 多个表达式用逗号拆分
      (HAVING ~> expression).? ~ //having表达式
      sortType.? ~ //order by 或者 sort by
      (LIMIT  ~> expression).? ^^ { //limit 表达式
        case d ~ p ~ r ~ f ~ g ~ h ~ o ~ l => //d表示是否distinct,   p表示select的属性集合   r 表示要查询的表集合  f表示where条件的一个表达式   g表示gruop by的表达式集合  h表示having对应的一个表达式   o表示rrder by表达式   l表示limit 表达式
          val base = r.getOrElse(OneRowRelation) //查询索要的表
          val withFilter = f.map(Filter(_, base)).getOrElse(base) //where条件,表达式 针对一个表child 结果进行过滤    没有where条件,则返回base原始表   , Filter(_, base)的结果依然是逻辑计划,因此相当于在原来的计划上进行过滤,filter是父,原来的base是子
          val withProjection = g
            .map(Aggregate(_, p.map(UnresolvedAlias(_)), withFilter)) //循环g的每一个表达式,每一个表达式转换成Aggregate对象
            .getOrElse(Project(p.map(UnresolvedAlias(_)), withFilter))//在过滤的结果集上进行group by 聚合

          val withDistinct = d.map(_ => Distinct(withProjection)).getOrElse(withProjection)
          val withHaving = h.map(Filter(_, withDistinct)).getOrElse(withDistinct) //在group by 的基础上进行having过滤

          val withOrder = o.map(_(withHaving)).getOrElse(withHaving) //o是一个函数,参数是LogicalPlan,返回值也是LogicalPlan,因此_(withHaving)的含义就是调用order by 函数,参数就是已经过滤到现在的结果集,返回排序后的结果集
          val withLimit = l.map(Limit(_, withOrder)).getOrElse(withOrder)
          withLimit
      }

  //insert OVERWRITE/INTO TABLE biao(或者join的结果集合) select逻辑计划
  protected lazy val insert: Parser[LogicalPlan] = //处理inster语法城市一个逻辑执行计划
    INSERT ~> (OVERWRITE ^^^ true | INTO ^^^ false) ~ (TABLE ~> relation) ~ select ^^ {
      case o ~ r ~ s => InsertIntoTable(r, Map.empty[String, Option[String]], s, o, false) //o表示OVERWRITE  r表示 table对应的表  s表示select部分的语法
    }

  //表达式 with ident as (sql),(sql)  或者 sql 或者insert sql
  protected lazy val cte: Parser[LogicalPlan] =
    WITH ~> rep1sep(ident ~ ( AS ~ "(" ~> start1 <~ ")"), ",") ~ (start1 | insert) ^^ {
      case r ~ s => With(s, r.map({case n ~ s => (n, Subquery(n, s))}).toMap) //子查询操作
    }

  //解析表达式  表达式 as 别名
  protected lazy val projection: Parser[Expression] =
    expression ~ (AS.? ~> ident.?) ^^ { //as ident是可以不存在的
      case e ~ a => a.fold(e)(Alias(e, _)()) //a.fold(e) 表示a别名如何为null,没有设置,则使用e表达式就当名字,如果e有设置,则执行(Alias(e, _)()) 即将表达式e 起一个别名为a
    }

  // Based very loosely on the MySQL Grammar.
  // http://dev.mysql.com/doc/refman/5.0/en/join.html
  //解析join
  protected lazy val relations: Parser[LogicalPlan] =
    ( relation ~ rep1("," ~> relation) ^^ { //使用以前常用的方式,不用join,就是用逗号分隔每一个表,或者子查询,比如from a,b 也没有on条件,也没有join方式,因此是默认采用inner join,on条件为null
        case r1 ~ joins => joins.foldLeft(r1) { case(lhs, r) => Join(lhs, r, Inner, None) } } //默认是inner join,其中None 表示关联条件
    | relation
    )

  //数据来源,比如来源于一个表,或者子查询产生的结果,或者多个表进行join,总之产生一个逻辑计划
  protected lazy val relation: Parser[LogicalPlan] =
    joinedRelation | relationFactor


  //相当于from biao  相当于没有join的表
  protected lazy val relationFactor: Parser[LogicalPlan] =
    ( rep1sep(ident, ".") ~ (opt(AS) ~> opt(ident)) ^^ { //ident.ident.ident（若干,一般就是两个最多,表示数据库.表名） as(可有可无)  ident(别名,可有可无)
        case tableIdent ~ alias => UnresolvedRelation(tableIdent, alias)
      }
      | ("(" ~> start <~ ")") ~ (AS.? ~> ident) ^^ { case s ~ a => Subquery(a, s) } //子查询 s表示一个完整的子查询sql,a表示别名
    )

  //可以多个表进行join,  相当于多个表参与join
  protected lazy val joinedRelation: Parser[LogicalPlan] =
    relationFactor ~ rep1(joinType.? ~ (JOIN ~> relationFactor) ~ joinConditions.?) ^^ { //表1 (join 表2 on 条件) 可以同时join多张表,因此用()围上,表示可以重复
      case r1 ~ joins => //r1表示基础表,joins 表示要join的所有表集合,每一个元素表示join类型、join哪个表 、 什么条件
        joins.foldLeft(r1) { case (lhs, jt ~ rhs ~ cond) => //循环joins所有的表,基础是r1,lhs 表示每次join一个表后的结果,jt表示join类型、rhs 表示要join的新表、cond表示join条件
          Join(lhs, rhs, joinType = jt.getOrElse(Inner), cond) //join类型默认是inner join
        }
    }

  //join的条件在on后面接入表达式
  protected lazy val joinConditions: Parser[Expression] =
    ON ~> expression //expression 表示一个总大表达式

  protected lazy val joinType: Parser[JoinType] =
    ( INNER           ^^^ Inner
    | LEFT  ~ SEMI    ^^^ LeftSemi
    | LEFT  ~ OUTER.? ^^^ LeftOuter
    | RIGHT ~ OUTER.? ^^^ RightOuter
    | FULL  ~ OUTER.? ^^^ FullOuter
    )

  //排序支持order by和sort by  返回从一个计划到另外一个计划映射函数
  protected lazy val sortType: Parser[LogicalPlan => LogicalPlan] =
    ( ORDER ~ BY  ~> ordering ^^ { case o => l: LogicalPlan => Sort(o, true, l) } //返回值o解析后是排序的表达式集合,然后调用sort方法进行排序,将原有逻辑计划l传进去
    | SORT ~ BY  ~> ordering ^^ { case o => l: LogicalPlan => Sort(o, false, l) }
    )

  //按照哪些表达式或者属性进行order by sort by操作
  protected lazy val ordering: Parser[Seq[SortOrder]] = //返回表达式集合
    ( rep1sep(expression ~ direction.? , ",") ^^ { //order by 跟的表达式一组用逗号拆分的
        case exps => exps.map(pair => SortOrder(pair._1, pair._2.getOrElse(Ascending)))
      }
    )

  //排序方式
  protected lazy val direction: Parser[SortDirection] =
    ( ASC  ^^^ Ascending
    | DESC ^^^ Descending
    )

  //表达式集合
  protected lazy val expression: Parser[Expression] =
    orExpression

  //表达式集合,一个and表达式 与 or表达式进行交互
  protected lazy val orExpression: Parser[Expression] =
    andExpression * (OR ^^^ { (e1: Expression, e2: Expression) => Or(e1, e2) })

  //比较表达式 与 一组and 表达式交互
  protected lazy val andExpression: Parser[Expression] =
    comparisonExpression * (AND ^^^ { (e1: Expression, e2: Expression) => And(e1, e2) })

  //比较表达式
  protected lazy val comparisonExpression: Parser[Expression] =
    ( termExpression ~ ("="  ~> termExpression) ^^ { case e1 ~ e2 => EqualTo(e1, e2) }
    | termExpression ~ ("<"  ~> termExpression) ^^ { case e1 ~ e2 => LessThan(e1, e2) }
    | termExpression ~ ("<=" ~> termExpression) ^^ { case e1 ~ e2 => LessThanOrEqual(e1, e2) }
    | termExpression ~ (">"  ~> termExpression) ^^ { case e1 ~ e2 => GreaterThan(e1, e2) }
    | termExpression ~ (">=" ~> termExpression) ^^ { case e1 ~ e2 => GreaterThanOrEqual(e1, e2) }
    | termExpression ~ ("!=" ~> termExpression) ^^ { case e1 ~ e2 => Not(EqualTo(e1, e2)) }
    | termExpression ~ ("<>" ~> termExpression) ^^ { case e1 ~ e2 => Not(EqualTo(e1, e2)) }
    | termExpression ~ ("<=>" ~> termExpression) ^^ { case e1 ~ e2 => EqualNullSafe(e1, e2) }
    | termExpression ~ NOT.? ~ (BETWEEN ~> termExpression) ~ (AND ~> termExpression) ^^ {
        case e ~ not ~ el ~ eu =>
          val betweenExpr: Expression = And(GreaterThanOrEqual(e, el), LessThanOrEqual(e, eu)) //即表达式e >= el and e <=eu
          not.fold(betweenExpr)(f => Not(betweenExpr)) //在between基础上 获取not非操作
      }//表达式 BETWEEN 表达式 and 表达式 或者 表达式 not BETWEEN 表达式 and 表达式
    | termExpression ~ (RLIKE  ~> termExpression) ^^ { case e1 ~ e2 => RLike(e1, e2) } //表达式RLIKE 表达式
    | termExpression ~ (REGEXP ~> termExpression) ^^ { case e1 ~ e2 => RLike(e1, e2) } //表达式REGEXP  表达式
    | termExpression ~ (LIKE   ~> termExpression) ^^ { case e1 ~ e2 => Like(e1, e2) } //表达式Like操作
    | termExpression ~ (NOT ~ LIKE ~> termExpression) ^^ { case e1 ~ e2 => Not(Like(e1, e2)) } //表达式 not Like操作
    | termExpression ~ (IN ~ "(" ~> rep1sep(termExpression, ",")) <~ ")" ^^ {
        case e1 ~ e2 => In(e1, e2)
      } //表达式 in 表达式 运算
    | termExpression ~ (NOT ~ IN ~ "(" ~> rep1sep(termExpression, ",")) <~ ")" ^^ {
        case e1 ~ e2 => Not(In(e1, e2))
      } //表达式not in 表达式 运算
    | termExpression <~ IS ~ NULL ^^ { case e => IsNull(e) } //表达式 IS null
    | termExpression <~ IS ~ NOT ~ NULL ^^ { case e => IsNotNull(e) } //表达式 IS not null
    | NOT ~> termExpression ^^ {e => Not(e)} //非运算,即not 一个表达式
    | termExpression //可以是表达式本身,即常量
    )

  protected lazy val termExpression: Parser[Expression] =
    productExpression *
      ( "+" ^^^ { (e1: Expression, e2: Expression) => Add(e1, e2) }
      | "-" ^^^ { (e1: Expression, e2: Expression) => Subtract(e1, e2) }
      )

  //基本表达式与 * / 等交互
  protected lazy val productExpression: Parser[Expression] =
    baseExpression *
      ( "*" ^^^ { (e1: Expression, e2: Expression) => Multiply(e1, e2) }
      | "/" ^^^ { (e1: Expression, e2: Expression) => Divide(e1, e2) }
      | "%" ^^^ { (e1: Expression, e2: Expression) => Remainder(e1, e2) }
      | "&" ^^^ { (e1: Expression, e2: Expression) => BitwiseAnd(e1, e2) } //表达式 & 逻辑与运算
      | "|" ^^^ { (e1: Expression, e2: Expression) => BitwiseOr(e1, e2) }//表达式 | 逻辑或运算
      | "^" ^^^ { (e1: Expression, e2: Expression) => BitwiseXor(e1, e2) } //表达式 ^ 逻辑非运算
      )

  //函数表达式
  protected lazy val function: Parser[Expression] =
    ( ident <~ ("(" ~ "*" ~ ")") ^^ { case udfName => //函数名字  即count(*)
      if (lexical.normalizeKeyword(udfName) == "count") {
        Count(Literal(1))
      } else {
        throw new AnalysisException(s"invalid expression $udfName(*)")
      }
    }
    | ident ~ ("(" ~> repsep(expression, ",")) <~ ")" ^^
      { case udfName ~ exprs => UnresolvedFunction(udfName, exprs, isDistinct = false) } //表示一个函数名字 + 一组表达式集合
    | ident ~ ("(" ~ DISTINCT ~> repsep(expression, ",")) <~ ")" ^^ { case udfName ~ exprs =>
      lexical.normalizeKeyword(udfName) match {
        case "sum" => SumDistinct(exprs.head) //sum(distinct(表达式))操作
        case "count" => CountDistinct(exprs) //count(distinct(表达式集合))
        case _ => UnresolvedFunction(udfName, exprs, isDistinct = true)
      }
    }
    | APPROXIMATE ~> ident ~ ("(" ~ DISTINCT ~> expression <~ ")") ^^ { case udfName ~ exp =>
      if (lexical.normalizeKeyword(udfName) == "count") {
        ApproxCountDistinct(exp) //APPROXIMATE count(DISTINCT(表达式))  预估count计算
      } else {
        throw new AnalysisException(s"invalid function approximate $udfName")
      }
    }
    | APPROXIMATE ~> "(" ~> unsignedFloat ~ ")" ~ ident ~ "(" ~ DISTINCT ~ expression <~ ")" ^^
      { case s ~ _ ~ udfName ~ _ ~ _ ~ exp =>
        if (lexical.normalizeKeyword(udfName) == "count") {
          ApproxCountDistinct(exp, s.toDouble)
        } else {
          throw new AnalysisException(s"invalid function approximate($s) $udfName")
        }
      }//APPROXIMATE (float) count (distinct 表达式)
    | CASE ~> whenThenElse ^^ CaseWhen //case when 表达式 then 表达式 else 表达式 end 操作
    | CASE ~> expression ~ whenThenElse ^^ //case 表达式 when 表达式 then 表达式 else 表达式 end 操作
      { case keyPart ~ branches => CaseKeyWhen(keyPart, branches) }
    )

  //when 表达式 then 表达式 else 表达式 end 操作
  protected lazy val whenThenElse: Parser[List[Expression]] =
    rep1(WHEN ~> expression ~ (THEN ~> expression)) ~ (ELSE ~> expression).? <~ END ^^ {
      case altPart ~ elsePart =>
        altPart.flatMap { case whenExpr ~ thenExpr =>
          Seq(whenExpr, thenExpr)
        } ++ elsePart
    }

  //cast (表达式 as dataType) 语法,强制转换成一个类型
  protected lazy val cast: Parser[Expression] =
    CAST ~ "(" ~> expression ~ (AS ~> dataType) <~ ")" ^^ {
      case exp ~ t => Cast(exp, t)
    }

//识别的有意义的文字
  protected lazy val literal: Parser[Literal] =
    ( numericLiteral //数字,整数或者小数
    | booleanLiteral //只有true和false字符串
    | stringLit ^^ {case s => Literal.create(s, StringType) } //字符串
    | intervalLiteral //周期 比如3 hour
    | NULL ^^^ Literal.create(null, NullType) //null字符串
    )

  //只有true和false字符串
  protected lazy val booleanLiteral: Parser[Literal] =
    ( TRUE ^^^ Literal.create(true, BooleanType)
    | FALSE ^^^ Literal.create(false, BooleanType)
    )

  //整数或者小数组成的字符串
  protected lazy val numericLiteral: Parser[Literal] =
    ( integral  ^^ { case i => Literal(toNarrowestIntegerType(i)) }
    | sign.? ~ unsignedFloat ^^ {//+或者1 表示的小数
      case s ~ f => Literal(toDecimalOrDouble(s.getOrElse("") + f))
    }
    )

  //float或者.456形式组成的字符串
  protected lazy val unsignedFloat: Parser[String] =
    ( "." ~> numericLit ^^ { u => "0." + u }//.456转换成0.456
    | elem("decimal", _.isInstanceOf[lexical.FloatLit]) ^^ (_.chars)
    )

  //+或者-表示的小数
  protected lazy val sign: Parser[String] = ("+" | "-")

  protected lazy val integral: Parser[String] =
    sign.? ~ numericLit ^^ { case s ~ n => s.getOrElse("") + n }

  private def intervalUnit(unitName: String) =
    acceptIf {
      case lexical.Identifier(str) =>
        val normalized = lexical.normalizeKeyword(str)
        normalized == unitName || normalized == unitName + "s"
      case _ => false
    } {_ => "wrong interval unit"}

  //数字+单位
  protected lazy val month: Parser[Int] =
    integral <~ intervalUnit("month") ^^ { case num => num.toInt }

  protected lazy val year: Parser[Int] =
    integral <~ intervalUnit("year") ^^ { case num => num.toInt * 12 } //年转换成多少个月

  protected lazy val microsecond: Parser[Long] =
    integral <~ intervalUnit("microsecond") ^^ { case num => num.toLong } //微妙

  protected lazy val millisecond: Parser[Long] =
    integral <~ intervalUnit("millisecond") ^^ {
      case num => num.toLong * CalendarInterval.MICROS_PER_MILLI //毫秒转换成微妙
    }

  protected lazy val second: Parser[Long] =
    integral <~ intervalUnit("second") ^^ {
      case num => num.toLong * CalendarInterval.MICROS_PER_SECOND //秒转换成微妙
    }

  protected lazy val minute: Parser[Long] =
    integral <~ intervalUnit("minute") ^^ {
      case num => num.toLong * CalendarInterval.MICROS_PER_MINUTE //分转换成微妙
    }

  protected lazy val hour: Parser[Long] =
    integral <~ intervalUnit("hour") ^^ {
      case num => num.toLong * CalendarInterval.MICROS_PER_HOUR //小时转换成微妙
    }

  protected lazy val day: Parser[Long] =
    integral <~ intervalUnit("day") ^^ {
      case num => num.toLong * CalendarInterval.MICROS_PER_DAY //天转换成微妙
    }

  protected lazy val week: Parser[Long] =
    integral <~ intervalUnit("week") ^^ {
      case num => num.toLong * CalendarInterval.MICROS_PER_WEEK //周转换成微妙
    }

  //周期 比如3 hour
  protected lazy val intervalLiteral: Parser[Literal] =
    INTERVAL ~> year.? ~ month.? ~ week.? ~ day.? ~ hour.? ~ minute.? ~ second.? ~
      millisecond.? ~ microsecond.? ^^ {
        case year ~ month ~ week ~ day ~ hour ~ minute ~ second ~
          millisecond ~ microsecond =>
          if (!Seq(year, month, week, day, hour, minute, second,
            millisecond, microsecond).exists(_.isDefined)) {
            throw new AnalysisException(
              "at least one time unit should be given for interval literal")
          }
          val months = Seq(year, month).map(_.getOrElse(0)).sum //汇总一共多少个月
          val microseconds = Seq(week, day, hour, minute, second, millisecond, microsecond)
            .map(_.getOrElse(0L)).sum //汇总多少微妙
          Literal.create(new CalendarInterval(months, microseconds), CalendarIntervalType) //使用多少个月  + 多少微妙表示一个具体的时间
      }

  //将数字类型的字符串转换成double 或者int 或者long
  private def toNarrowestIntegerType(value: String): Any = {
    val bigIntValue = BigDecimal(value) //将字符串转换成double

    bigIntValue match {
      case v if bigIntValue.isValidInt => v.toIntExact
      case v if bigIntValue.isValidLong => v.toLongExact
      case v => v.underlying()
    }
  }

  //将字符串转换成double
  private def toDecimalOrDouble(value: String): Any = {
    val decimal = BigDecimal(value)
    // follow the behavior in MS SQL Server
    // https://msdn.microsoft.com/en-us/library/ms179899.aspx
    if (value.contains('E') || value.contains('e')) {
      decimal.doubleValue()
    } else {
      decimal.underlying()
    }
  }

  //基本表达式
  protected lazy val baseExpression: Parser[Expression] =
    ( "*" ^^^ UnresolvedStar(None) //表达一个*
    | ident <~ "." ~ "*" ^^ { case tableName => UnresolvedStar(Option(tableName)) } //表达tableName.*
    | primary //或者有意义的内容
    )

  //- 表达式 或者 表达式
  protected lazy val signedPrimary: Parser[Expression] =
    sign ~ primary ^^ { case s ~ e => if (s == "-") UnaryMinus(e) else e }

  //返回属性名字  --- 第二个函数是一个偏函数,将任意元素转换成String
  protected lazy val attributeName: Parser[String] = acceptMatch("attribute name", {
    case lexical.Identifier(str) => str //属性name字符串
    case lexical.Keyword(str) if !lexical.delimiters.contains(str) => str //返回关键字
  })

  //支持的全部sql内容
  protected lazy val primary: PackratParser[Expression] =
    ( literal //识别的有意义的文字
    | expression ~ ("[" ~> expression <~ "]") ^^
      { case base ~ ordinal => UnresolvedExtractValue(base, ordinal) } // 表达式 [表达式] 语法  表示抽取信息表达式
    | (expression <~ ".") ~ ident ^^
      { case base ~ fieldName => UnresolvedExtractValue(base, Literal(fieldName)) }  // 表达式.fieldName语法 语法  表示抽取信息表达式
    | cast //类型转换函数
    | "(" ~> expression <~ ")" //(表达式),即一个表达式被()括起来
    | function //函数
    | dotExpressionHeader //name.name.name的方式确定一个属性
    | signedPrimary //- 表达式 或者 表达式
    | "~" ~> expression ^^ BitwiseNot //~ 表达式
    | attributeName ^^ UnresolvedAttribute.quoted //表示属性名字
    )

  //name.name.name的方式确定一个属性 前两个必须存在
  protected lazy val dotExpressionHeader: Parser[Expression] =
    (ident <~ ".") ~ ident ~ rep("." ~> ident) ^^ {
      case i1 ~ i2 ~ rest => UnresolvedAttribute(Seq(i1, i2) ++ rest)
    }

  //匹配 database.table 可能database不存在
  protected lazy val tableIdentifier: Parser[TableIdentifier] =
  //(ident <~ ".").?表示.前面是一个任意字符,?表示这段可以没有
    (ident <~ ".").? ~ ident ^^ {
      case maybeDbName ~ tableName => TableIdentifier(tableName, maybeDbName)
    }
}
