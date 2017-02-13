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
 */
class SqlParser extends AbstractSparkSQLParser with DataTypeParser {

  //解析表达式
  def parseExpression(input: String): Expression = {
    // Initialize the Keywords.
    initLexical //语法解析器
    phrase(projection)(new lexical.Scanner(input)) match {
      case Success(plan, _) => plan
      case failureOrError => sys.error(failureOrError.toString)
    }
  }

  //解析唯一名称
  def parseTableIdentifier(input: String): TableIdentifier = {
    // Initialize the Keywords.
    initLexical
    phrase(tableIdentifier)(new lexical.Scanner(input)) match {
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
    (select | ("(" ~> select <~ ")")) * //纯粹的select 或者子查询中的select
    ( UNION ~ ALL        ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Union(q1, q2) }//union操作
    | INTERSECT          ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Intersect(q1, q2) }//两个集合做交集
    | EXCEPT             ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Except(q1, q2)}
    | UNION ~ DISTINCT.? ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Distinct(Union(q1, q2)) }//union操作后过滤重复
    )

  //将select sql 转换成一个逻辑执行计划
  protected lazy val select: Parser[LogicalPlan] =
    SELECT ~> DISTINCT.? ~
      repsep(projection, ",") ~
      (FROM   ~> relations).? ~
      (WHERE  ~> expression).? ~
      (GROUP  ~  BY ~> rep1sep(expression, ",")).? ~
      (HAVING ~> expression).? ~
      sortType.? ~
      (LIMIT  ~> expression).? ^^ {
        case d ~ p ~ r ~ f ~ g ~ h ~ o ~ l =>
          val base = r.getOrElse(OneRowRelation)
          val withFilter = f.map(Filter(_, base)).getOrElse(base)
          val withProjection = g
            .map(Aggregate(_, p.map(UnresolvedAlias(_)), withFilter))
            .getOrElse(Project(p.map(UnresolvedAlias(_)), withFilter))
          val withDistinct = d.map(_ => Distinct(withProjection)).getOrElse(withProjection)
          val withHaving = h.map(Filter(_, withDistinct)).getOrElse(withDistinct)
          val withOrder = o.map(_(withHaving)).getOrElse(withHaving)
          val withLimit = l.map(Limit(_, withOrder)).getOrElse(withOrder)
          withLimit
      }

  protected lazy val insert: Parser[LogicalPlan] = //处理inster语法城市一个逻辑执行计划
    INSERT ~> (OVERWRITE ^^^ true | INTO ^^^ false) ~ (TABLE ~> relation) ~ select ^^ {
      case o ~ r ~ s => InsertIntoTable(r, Map.empty[String, Option[String]], s, o, false)
    }

  //表达式 with ident as (sql),(sql)  或者 sql 或者insert sql
  protected lazy val cte: Parser[LogicalPlan] =
    WITH ~> rep1sep(ident ~ ( AS ~ "(" ~> start1 <~ ")"), ",") ~ (start1 | insert) ^^ {
      case r ~ s => With(s, r.map({case n ~ s => (n, Subquery(n, s))}).toMap) //子查询操作
    }

  //解析表达式   表达式 as 别名
  protected lazy val projection: Parser[Expression] =
    expression ~ (AS.? ~> ident.?) ^^ {
      case e ~ a => a.fold(e)(Alias(e, _)())
    }

  // Based very loosely on the MySQL Grammar.
  // http://dev.mysql.com/doc/refman/5.0/en/join.html
  //解析join
  protected lazy val relations: Parser[LogicalPlan] =
    ( relation ~ rep1("," ~> relation) ^^ {
        case r1 ~ joins => joins.foldLeft(r1) { case(lhs, r) => Join(lhs, r, Inner, None) } } //默认是inner join,其中None 表示关联条件
    | relation
    )

  protected lazy val relation: Parser[LogicalPlan] =
    joinedRelation | relationFactor

  protected lazy val relationFactor: Parser[LogicalPlan] =
    ( rep1sep(ident, ".") ~ (opt(AS) ~> opt(ident)) ^^ {
        case tableIdent ~ alias => UnresolvedRelation(tableIdent, alias)
      }
      | ("(" ~> start <~ ")") ~ (AS.? ~> ident) ^^ { case s ~ a => Subquery(a, s) }
    )

  protected lazy val joinedRelation: Parser[LogicalPlan] =
    relationFactor ~ rep1(joinType.? ~ (JOIN ~> relationFactor) ~ joinConditions.?) ^^ {
      case r1 ~ joins =>
        joins.foldLeft(r1) { case (lhs, jt ~ rhs ~ cond) =>
          Join(lhs, rhs, joinType = jt.getOrElse(Inner), cond)
        }
    }

  //join的条件在on后面接入表达式
  protected lazy val joinConditions: Parser[Expression] =
    ON ~> expression

  protected lazy val joinType: Parser[JoinType] =
    ( INNER           ^^^ Inner
    | LEFT  ~ SEMI    ^^^ LeftSemi
    | LEFT  ~ OUTER.? ^^^ LeftOuter
    | RIGHT ~ OUTER.? ^^^ RightOuter
    | FULL  ~ OUTER.? ^^^ FullOuter
    )

  //排序支持order by和sort by
  protected lazy val sortType: Parser[LogicalPlan => LogicalPlan] =
    ( ORDER ~ BY  ~> ordering ^^ { case o => l: LogicalPlan => Sort(o, true, l) }
    | SORT ~ BY  ~> ordering ^^ { case o => l: LogicalPlan => Sort(o, false, l) }
    )

  //按照哪些表达式或者属性进行order by sort by操作
  protected lazy val ordering: Parser[Seq[SortOrder]] =
    ( rep1sep(expression ~ direction.? , ",") ^^ {
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

  //返回属性名字
  protected lazy val attributeName: Parser[String] = acceptMatch("attribute name", {
    case lexical.Identifier(str) => str //属性name字符串
    case lexical.Keyword(str) if !lexical.delimiters.contains(str) => str
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

  //name.name.name的方式确定一个属性
  protected lazy val dotExpressionHeader: Parser[Expression] =
    (ident <~ ".") ~ ident ~ rep("." ~> ident) ^^ {
      case i1 ~ i2 ~ rest => UnresolvedAttribute(Seq(i1, i2) ++ rest)
    }

  //匹配 database.table 可能database不存在
  protected lazy val tableIdentifier: Parser[TableIdentifier] =
    (ident <~ ".").? ~ ident ^^ {
      case maybeDbName ~ tableName => TableIdentifier(tableName, maybeDbName)
    }
}
