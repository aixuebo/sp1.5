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
import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import scala.util.parsing.combinator.PackratParsers
import scala.util.parsing.input.CharArrayReader.EofCh

import org.apache.spark.sql.catalyst.plans.logical._

private[sql] abstract class AbstractSparkSQLParser
  extends StandardTokenParsers with PackratParsers {

  //开始解析,给定一个sql,返回执行的逻辑计划
  def parse(input: String): LogicalPlan = {
    // Initialize the Keywords.
    initLexical
    phrase(start)(new lexical.Scanner(input)) match {//使用sql语法解析器去扫描input输入内容
      case Success(plan, _) => plan //成功,则返回的就是逻辑执行计划
      case failureOrError => sys.error(failureOrError.toString)
    }
  }

  /* One time initialization of lexical.This avoid reinitialization of  lexical in parse method */
  protected lazy val initLexical: Unit = lexical.initialize(reservedWords) //通过反射,获取到Keyword关键案子的name集合,然后初始化给sql语法解析器,让其知道这个是关键词

  protected case class Keyword(str: String) {
    def normalize: String = lexical.normalizeKeyword(str) //对关键字进行格式化
    def parser: Parser[String] = normalize
  }

  protected implicit def asParser(k: Keyword): Parser[String] = k.parser

  // By default, use Reflection to find the reserved words defined in the sub class.
  // NOTICE, Since the Keyword properties defined by sub class, we couldn't call this
  // method during the parent class instantiation, because the sub class instance
  // isn't created yet.
  //通过反射,获取到Keyword关键案子的name集合
  protected lazy val reservedWords: Seq[String] =
    this
      .getClass
      .getMethods
      .filter(_.getReturnType == classOf[Keyword])
      .map(_.invoke(this).asInstanceOf[Keyword].normalize)

  // Set the keywords as empty by default, will change that later.
  override val lexical = new SqlLexical

  protected def start: Parser[LogicalPlan] //程序入口,返回解析后的逻辑计划

  // Returns the whole input string 返回整个的数据内容
  protected lazy val wholeInput: Parser[String] = new Parser[String] {
    def apply(in: Input): ParseResult[String] =
      Success(in.source.toString, in.drop(in.source.length()))
  }

  // Returns the rest of the input string that are not parsed yet 返回没有被解析的剩余的输入内容
  protected lazy val restInput: Parser[String] = new Parser[String] {
    def apply(in: Input): ParseResult[String] =
      Success(
        in.source.subSequence(in.offset, in.source.length()).toString,//从offset现在已经解析到的位置开始截取字符串
        in.drop(in.source.length()))
  }
}

//sql语法解析器
class SqlLexical extends StdLexical {
  case class FloatLit(chars: String) extends Token {
    override def toString: String = chars
  }

  /* This is a work around to support the lazy setting
  * 初始化关键字集合
  **/
  def initialize(keywords: Seq[String]): Unit = {
    reserved.clear()
    reserved ++= keywords
  }

  /* Normal the keyword string
  * 格式化字符串,都变成小写字符
  **/
  def normalizeKeyword(str: String): String = str.toLowerCase

  delimiters += ( //分隔符
    "@", "*", "+", "-", "<", "=", "<>", "!=", "<=", ">=", ">", "/", "(", ")",
    ",", ";", "%", "{", "}", ":", "[", "]", ".", "&", "|", "^", "~", "<=>"
  )

  //返回Token,要么是关键字,要么就是Identifier
  protected override def processIdent(name: String) = {
    val token = normalizeKeyword(name) // 格式化字符串,都变成小写字符
    if (reserved contains token) Keyword(token) else Identifier(name)
  }

  override lazy val token: Parser[Token] =
    ( identChar ~ (identChar | digit).* ^^
      { case first ~ rest => processIdent((first :: rest).mkString) }
    | digit.* ~ identChar ~ (identChar | digit).* ^^
      { case first ~ middle ~ rest => processIdent((first ++ (middle :: rest)).mkString) }
    | rep1(digit) ~ ('.' ~> digit.*).? ^^ {
        case i ~ None => NumericLit(i.mkString)
        case i ~ Some(d) => FloatLit(i.mkString + "." + d.mkString)
      }
    | '\'' ~> chrExcept('\'', '\n', EofCh).* <~ '\'' ^^
      { case chars => StringLit(chars mkString "") }
    | '"' ~> chrExcept('"', '\n', EofCh).* <~ '"' ^^
      { case chars => StringLit(chars mkString "") }
    | '`' ~> chrExcept('`', '\n', EofCh).* <~ '`' ^^
      { case chars => Identifier(chars mkString "") }
    | EofCh ^^^ EOF
    | '\'' ~> failure("unclosed string literal")
    | '"' ~> failure("unclosed string literal")
    | delim
    | failure("illegal character")
    )

  //是字符
  override def identChar: Parser[Elem] = letter | elem('_')

  //是空格
  override def whitespace: Parser[Any] =
    ( whitespaceChar
    | '/' ~ '*' ~ comment
    | '/' ~ '/' ~ chrExcept(EofCh, '\n').*
    | '#' ~ chrExcept(EofCh, '\n').*
    | '-' ~ '-' ~ chrExcept(EofCh, '\n').*
    | '/' ~ '*' ~ failure("unclosed comment")
    ).*
}

