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

import java.util.regex.{MatchResult, Pattern}

import org.apache.commons.lang3.StringEscapeUtils

import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

//第一个参数是字符串内容作为返回值的表达式,第二个参数就是需要匹配的正则表达式内容
trait StringRegexExpression extends ImplicitCastInputTypes {
  self: BinaryExpression => //二元表达式,即两个输入 一个输出

  def escape(v: String): String//格式化正则表达式
  def matches(regex: Pattern, str: String): Boolean //返回字符传是否匹配正则表达式

  override def dataType: DataType = BooleanType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType) //输入类型

  // try cache the pattern for Literal
  private lazy val cache: Pattern = right match {
    case x @ Literal(value: String, StringType) => compile(value) //对字符串类型的数据直接创建正则表达式
    case _ => null
  }

  //创建正则表达式对象
  protected def compile(str: String): Pattern = if (str == null) {
    null
  } else {
    // Let it raise exception if couldn't compile the regex string
    Pattern.compile(escape(str))
  }

  //创建正则表达式
  protected def pattern(str: String) = if (cache == null) compile(str) else cache

  //两个表达式对应的String结果作为参数
  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    val regex = pattern(input2.asInstanceOf[UTF8String].toString) //获取第二个正则表达式对象
    if(regex == null) {
      null
    } else {
      matches(regex, input1.asInstanceOf[UTF8String].toString) //第一个参数是否匹配正则表达式
    }
  }
}


/**
 * Simple RegEx pattern matching function
 * 最终表达式为 left like right  即left是字符串内容作为返回值的表达式,right是正则表达式内容
 */
case class Like(left: Expression, right: Expression)
  extends BinaryExpression with StringRegexExpression with CodegenFallback {

  override def escape(v: String): String = StringUtils.escapeLikeRegex(v)

  override def matches(regex: Pattern, str: String): Boolean = regex.matcher(str).matches() //该字符串是否匹配正则表达式

  override def toString: String = s"$left LIKE $right"

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val patternClass = classOf[Pattern].getName
    val escapeFunc = StringUtils.getClass.getName.stripSuffix("$") + ".escapeLikeRegex"
    val pattern = ctx.freshName("pattern")

    if (right.foldable) {
      val rVal = right.eval()
      if (rVal != null) {
        val regexStr =
          StringEscapeUtils.escapeJava(escape(rVal.asInstanceOf[UTF8String].toString()))
        ctx.addMutableState(patternClass, pattern,
          s"""$pattern = ${patternClass}.compile("$regexStr");""")

        // We don't use nullSafeCodeGen here because we don't want to re-evaluate right again.
        val eval = left.gen(ctx)
        s"""
          ${eval.code}
          boolean ${ev.isNull} = ${eval.isNull};
          ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
          if (!${ev.isNull}) {
            ${ev.primitive} = $pattern.matcher(${eval.primitive}.toString()).matches();
          }
        """
      } else {
        s"""
          boolean ${ev.isNull} = true;
          ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
        """
      }
    } else {
      nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
        s"""
          String rightStr = ${eval2}.toString();
          ${patternClass} $pattern = ${patternClass}.compile($escapeFunc(rightStr));
          ${ev.primitive} = $pattern.matcher(${eval1}.toString()).matches();
        """
      })
    }
  }
}

//表达式RLIKE 表达式  或者 表达式REGEXP  表达式
//即left是字符串内容返回值的表达式,right是正则表达式内容
case class RLike(left: Expression, right: Expression)
  extends BinaryExpression with StringRegexExpression with CodegenFallback {

  override def escape(v: String): String = v
  override def matches(regex: Pattern, str: String): Boolean = regex.matcher(str).find(0)
  override def toString: String = s"$left RLIKE $right"

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val patternClass = classOf[Pattern].getName
    val pattern = ctx.freshName("pattern")

    if (right.foldable) {
      val rVal = right.eval()
      if (rVal != null) {
        val regexStr =
          StringEscapeUtils.escapeJava(rVal.asInstanceOf[UTF8String].toString())
        ctx.addMutableState(patternClass, pattern,
          s"""$pattern = ${patternClass}.compile("$regexStr");""")

        // We don't use nullSafeCodeGen here because we don't want to re-evaluate right again.
        val eval = left.gen(ctx)
        s"""
          ${eval.code}
          boolean ${ev.isNull} = ${eval.isNull};
          ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
          if (!${ev.isNull}) {
            ${ev.primitive} = $pattern.matcher(${eval.primitive}.toString()).find(0);
          }
        """
      } else {
        s"""
          boolean ${ev.isNull} = true;
          ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
        """
      }
    } else {
      nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
        s"""
          String rightStr = ${eval2}.toString();
          ${patternClass} $pattern = ${patternClass}.compile(rightStr);
          ${ev.primitive} = $pattern.matcher(${eval1}.toString()).find(0);
        """
      })
    }
  }
}


/**
 * Splits str around pat (pattern is a regular expression).
 * 即left是字符串内容返回值的表达式,right是正则表达式内容
 * 最终语法:字符串.split(正则表达式),返回数组
 */
case class StringSplit(str: Expression, pattern: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = str
  override def right: Expression = pattern
  override def dataType: DataType = ArrayType(StringType) //返回值是字符串
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType) //输入类型是字符串

  override def nullSafeEval(string: Any, regex: Any): Any = {
    val strings = string.asInstanceOf[UTF8String].split(regex.asInstanceOf[UTF8String], -1) //最终语法:字符串.split(正则表达式)  返回值是数组
    new GenericArrayData(strings.asInstanceOf[Array[Any]]) //返回数组
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val arrayClass = classOf[GenericArrayData].getName
    nullSafeCodeGen(ctx, ev, (str, pattern) =>
      // Array in java is covariant, so we don't need to cast UTF8String[] to Object[].
      s"""${ev.primitive} = new $arrayClass($str.split($pattern, -1));""")
  }

  override def prettyName: String = "split"
}


/**
 * Replace all substrings of str that match regexp with rep.
 * 对匹配的字符串进行替换
 * NOTE: this expression is not THREAD-SAFE, as it has some internal mutable status.
 * 注意该表达式不是线程安全的,因为有一些内部状态
 *
 * 三元函数 顺序是 原始字符串、 正则表达式、  要替换的文字
 */
case class RegExpReplace(subject: Expression, regexp: Expression, rep: Expression)
  extends TernaryExpression with ImplicitCastInputTypes {

  // last regex in string, we will update the pattern iff regexp value changed.
  @transient private var lastRegex: UTF8String = _ //最后一组正则表达式字符串内容
  // last regex pattern, we cache it for performance concern
  @transient private var pattern: Pattern = _ //最终表达式
  // last replacement string, we don't want to convert a UTF8String => java.langString every time.
  @transient private var lastReplacement: String = _ //要替换的文字
  @transient private var lastReplacementInUTF8: UTF8String = _
  // result buffer write by Matcher
  @transient private val result: StringBuffer = new StringBuffer //最终替换后的结果

  override def dataType: DataType = StringType //输出类型
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType, StringType) //输入类型
  override def children: Seq[Expression] = subject :: regexp :: rep :: Nil //三个表达式
  override def prettyName: String = "regexp_replace" //简单名字

  override def nullSafeEval(s: Any, p: Any, r: Any): Any = {
    if (!p.equals(lastRegex)) {
      // regex value changed
      lastRegex = p.asInstanceOf[UTF8String].clone()
      pattern = Pattern.compile(lastRegex.toString)
    }
    if (!r.equals(lastReplacementInUTF8)) {
      // replacement string changed
      lastReplacementInUTF8 = r.asInstanceOf[UTF8String].clone()
      lastReplacement = lastReplacementInUTF8.toString
    }
    val m = pattern.matcher(s.toString()) //正则表达式匹配字符串
    result.delete(0, result.length())

    while (m.find) {
      m.appendReplacement(result, lastReplacement)
    }
    m.appendTail(result)

    UTF8String.fromString(result.toString)
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val termLastRegex = ctx.freshName("lastRegex")
    val termPattern = ctx.freshName("pattern")

    val termLastReplacement = ctx.freshName("lastReplacement")
    val termLastReplacementInUTF8 = ctx.freshName("lastReplacementInUTF8")

    val termResult = ctx.freshName("result")

    val classNamePattern = classOf[Pattern].getCanonicalName
    val classNameStringBuffer = classOf[java.lang.StringBuffer].getCanonicalName

    ctx.addMutableState("UTF8String", termLastRegex, s"${termLastRegex} = null;")
    ctx.addMutableState(classNamePattern, termPattern, s"${termPattern} = null;")
    ctx.addMutableState("String", termLastReplacement, s"${termLastReplacement} = null;")
    ctx.addMutableState("UTF8String",
      termLastReplacementInUTF8, s"${termLastReplacementInUTF8} = null;")
    ctx.addMutableState(classNameStringBuffer,
      termResult, s"${termResult} = new $classNameStringBuffer();")

    nullSafeCodeGen(ctx, ev, (subject, regexp, rep) => {
    s"""
      if (!$regexp.equals(${termLastRegex})) {
        // regex value changed
        ${termLastRegex} = $regexp.clone();
        ${termPattern} = ${classNamePattern}.compile(${termLastRegex}.toString());
      }
      if (!$rep.equals(${termLastReplacementInUTF8})) {
        // replacement string changed
        ${termLastReplacementInUTF8} = $rep.clone();
        ${termLastReplacement} = ${termLastReplacementInUTF8}.toString();
      }
      ${termResult}.delete(0, ${termResult}.length());
      java.util.regex.Matcher m = ${termPattern}.matcher($subject.toString());

      while (m.find()) {
        m.appendReplacement(${termResult}, ${termLastReplacement});
      }
      m.appendTail(${termResult});
      ${ev.primitive} = UTF8String.fromString(${termResult}.toString());
      ${ev.isNull} = false;
    """
    })
  }
}

/**
 * Extract a specific(idx) group identified by a Java regex.
 * 抽取一个指定的group匹配的内容
 * NOTE: this expression is not THREAD-SAFE, as it has some internal mutable status.
 * 注意该表达式不是线程安全的,因为有一些内部状态
 *
 * 三元函数 顺序是 原始字符串、 正则表达式、要抽取哪个group
 */
case class RegExpExtract(subject: Expression, regexp: Expression, idx: Expression)
  extends TernaryExpression with ImplicitCastInputTypes {
  def this(s: Expression, r: Expression) = this(s, r, Literal(1))//默认抽取第1个group

  // last regex in string, we will update the pattern iff regexp value changed.
  @transient private var lastRegex: UTF8String = _ //最后的正则表达式内容
  // last regex pattern, we cache it for performance concern
  @transient private var pattern: Pattern = _ //匹配的正则表达式对象

  override def dataType: DataType = StringType
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType, IntegerType)
  override def children: Seq[Expression] = subject :: regexp :: idx :: Nil
  override def prettyName: String = "regexp_extract"

  override def nullSafeEval(s: Any, p: Any, r: Any): Any = {
    if (!p.equals(lastRegex)) {
      // regex value changed
      lastRegex = p.asInstanceOf[UTF8String].clone()
      pattern = Pattern.compile(lastRegex.toString)
    }
    val m = pattern.matcher(s.toString)
    if (m.find) {
      val mr: MatchResult = m.toMatchResult
      UTF8String.fromString(mr.group(r.asInstanceOf[Int])) //获取指定group的内容
    } else {
      UTF8String.EMPTY_UTF8
    }
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val termLastRegex = ctx.freshName("lastRegex")
    val termPattern = ctx.freshName("pattern")
    val classNamePattern = classOf[Pattern].getCanonicalName

    ctx.addMutableState("UTF8String", termLastRegex, s"${termLastRegex} = null;")
    ctx.addMutableState(classNamePattern, termPattern, s"${termPattern} = null;")

    nullSafeCodeGen(ctx, ev, (subject, regexp, idx) => {
      s"""
      if (!$regexp.equals(${termLastRegex})) {
        // regex value changed
        ${termLastRegex} = $regexp.clone();
        ${termPattern} = ${classNamePattern}.compile(${termLastRegex}.toString());
      }
      java.util.regex.Matcher m =
        ${termPattern}.matcher($subject.toString());
      if (m.find()) {
        java.util.regex.MatchResult mr = m.toMatchResult();
        ${ev.primitive} = UTF8String.fromString(mr.group($idx));
        ${ev.isNull} = false;
      } else {
        ${ev.primitive} = UTF8String.EMPTY_UTF8;
        ${ev.isNull} = false;
      }"""
    })
  }
}
