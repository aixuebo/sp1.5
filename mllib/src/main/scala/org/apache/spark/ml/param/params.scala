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

package org.apache.spark.ml.param

import java.lang.reflect.Modifier
import java.util.NoSuchElementException

import scala.annotation.varargs
import scala.collection.mutable
import scala.collection.JavaConverters._

import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.ml.util.Identifiable

/**
 * :: DeveloperApi ::
 * A param with self-contained documentation and optionally default value. Primitive-typed param
 * should use the specialized versions, which are more friendly to Java users.
 *
 * @param parent parent object 该属性属于哪个父的属性
 * @param name param name 该属性的key
 * @param doc documentation 该属性的描述信息
 * @param isValid optional validation method which indicates if a value is valid. 一个函数,表示T对象对应的值是否合法,true表示校验通过
 *                See [[ParamValidators]] for factory methods for common validation functions.
 * @tparam T param value type
 */
@DeveloperApi
class Param[T](val parent: String, val name: String, val doc: String, val isValid: T => Boolean)
  extends Serializable {

  def this(parent: Identifiable, name: String, doc: String, isValid: T => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue[T])

  def this(parent: Identifiable, name: String, doc: String) = this(parent.uid, name, doc)

  /**
   * Assert that the given value is valid for this parameter.
   *
   * Note: Parameter checks involving interactions between multiple parameters should be
   *       implemented in [[Params.validateParams()]].  Checks for input/output columns should be
   *       implemented in [[org.apache.spark.ml.PipelineStage.transformSchema()]].
   *
   * DEVELOPERS: This method is only called by [[ParamPair]], which means that all parameters
   *             should be specified via [[ParamPair]].
   *
   * @throws IllegalArgumentException if the value is invalid
   * 校验参数T是否合法,不合法则抛异常
   */
  private[param] def validate(value: T): Unit = {
    if (!isValid(value)) {
      throw new IllegalArgumentException(s"$parent parameter $name given invalid value $value.")
    }
  }

  /** Creates a param pair with the given value (for Java).
    * 为该属性设置一个T值
    **/
  def w(value: T): ParamPair[T] = this -> value

  /** Creates a param pair with the given value (for Scala).
    * 为该属性设置一个T值
    **/
  def ->(value: T): ParamPair[T] = ParamPair(this, value)

  override final def toString: String = s"${parent}__$name"

  override final def hashCode: Int = toString.##

  override final def equals(obj: Any): Boolean = {
    obj match {
      case p: Param[_] => (p.parent == parent) && (p.name == name)
      case _ => false
    }
  }
}

/**
 * :: DeveloperApi ::
 * Factory methods for common validation functions for [[Param.isValid]].
 * The numerical methods only support Int, Long, Float, and Double.
 * 一些校验规则的工厂类
 */
@DeveloperApi
object ParamValidators {

  /** (private[param]) Default validation always return true
    * 无论属性值T是任何内容,都返回true,即都校验成功
    **/
  private[param] def alwaysTrue[T]: T => Boolean = (_: T) => true

  /**
   * Private method for checking numerical types and converting to Double.
   * This is mainly for the sake of compilation; type checks are really handled
   * by [[Params]] setters and the [[ParamPair]] constructor.
   * 将参数T转换成double类型
   */
  private def getDouble[T](value: T): Double = value match {
    case x: Int => x.toDouble
    case x: Long => x.toDouble
    case x: Float => x.toDouble
    case x: Double => x.toDouble
    case _ =>
      // The type should be checked before this is ever called.
      throw new IllegalArgumentException("Numerical Param validation failed because" +
        s" of unexpected input type: ${value.getClass}")
  }

  /** Check if value > lowerBound
    * 必须参数值T是double类型的,并且大于lowerBound
    **/
  def gt[T](lowerBound: Double): T => Boolean = { (value: T) =>
    getDouble(value) > lowerBound
  }

  /** Check if value >= lowerBound
    * 必须参数值T是double类型的,并且>=lowerBound
    **/
  def gtEq[T](lowerBound: Double): T => Boolean = { (value: T) =>
    getDouble(value) >= lowerBound
  }

  /** Check if value < upperBound */
  def lt[T](upperBound: Double): T => Boolean = { (value: T) =>
    getDouble(value) < upperBound
  }

  /** Check if value <= upperBound */
  def ltEq[T](upperBound: Double): T => Boolean = { (value: T) =>
    getDouble(value) <= upperBound
  }

  /**
   * Check for value in range lowerBound to upperBound.
   * @param lowerInclusive  If true, check for value >= lowerBound.
   *                        If false, check for value > lowerBound.
   * @param upperInclusive  If true, check for value <= upperBound.
   *                        If false, check for value < upperBound.
   * 必须参数值T是double类型的,并且在lowerBound和upperBound区间内
   */
  def inRange[T](
      lowerBound: Double,
      upperBound: Double,
      lowerInclusive: Boolean,//true表示 >= false表示>
      upperInclusive: Boolean) //true 表示 <= false表示 <
     : T => Boolean = { (value: T) =>
    val x: Double = getDouble(value)
    val lowerValid = if (lowerInclusive) x >= lowerBound else x > lowerBound
    val upperValid = if (upperInclusive) x <= upperBound else x < upperBound
    lowerValid && upperValid
  }

  /** Version of [[inRange()]] which uses inclusive be default: [lowerBound, upperBound] */
  def inRange[T](lowerBound: Double, upperBound: Double): T => Boolean = {
    inRange[T](lowerBound, upperBound, lowerInclusive = true, upperInclusive = true)
  }

  /** Check for value in an allowed set of values.
    * 表示参数值T必须在数组中存在,才返回true
    **/
  def inArray[T](allowed: Array[T]): T => Boolean = { (value: T) =>
    allowed.contains(value)
  }

  /** Check for value in an allowed set of values. */
  def inArray[T](allowed: java.util.List[T]): T => Boolean = { (value: T) =>
    allowed.contains(value)
  }

  /** Check that the array length is greater than lowerBound.
    * 校验参数值必须是数组类型的T,并且数组的长度 > lowerBound
    **/
  def arrayLengthGt[T](lowerBound: Double): Array[T] => Boolean = { (value: Array[T]) =>
    value.length > lowerBound
  }
}

// specialize primitive-typed params because Java doesn't recognize scala.Double, scala.Int, ...

/**
 * :: DeveloperApi ::
 * Specialized version of [[Param[Double]]] for Java.
 * 表示属性值必须是double类型的属性
 */
@DeveloperApi
class DoubleParam(parent: String, name: String, doc: String, isValid: Double => Boolean)
  extends Param[Double](parent, name, doc, isValid) {

  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  def this(parent: Identifiable, name: String, doc: String, isValid: Double => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) = this(parent.uid, name, doc)

  /** Creates a param pair with the given value (for Java). */
  override def w(value: Double): ParamPair[Double] = super.w(value)
}

/**
 * :: DeveloperApi ::
 * Specialized version of [[Param[Int]]] for Java.
 */
@DeveloperApi
class IntParam(parent: String, name: String, doc: String, isValid: Int => Boolean)
  extends Param[Int](parent, name, doc, isValid) {

  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  def this(parent: Identifiable, name: String, doc: String, isValid: Int => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) = this(parent.uid, name, doc)

  /** Creates a param pair with the given value (for Java). */
  override def w(value: Int): ParamPair[Int] = super.w(value)
}

/**
 * :: DeveloperApi ::
 * Specialized version of [[Param[Float]]] for Java.
 */
@DeveloperApi
class FloatParam(parent: String, name: String, doc: String, isValid: Float => Boolean)
  extends Param[Float](parent, name, doc, isValid) {

  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  def this(parent: Identifiable, name: String, doc: String, isValid: Float => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) = this(parent.uid, name, doc)

  /** Creates a param pair with the given value (for Java). */
  override def w(value: Float): ParamPair[Float] = super.w(value)
}

/**
 * :: DeveloperApi ::
 * Specialized version of [[Param[Long]]] for Java.
 */
@DeveloperApi
class LongParam(parent: String, name: String, doc: String, isValid: Long => Boolean)
  extends Param[Long](parent, name, doc, isValid) {

  def this(parent: String, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  def this(parent: Identifiable, name: String, doc: String, isValid: Long => Boolean) =
    this(parent.uid, name, doc, isValid)

  def this(parent: Identifiable, name: String, doc: String) = this(parent.uid, name, doc)

  /** Creates a param pair with the given value (for Java). */
  override def w(value: Long): ParamPair[Long] = super.w(value)
}

/**
 * :: DeveloperApi ::
 * Specialized version of [[Param[Boolean]]] for Java.
 */
@DeveloperApi
class BooleanParam(parent: String, name: String, doc: String) // No need for isValid
  extends Param[Boolean](parent, name, doc) {

  def this(parent: Identifiable, name: String, doc: String) = this(parent.uid, name, doc)

  /** Creates a param pair with the given value (for Java). */
  override def w(value: Boolean): ParamPair[Boolean] = super.w(value)
}

/**
 * :: DeveloperApi ::
 * Specialized version of [[Param[Array[String]]]] for Java.
 * 表示属性值必须是 Array[String] 类型的属性值
 */
@DeveloperApi
class StringArrayParam(parent: Params, name: String, doc: String, isValid: Array[String] => Boolean)
  extends Param[Array[String]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  /** Creates a param pair with a [[java.util.List]] of values (for Java and Python). */
  def w(value: java.util.List[String]): ParamPair[Array[String]] = w(value.asScala.toArray)
}

/**
 * :: DeveloperApi ::
 * Specialized version of [[Param[Array[Double]]]] for Java.
 */
@DeveloperApi
class DoubleArrayParam(parent: Params, name: String, doc: String, isValid: Array[Double] => Boolean)
  extends Param[Array[Double]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  /** Creates a param pair with a [[java.util.List]] of values (for Java and Python). */
  def w(value: java.util.List[java.lang.Double]): ParamPair[Array[Double]] =
    w(value.asScala.map(_.asInstanceOf[Double]).toArray)
}

/**
 * :: DeveloperApi ::
 * Specialized version of [[Param[Array[Int]]]] for Java.
 */
@DeveloperApi
class IntArrayParam(parent: Params, name: String, doc: String, isValid: Array[Int] => Boolean)
  extends Param[Array[Int]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  /** Creates a param pair with a [[java.util.List]] of values (for Java and Python). */
  def w(value: java.util.List[java.lang.Integer]): ParamPair[Array[Int]] =
    w(value.asScala.map(_.asInstanceOf[Int]).toArray)
}

/**
 * :: Experimental ::
 * A param and its value.
 * 表示一个属性name和属性值
 */
@Experimental
case class ParamPair[T](param: Param[T], value: T) {
  // This is *the* place Param.validate is called.  Whenever a parameter is specified, we should
  // always construct a ParamPair so that validate is called.
  param.validate(value)
}

/**
 * :: DeveloperApi ::
 * Trait for components that take parameters. This also provides an internal param map to store
 * parameter values attached to the instance.
 * 定义一组参数集合
 */
@DeveloperApi
trait Params extends Identifiable with Serializable {

  /**
   * Returns all params sorted by their names. The default implementation uses Java reflection to
   * list all public methods that have no arguments and return [[Param]].
   *
   * Note: Developer should not use this method in constructor because we cannot guarantee that
   * this variable gets initialized before other params.
   * 反射获取所有的属性集合
   */
  lazy val params: Array[Param[_]] = {
    val methods = this.getClass.getMethods
    methods.filter { m =>
        Modifier.isPublic(m.getModifiers) &&
          classOf[Param[_]].isAssignableFrom(m.getReturnType) &&
          m.getParameterTypes.isEmpty
      }.sortBy(_.getName)
      .map(m => m.invoke(this).asInstanceOf[Param[_]])
  }

  /**
   * Validates parameter values stored internally.
   * Raise an exception if any parameter value is invalid.
   *
   * This only needs to check for interactions between parameters.
   * Parameter value checks which do not depend on other parameters are handled by
   * [[Param.validate()]].  This method does not handle input/output column parameters;
   * those are checked during schema validation.
   */
  def validateParams(): Unit = {
    // Do nothing by default.  Override to handle Param interactions.
  }

  /**
   * Explains a param.
   * @param param input param, must belong to this instance.
   * @return a string that contains the input param name, doc, and optionally its default value and
   *         the user-supplied value
   * 给定一个参数,
   */
  def explainParam(param: Param[_]): String = {
    shouldOwn(param)
    val valueStr = if (isDefined(param)) {
      val defaultValueStr = getDefault(param).map("default: " + _)//属性的默认值
      val currentValueStr = get(param).map("current: " + _) //属性当前值
      (defaultValueStr ++ currentValueStr).mkString("(", ", ", ")")
    } else {
      "(undefined)"
    }
    s"${param.name}: ${param.doc} $valueStr"
  }

  /**
   * Explains all params of this instance.
   * @see [[explainParam()]]
   */
  def explainParams(): String = {
    params.map(explainParam).mkString("\n")
  }

  /** Checks whether a param is explicitly set. */
  final def isSet(param: Param[_]): Boolean = {
    shouldOwn(param)
    paramMap.contains(param)
  }

  /** Checks whether a param is explicitly set or has a default value.
    * true表示该参数定义了
    **/
  final def isDefined(param: Param[_]): Boolean = {
    shouldOwn(param)
    defaultParamMap.contains(param) || paramMap.contains(param)
  }

  /** Tests whether this instance contains a param with a given name.
    * true表示有这个参数name
    **/
  def hasParam(paramName: String): Boolean = {
    params.exists(_.name == paramName)
  }

  /** Gets a param by its name.
    * 通过name获取该参数对象
    **/
  def getParam(paramName: String): Param[Any] = {
    params.find(_.name == paramName).getOrElse {
      throw new NoSuchElementException(s"Param $paramName does not exist.")
    }.asInstanceOf[Param[Any]]
  }

  /**
   * Sets a parameter in the embedded param map.
   */
  protected final def set[T](param: Param[T], value: T): this.type = {
    set(param -> value) //param -> value 组装成参数name=value形式的对象 ParamPair
  }

  /**
   * Sets a parameter (by name) in the embedded param map.
   * 为参数name设置value值
   */
  protected final def set(param: String, value: Any): this.type = {
    set(getParam(param), value)
  }

  /**
   * Sets a parameter in the embedded param map.
   * 设置一个参数name和value值
   */
  protected final def set(paramPair: ParamPair[_]): this.type = {
    shouldOwn(paramPair.param)
    paramMap.put(paramPair)
    this
  }

  /**
   * Optionally returns the user-supplied value of a param.
   * 获得属性值
   */
  final def get[T](param: Param[T]): Option[T] = {
    shouldOwn(param)
    paramMap.get(param)
  }

  /**
   * Clears the user-supplied value for the input param.
   * 删除该属性
   */
  protected final def clear(param: Param[_]): this.type = {
    shouldOwn(param)
    paramMap.remove(param)
    this
  }

  /**
   * Gets the value of a param in the embedded param map or its default value. Throws an exception
   * if neither is set.
   * 获取该属性值
   */
  final def getOrDefault[T](param: Param[T]): T = {
    shouldOwn(param)
    get(param).orElse(getDefault(param)).get
  }

  /** An alias for [[getOrDefault()]].
    * 获取该属性值
    **/
  protected final def $[T](param: Param[T]): T = getOrDefault(param)

  /**
   * Sets a default value for a param.
   * @param param  param to set the default value. Make sure that this param is initialized before
   *               this method gets called.
   * @param value  the default value
   * 设置属性的默认值
   */
  protected final def setDefault[T](param: Param[T], value: T): this.type = {
    defaultParamMap.put(param -> value)
    this
  }

  /**
   * Sets default values for a list of params.
   *
   * Note: Java developers should use the single-parameter [[setDefault()]].
   *       Annotating this with varargs can cause compilation failures due to a Scala compiler bug.
   *       See SPARK-9268.
   *
   * @param paramPairs  a list of param pairs that specify params and their default values to set
   *                    respectively. Make sure that the params are initialized before this method
   *                    gets called.
   *  设置属性的默认值
   */
  protected final def setDefault(paramPairs: ParamPair[_]*): this.type = {
    paramPairs.foreach { p =>
      setDefault(p.param.asInstanceOf[Param[Any]], p.value)
    }
    this
  }

  /**
   * Gets the default value of a parameter.
   * 获取属性的默认值
   */
  final def getDefault[T](param: Param[T]): Option[T] = {
    shouldOwn(param)
    defaultParamMap.get(param)
  }

  /**
   * Tests whether the input param has a default value set.
   * 判断该属性是否有默认值
   */
  final def hasDefault[T](param: Param[T]): Boolean = {
    shouldOwn(param)
    defaultParamMap.contains(param)
  }

  /**
   * Creates a copy of this instance with the same UID and some extra params.
   * Subclasses should implement this method and set the return type properly.
   *
   * @see [[defaultCopy()]]
   */
  def copy(extra: ParamMap): Params

  /**
   * Default implementation of copy with extra params.
   * It tries to create a new instance with the same UID.
   * Then it copies the embedded and extra parameters over and returns the new instance.
   */
  protected final def defaultCopy[T <: Params](extra: ParamMap): T = {
    val that = this.getClass.getConstructor(classOf[String]).newInstance(uid)
    copyValues(that, extra).asInstanceOf[T]
  }

  /**
   * Extracts the embedded default param values and user-supplied values, and then merges them with
   * extra values from input into a flat param map, where the latter value is used if there exist
   * conflicts, i.e., with ordering: default param values < user-supplied values < extra.
   * 合并三个集合.注意合并有优先级顺序
   */
  final def extractParamMap(extra: ParamMap): ParamMap = {
    defaultParamMap ++ paramMap ++ extra
  }

  /**
   * [[extractParamMap]] with no extra values.
   * 获取所有的属性集合
   */
  final def extractParamMap(): ParamMap = {
    extractParamMap(ParamMap.empty)
  }

  /** Internal param map for user-supplied values.
    * 内部的所有参数集合
    **/
  private val paramMap: ParamMap = ParamMap.empty

  /** Internal param map for default values.
    * 设置属性的默认值
    **/
  private val defaultParamMap: ParamMap = ParamMap.empty

  /** Validates that the input param belongs to this instance.
    * 校验该参数是自己的参数
    * 1.param.parent == uid  表示该参数的父类是跟自己序号一样,说明该参数有权限
    * 2.hasParam(param.name) true表示有这个参数name
    **/
  private def shouldOwn(param: Param[_]): Unit = {
    require(param.parent == uid && hasParam(param.name), s"Param $param does not belong to $this.")
  }

  /**
   * Copies param values from this instance to another instance for params shared by them.
   *
   * This handles default Params and explicitly set Params separately.
   * Default Params are copied from and to [[defaultParamMap]], and explicitly set Params are
   * copied from and to [[paramMap]].
   * Warning: This implicitly assumes that this [[Params]] instance and the target instance
   *          share the same set of default Params.
   *
   * @param to the target instance, which should work with the same set of default Params as this
   *           source instance
   * @param extra extra params to be copied to the target's [[paramMap]]
   * @return the target instance with param values copied
   */
  protected def copyValues[T <: Params](to: T, extra: ParamMap = ParamMap.empty): T = {
    val map = paramMap ++ extra
    params.foreach { param =>
      // copy default Params
      if (defaultParamMap.contains(param) && to.hasParam(param.name)) {
        to.defaultParamMap.put(to.getParam(param.name), defaultParamMap(param))
      }
      // copy explicitly set Params
      if (map.contains(param) && to.hasParam(param.name)) {
        to.set(param.name, map(param))
      }
    }
    to
  }
}

/**
 * :: DeveloperApi ::
 * Java-friendly wrapper for [[Params]].
 * Java developers who need to extend [[Params]] should use this class instead.
 * If you need to extend a abstract class which already extends [[Params]], then that abstract
 * class should be Java-friendly as well.
 */
@DeveloperApi
abstract class JavaParams extends Params

/**
 * :: Experimental ::
 * A param to value map.
 * 参数和参数值的映射
 */
@Experimental
final class ParamMap private[ml] (private val map: mutable.Map[Param[Any], Any])
  extends Serializable {

  /* DEVELOPERS: About validating parameter values
   *   This and ParamPair are the only two collections of parameters.
   *   This class should always create ParamPairs when
   *   specifying new parameter values.  ParamPair will then call Param.validate().
   */

  /**
   * Creates an empty param map.
   */
  def this() = this(mutable.Map.empty)

  /**
   * Puts a (param, value) pair (overwrites if the input param exists).
   * 设置参数和参数值
   */
  def put[T](param: Param[T], value: T): this.type = put(param -> value)

  /**
   * Puts a list of param pairs (overwrites if the input params exists).
   * 设置一组参数对象集合
   */
  @varargs
  def put(paramPairs: ParamPair[_]*): this.type = {
    paramPairs.foreach { p =>
      map(p.param.asInstanceOf[Param[Any]]) = p.value
    }
    this
  }

  /**
   * Optionally returns the value associated with a param.
   * 获取一个参数对应的值
   */
  def get[T](param: Param[T]): Option[T] = {
    map.get(param.asInstanceOf[Param[Any]]).asInstanceOf[Option[T]]
  }

  /**
   * Returns the value associated with a param or a default value.
   */
  def getOrElse[T](param: Param[T], default: T): T = {
    get(param).getOrElse(default)
  }

  /**
   * Gets the value of the input param or its default value if it does not exist.
   * Raises a NoSuchElementException if there is no value associated with the input param.
   * 获取参数对应的值
   */
  def apply[T](param: Param[T]): T = {
    get(param).getOrElse {
      throw new NoSuchElementException(s"Cannot find param ${param.name}.")
    }
  }

  /**
   * Checks whether a parameter is explicitly specified.
   * 判断是否存在该参数
   */
  def contains(param: Param[_]): Boolean = {
    map.contains(param.asInstanceOf[Param[Any]])
  }

  /**
   * Removes a key from this map and returns its value associated previously as an option.
   * 移除一个参数
   */
  def remove[T](param: Param[T]): Option[T] = {
    map.remove(param.asInstanceOf[Param[Any]]).asInstanceOf[Option[T]]
  }

  /**
   * Filters this param map for the given parent.
   * 找到参数是父的所有子属性集合
   */
  def filter(parent: Params): ParamMap = {
    val filtered = map.filterKeys(_.parent == parent)
    new ParamMap(filtered.asInstanceOf[mutable.Map[Param[Any], Any]])
  }

  /**
   * Creates a copy of this param map.
   */
  def copy: ParamMap = new ParamMap(map.clone())

  //所有属性集合输出字符串
  override def toString: String = {
    map.toSeq.sortBy(_._1.name).map { case (param, value) =>
      s"\t${param.parent}-${param.name}: $value"
    }.mkString("{\n", ",\n", "\n}")
  }

  /**
   * Returns a new param map that contains parameters in this map and the given map,
   * where the latter overwrites this if there exist conflicts.
   * 两个map集合相加,产生一个新的集合
   */
  def ++(other: ParamMap): ParamMap = {
    // TODO: Provide a better method name for Java users.
    new ParamMap(this.map ++ other.map)
  }

  /**
   * Adds all parameters from the input param map into this param map.
   * 两个map集合相加
   */
  def ++=(other: ParamMap): this.type = {
    // TODO: Provide a better method name for Java users.
    this.map ++= other.map
    this
  }

  /**
   * Converts this param map to a sequence of param pairs.
   */
  def toSeq: Seq[ParamPair[_]] = {
    map.toSeq.map { case (param, value) =>
      ParamPair(param, value)
    }
  }

  /**
   * Number of param pairs in this map.
   */
  def size: Int = map.size
}

@Experimental
object ParamMap {

  /**
   * Returns an empty param map.
   */
  def empty: ParamMap = new ParamMap()

  /**
   * Constructs a param map by specifying its entries.
   */
  @varargs
  def apply(paramPairs: ParamPair[_]*): ParamMap = {
    new ParamMap().put(paramPairs: _*)
  }
}
