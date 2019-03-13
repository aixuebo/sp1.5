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

package org.apache.spark.ml.feature

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{DoubleType, StructType}

/**
 * :: Experimental ::
 * A one-hot encoder that maps a column of category indices to a column of binary vectors, with
 * at most a single one-value per row that indicates the input category index.
 * For example with 5 categories, an input value of 2.0 would map to an output vector of
 * `[0.0, 0.0, 1.0, 0.0]`.
 * The last category is not included by default (configurable via [[OneHotEncoder!.dropLast]]
 * because it makes the vector entries sum up to one, and hence linearly dependent.
 * So an input value of 4.0 maps to `[0.0, 0.0, 0.0, 0.0]`.
 * Note that this is different from scikit-learn's OneHotEncoder, which keeps all categories.
 * The output vectors are sparse.
  * onehot 是将离散型的属性值 映射到多个字段中，每一个字段只表示一种类型
 *
 * @see [[StringIndexer]] for converting categorical values into category indices
 */
@Experimental
class OneHotEncoder(override val uid: String) extends Transformer
  with HasInputCol with HasOutputCol {

  def this() = this(Identifiable.randomUID("oneHot"))

  /**
   * Whether to drop the last category in the encoded vector (default: true)
   * @group param
    * 是否丢掉最后一个分类类型
    * 默认值是true,会丢掉最后一个元素,因此容易出现bug
   */
  final val dropLast: BooleanParam =
    new BooleanParam(this, "dropLast", "whether to drop the last category")
  setDefault(dropLast -> true)

  /** @group setParam */
  def setDropLast(value: Boolean): this.type = set(dropLast, value)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transformSchema(schema: StructType): StructType = {
    val inputColName = $(inputCol) //输入列
    val outputColName = $(outputCol) //输出列

    SchemaUtils.checkColumnType(schema, inputColName, DoubleType) //校验存在输入字段，并且字段类型是double
    val inputFields = schema.fields
    require(!inputFields.exists(_.name == outputColName),
      s"Output column $outputColName already exists.") //确定输出列不存在

    val inputAttr = Attribute.fromStructField(schema(inputColName)) //获取输入列对应的元数据
    val outputAttrNames: Option[Array[String]] = inputAttr match {//离散型数值对应的string类型内容
      case nominal: NominalAttribute =>
        if (nominal.values.isDefined) {
          nominal.values
        } else if (nominal.numValues.isDefined) {
          nominal.numValues.map(n => Array.tabulate(n)(_.toString))
        } else {
          None
        }
      case binary: BinaryAttribute =>
        if (binary.values.isDefined) {
          binary.values
        } else {
          Some(Array.tabulate(2)(_.toString))
        }
      case _: NumericAttribute =>
        throw new RuntimeException(
          s"The input column $inputColName cannot be numeric.")
      case _ =>
        None // optimistic about unknown attributes
    }

    val filteredOutputAttrNames = outputAttrNames.map { names =>
      if ($(dropLast)) {
        require(names.length > 1,
          s"The input column $inputColName should have at least two distinct values.")
        names.dropRight(1)
      } else {
        names
      }
    }

    //定义一个输出类型为向量vector类型的字段
    val outputAttrGroup = if (filteredOutputAttrNames.isDefined) { //如果该类型里面有分类具体的值,则设置Attribute的name
      val attrs: Array[Attribute] = filteredOutputAttrNames.get.map { name =>
        BinaryAttribute.defaultAttr.withName(name)
      }
      new AttributeGroup($(outputCol), attrs)
    } else {
      new AttributeGroup($(outputCol)) //没有具体的值,则创建一个单独的vector向量即可
    }

    val outputFields = inputFields :+ outputAttrGroup.toStructField()
    StructType(outputFields)
  }

  override def transform(dataset: DataFrame): DataFrame = {
    // schema transformation
    val inputColName = $(inputCol)
    val outputColName = $(outputCol)
    val shouldDropLast = $(dropLast)
    var outputAttrGroup = AttributeGroup.fromStructField(
      transformSchema(dataset.schema)(outputColName)) //校验 & 构造最终的dataFrame数据结构,返回输出的vector列
    if (outputAttrGroup.size < 0) {
      // If the number of attributes is unknown, we check the values from the input column.
      //因为不知道分类数量是多少,因此要进行一次查询
      val numAttrs = dataset.select(col(inputColName).cast(DoubleType)).map(_.getDouble(0))
        //获取最大的index值,这里假设的是序号都是连续从0开始存在的
        .aggregate(0.0)(
          (m, x) => {
            assert(x >=0.0 && x == x.toInt,//即x必须是>0的元素
              s"Values from column $inputColName must be indices, but got $x.")
            math.max(m, x)
          },
          (m0, m1) => {
            math.max(m0, m1)
          }
        ).toInt + 1
      val outputAttrNames = Array.tabulate(numAttrs)(_.toString)
      val filtered = if (shouldDropLast) outputAttrNames.dropRight(1) else outputAttrNames
      val outputAttrs: Array[Attribute] =
        filtered.map(name => BinaryAttribute.defaultAttr.withName(name))
      outputAttrGroup = new AttributeGroup(outputColName, outputAttrs)
    }
    val metadata = outputAttrGroup.toMetadata()

    // data transformation
    val size = outputAttrGroup.size
    val oneValue = Array(1.0)
    val emptyValues = Array[Double]()
    val emptyIndices = Array[Int]()

    //将分类的int类型转换成vector类型
    val encode = udf { label: Double =>
      if (label < size) {
        Vectors.sparse(size, Array(label.toInt), oneValue) //将该位置设置成1即可
      } else {//超出范围的设置空
        Vectors.sparse(size, emptyIndices, emptyValues)
      }
    }

    dataset.select(col("*"), //原始列集合
      encode(col(inputColName).cast(DoubleType)).as(outputColName, metadata)) //输出vector新添加的列对象,注意:输入一定是double类型的
  }

  override def copy(extra: ParamMap): OneHotEncoder = defaultCopy(extra)
}
