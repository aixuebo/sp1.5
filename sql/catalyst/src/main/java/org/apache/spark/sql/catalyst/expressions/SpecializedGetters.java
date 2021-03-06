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

package org.apache.spark.sql.catalyst.expressions;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.ArrayData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.MapData;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;
//从数组中获取一个元素,并且返回的是该元素对应的类型
public interface SpecializedGetters {

  boolean isNullAt(int ordinal); //true表示该位置是否是null

  boolean getBoolean(int ordinal);

  byte getByte(int ordinal);

  short getShort(int ordinal);

  int getInt(int ordinal);

  long getLong(int ordinal);

  float getFloat(int ordinal);

  double getDouble(int ordinal);

  Decimal getDecimal(int ordinal, int precision, int scale);

  UTF8String getUTF8String(int ordinal);

  byte[] getBinary(int ordinal);

  CalendarInterval getInterval(int ordinal);

  InternalRow getStruct(int ordinal, int numFields);//获取第ordinal列,该列是struct组成的行,有numFields个属性

  ArrayData getArray(int ordinal);

  MapData getMap(int ordinal);

    //获取第ordinal个元素,该元素是dataType类型的
  Object get(int ordinal, DataType dataType);
}
