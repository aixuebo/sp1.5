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

package org.apache.spark.sql.catalyst.util

import java.util.regex.Pattern

object StringUtils {

  // replace the _ with .{1} exactly match 1 time of any character
  // replace the % with .*, match 0 or more times with any character
  //将like语法转换成正则表达式
  def escapeLikeRegex(v: String): String = {
    if (!v.isEmpty) {
      "(?s)" + (' ' +: v.init).zip(v).flatMap {
        case (prev, '\\') => "" //取消转义字符\\
        case ('\\', c) => //说明转义字符后面跟着字符
          c match {
            case '_' => "_" //说明对_进行转义,因此原本需要的就是_,即他不是like语法
            case '%' => "%" //说明对%进行转义了,因此原本要的就是%
            case _ => Pattern.quote("\\" + c)
          }
        case (prev, c) =>
          c match {
            case '_' => "." //将_转换成. 表示支持任意字符
            case '%' => ".*" //将%转换成.*,表示支持任意多个字符
            case _ => Pattern.quote(Character.toString(c))
          }
      }.mkString
    } else {
      v
    }
  }
}
