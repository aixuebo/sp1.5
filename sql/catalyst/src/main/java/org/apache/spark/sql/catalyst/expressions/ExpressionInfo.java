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

/**
 * Expression information, will be used to describe a expression.
 * 用于描述一个表达式的信息
 */
public class ExpressionInfo {
    private String className;//Expression的类全路径
    private String usage;//Expression的Annotation--->ExpressionDescription.usage 简短的方式描述该函数
    private String name;//别名
    private String extended;//Expression的Annotation--->ExpressionDescription.extended 复杂的方式描述该函数,或者是该函数的一个demo例子

    public String getClassName() {
        return className;
    }

    public String getUsage() {
        return usage;
    }

    public String getName() {
        return name;
    }

    public String getExtended() {
        return extended;
    }

    public ExpressionInfo(String className, String name, String usage, String extended) {
        this.className = className;
        this.name = name;
        this.usage = usage;
        this.extended = extended;
    }

    public ExpressionInfo(String className, String name) {
        this(className, name, null, null);
    }
}
