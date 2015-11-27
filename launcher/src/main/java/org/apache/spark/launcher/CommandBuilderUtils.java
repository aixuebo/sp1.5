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

package org.apache.spark.launcher;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Helper methods for command builders.
 */
class CommandBuilderUtils {

  static final String DEFAULT_MEM = "1g";//默认内存,被用于设置-Xms,开启JVM的默认内存
  static final String DEFAULT_PROPERTIES_FILE = "spark-defaults.conf";//默认配置文件
  static final String ENV_SPARK_HOME = "SPARK_HOME"; //spark的安装目录
  static final String ENV_SPARK_ASSEMBLY = "_SPARK_ASSEMBLY";

  /** The set of known JVM vendors.
   * JVM提供商
   **/
  static enum JavaVendor {
    Oracle, IBM, OpenJDK, Unknown
  };

  /** Returns whether the given string is null or empty. */
  static boolean isEmpty(String s) {
    return s == null || s.isEmpty();
  }

  /** Joins a list of strings using the given separator. 
   * 使用第一个参数,将第二个数组参数组装成一个字符串返回
   **/
  static String join(String sep, String... elements) {
    StringBuilder sb = new StringBuilder();
    for (String e : elements) {
      if (e != null) {
        if (sb.length() > 0) {
          sb.append(sep);
        }
        sb.append(e);
      }
    }
    return sb.toString();
  }

  /** Joins a list of strings using the given separator. 
   * 使用第一个参数,将第二个集合参数组装成一个字符串返回
   **/
  static String join(String sep, Iterable<String> elements) {
    StringBuilder sb = new StringBuilder();
    for (String e : elements) {
      if (e != null) {
        if (sb.length() > 0) {
          sb.append(sep);
        }
        sb.append(e);
      }
    }
    return sb.toString();
  }

  /**
   * Returns the first non-empty value mapped to the given key in the given maps, or null otherwise.
   * 从一组map中查询key的值,返回第一个存在该key的value值,如果所有map该值不存在,则返回null
   */
  static String firstNonEmptyValue(String key, Map<?, ?>... maps) {
    for (Map<?, ?> map : maps) {//循环所有map集合
      String value = (String) map.get(key);
      if (!isEmpty(value)) {
        return value;
      }
    }
    return null;
  }

  /** Returns the first non-empty, non-null string in the given list, or null otherwise. 
   * 返回数组中第一个不是null的值
   **/
  static String firstNonEmpty(String... candidates) {
    for (String s : candidates) {
      if (!isEmpty(s)) {
        return s;
      }
    }
    return null;
  }

  /** Returns the name of the env variable that holds the native library path. 
   * 根据操作系统不同,获取对应的环境变量命令 
   **/
  static String getLibPathEnvName() {
    if (isWindows()) {
      return "PATH";
    }

    String os = System.getProperty("os.name");
    if (os.startsWith("Mac OS X")) {
      return "DYLD_LIBRARY_PATH";
    } else {
      return "LD_LIBRARY_PATH";
    }
  }

  /** Returns whether the OS is Windows. */
  static boolean isWindows() {
    String os = System.getProperty("os.name");
    return os.startsWith("Windows");
  }

  /** Returns an enum value indicating whose JVM is being used.
   * 获取JVM的提供方 
   **/
  static JavaVendor getJavaVendor() {
    String vendorString = System.getProperty("java.vendor");
    if (vendorString.contains("Oracle")) {
      return JavaVendor.Oracle;
    }
    if (vendorString.contains("IBM")) {
      return JavaVendor.IBM;
    }
    if (vendorString.contains("OpenJDK")) {
      return JavaVendor.OpenJDK;
    }
    return JavaVendor.Unknown;
  }

  /**
   * Updates the user environment, appending the given pathList to the existing value of the given
   * environment variable (or setting it if it hasn't yet been set).
   * 合并环境变量,使用;符号进行合并,类似于设置环境变量
   * 在当前环境中,追加;pathList
   * 从userEnv或者System中获取envKey对应的当前path,然后追加新的pathList
   */
  static void mergeEnvPathList(Map<String, String> userEnv, String envKey, String pathList) {
    if (!isEmpty(pathList)) {
      String current = firstNonEmpty(userEnv.get(envKey), System.getenv(envKey));
      userEnv.put(envKey, join(File.pathSeparator, current, pathList));
    }
  }

  /**
   * Parse a string as if it were a list of arguments, following bash semantics.
   * For example:
   *
   * Input: "\"ab cd\" efgh 'i \" j'"
   * Output: [ "ab cd", "efgh", "i \" j" ]
   */
  static List<String> parseOptionString(String s) {
    List<String> opts = new ArrayList<String>();
    StringBuilder opt = new StringBuilder();
    boolean inOpt = false;
    boolean inSingleQuote = false;
    boolean inDoubleQuote = false;
    boolean escapeNext = false;

    // This is needed to detect when a quoted empty string is used as an argument ("" or '').
    boolean hasData = false;

    for (int i = 0; i < s.length(); i++) {
      int c = s.codePointAt(i);
      if (escapeNext) {
        opt.appendCodePoint(c);
        escapeNext = false;
      } else if (inOpt) {
        switch (c) {
        case '\\':
          if (inSingleQuote) {
            opt.appendCodePoint(c);
          } else {
            escapeNext = true;
          }
          break;
        case '\'':
          if (inDoubleQuote) {
            opt.appendCodePoint(c);
          } else {
            inSingleQuote = !inSingleQuote;
          }
          break;
        case '"':
          if (inSingleQuote) {
            opt.appendCodePoint(c);
          } else {
            inDoubleQuote = !inDoubleQuote;
          }
          break;
        default:
          if (!Character.isWhitespace(c) || inSingleQuote || inDoubleQuote) {
            opt.appendCodePoint(c);
          } else {
            opts.add(opt.toString());
            opt.setLength(0);
            inOpt = false;
            hasData = false;
          }
        }
      } else {
        switch (c) {
        case '\'':
          inSingleQuote = true;
          inOpt = true;
          hasData = true;
          break;
        case '"':
          inDoubleQuote = true;
          inOpt = true;
          hasData = true;
          break;
        case '\\':
          escapeNext = true;
          inOpt = true;
          hasData = true;
          break;
        default:
          if (!Character.isWhitespace(c)) {
            inOpt = true;
            hasData = true;
            opt.appendCodePoint(c);
          }
        }
      }
    }

    checkArgument(!inSingleQuote && !inDoubleQuote && !escapeNext, "Invalid option string: %s", s);
    if (hasData) {
      opts.add(opt.toString());
    }
    return opts;
  }

  /** Throws IllegalArgumentException if the given object is null. 
   * 如果o==null,则抛异常 
   **/
  static void checkNotNull(Object o, String arg) {
    if (o == null) {
      throw new IllegalArgumentException(String.format("'%s' must not be null.", arg));
    }
  }

  /** Throws IllegalArgumentException with the given message if the check is false. 
   * 如果check=false,则抛异常,异常信息就是message以及参数集合args 
   **/
  static void checkArgument(boolean check, String msg, Object... args) {
    if (!check) {
      throw new IllegalArgumentException(String.format(msg, args));
    }
  }

  /** Throws IllegalStateException with the given message if the check is false.
   * 如果check=false,则抛异常,异常信息就是message以及参数集合args
   **/
  static void checkState(boolean check, String msg, Object... args) {
    if (!check) {
      throw new IllegalStateException(String.format(msg, args));
    }
  }

  /**
   * Quote a command argument for a command to be run by a Windows batch script, if the argument
   * needs quoting. Arguments only seem to need quotes in batch scripts if they have certain
   * special characters, some of which need extra (and different) escaping.
   *
   *  For example:
   *    original single argument: ab="cde fgh"
   *    quoted: "ab^=""cde fgh"""
   *    
   *如果参数有特殊字符的话,需要将参数添加双引号,
   例子:
   zzz=\"sds,sd s,\\ 改成"zzz=""sds,sd s,\\"
	zzz=sds,sd s, 改成"zzz=sds,sd s,"
   */
  static String quoteForBatchScript(String arg) {

    boolean needsQuotes = false;
    for (int i = 0; i < arg.length(); i++) {
      int c = arg.codePointAt(i);
      if (Character.isWhitespace(c) || c == '"' || c == '=' || c == ',' || c == ';') {
        needsQuotes = true;
        break;
      }
    }
    if (!needsQuotes) {
      return arg;
    }
    StringBuilder quoted = new StringBuilder();
    quoted.append("\"");
    for (int i = 0; i < arg.length(); i++) {
      int cp = arg.codePointAt(i);
      switch (cp) {
      case '"':
        quoted.append('"'); //如果遇见”,则多添加一个"
        break;

      default:
        break;
      }
      quoted.appendCodePoint(cp);
    }
    if (arg.codePointAt(arg.length() - 1) == '\\') {
      quoted.append("\\");
    }
    quoted.append("\"");
    return quoted.toString();
  }

  /**
   * Quotes a string so that it can be used in a command string.
   * Basically, just add simple escapes. E.g.:
   *    original single argument : ab "cd" ef
   *    after: "ab \"cd\" ef"
   *
   * This can be parsed back into a single argument by python's "shlex.split()" function.
   * 参数字符中有"或者\的时候,需要追加\\转义字符
   */
  static String quoteForCommandString(String s) {
    StringBuilder quoted = new StringBuilder().append('"');
    for (int i = 0; i < s.length(); i++) {
      int cp = s.codePointAt(i);
      if (cp == '"' || cp == '\\') {
        quoted.appendCodePoint('\\');
      }
      quoted.appendCodePoint(cp);
    }
    return quoted.append('"').toString();
  }

}
