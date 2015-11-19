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

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parser for spark-submit command line options.
 * <p>
 * This class encapsulates the parsing code for spark-submit command line options, so that there
 * is a single list of options that needs to be maintained (well, sort of, but it makes it harder
 * to break things).
 * 
 * 该类解析指定的参数信息,以及提取除了指定参数列表外的额外参数信息集合
 * 
 * 参见SparkSubmitArguments类
 */
class SparkSubmitOptionParser {

  // The following constants define the "main" name for the available options. They're defined
  // to avoid copy & paste of the raw strings where they're needed.
  //
  // The fields are not static so that they're exposed to Scala code that uses this class. See
  // SparkSubmitArguments.scala. That is also why this class is not abstract - to allow code to
  // easily use these constants without having to create dummy implementations of this class.
  protected final String CLASS = "--class";
  protected final String CONF = "--conf"; //--conf PROP=VALUE   .配置任意的spark配置信息,一次仅能设置一个键值对
  protected final String DEPLOY_MODE = "--deploy-mode";
  protected final String DRIVER_CLASS_PATH = "--driver-class-path";
  protected final String DRIVER_CORES = "--driver-cores";
  protected final String DRIVER_JAVA_OPTIONS =  "--driver-java-options";
  protected final String DRIVER_LIBRARY_PATH = "--driver-library-path";
  protected final String DRIVER_MEMORY = "--driver-memory";
  protected final String EXECUTOR_MEMORY = "--executor-memory";
  protected final String FILES = "--files";
  protected final String JARS = "--jars";
  protected final String KILL_SUBMISSION = "--kill";
  protected final String MASTER = "--master";
  protected final String NAME = "--name";
  protected final String PACKAGES = "--packages";
  protected final String PACKAGES_EXCLUDE = "--exclude-packages";
  protected final String PROPERTIES_FILE = "--properties-file";
  protected final String PROXY_USER = "--proxy-user";
  protected final String PY_FILES = "--py-files";
  protected final String REPOSITORIES = "--repositories";//maven资源集合
  protected final String STATUS = "--status";
  protected final String TOTAL_EXECUTOR_CORES = "--total-executor-cores";

  // Options that do not take arguments.
  protected final String HELP = "--help";
  protected final String SUPERVISE = "--supervise";
  protected final String USAGE_ERROR = "--usage-error";//参数使用错误
  protected final String VERBOSE = "--verbose"; //打印更详细日志
  protected final String VERSION = "--version"; //打印spark版本信息,并且退出程序

  // Standalone-only options.

  // YARN-only options.
  protected final String ARCHIVES = "--archives";
  protected final String EXECUTOR_CORES = "--executor-cores";
  protected final String KEYTAB = "--keytab";
  protected final String NUM_EXECUTORS = "--num-executors";
  protected final String PRINCIPAL = "--principal";
  protected final String QUEUE = "--queue";

  /**
   * This is the canonical list of spark-submit options. Each entry in the array contains the
   * different aliases for the same option; the first element of each entry is the "official"
   * name of the option, passed to {@link #handle(String, String)}.
   * <p>
   * Options not listed here nor in the "switch" list below will result in a call to
   * {@link $#handleUnknown(String)}.
   * <p>
   * These two arrays are visible for tests.
   * 可以包含参数的选项
   */
  final String[][] opts = {
    { ARCHIVES },
    { CLASS },
    { CONF, "-c" },
    { DEPLOY_MODE },
    { DRIVER_CLASS_PATH },
    { DRIVER_CORES },
    { DRIVER_JAVA_OPTIONS },
    { DRIVER_LIBRARY_PATH },
    { DRIVER_MEMORY },
    { EXECUTOR_CORES },
    { EXECUTOR_MEMORY },
    { FILES },
    { JARS },
    { KEYTAB },
    { KILL_SUBMISSION },
    { MASTER },
    { NAME },
    { NUM_EXECUTORS },
    { PACKAGES },
    { PACKAGES_EXCLUDE },
    { PRINCIPAL },
    { PROPERTIES_FILE },
    { PROXY_USER },
    { PY_FILES },
    { QUEUE },
    { REPOSITORIES },
    { STATUS },
    { TOTAL_EXECUTOR_CORES },
  };

  /**
   * List of switches (command line options that do not take parameters) recognized by spark-submit.
   * 不允许包含参数的选项
   */
  final String[][] switches = {
    { HELP, "-h" },
    { SUPERVISE },
    { USAGE_ERROR },
    { VERBOSE, "-v" },
    { VERSION },
  };

  /**
   * Parse a list of spark-submit command line options.
   * <p>
   * See SparkSubmitArguments.scala for a more formal description of available options.
   *
   * @throws IllegalArgumentException If an error is found during parsing.
   * 
   * 每一个args中元素是--class=xxx方式
   * 或者也支持支持--class xxx方式
   */
  protected final void parse(List<String> args) {
    Pattern eqSeparatedOpt = Pattern.compile("(--[^=]+)=(.+)");

    int idx = 0;
    for (idx = 0; idx < args.size(); idx++) {
      String arg = args.get(idx);//每一个args中元素是--class=xxx方式,解析后arg就是key
      String value = null;

      //解析后arg就是key,value就是=号后面的值
      Matcher m = eqSeparatedOpt.matcher(arg);
      if (m.matches()) {
        arg = m.group(1);
        value = m.group(2);
      }

      // Look for options with a value.查看该key是否允许有value
      String name = findCliOption(arg, opts);
      if (name != null) {//说明该key可以有value
        if (value == null) {//如果value没有被解析出来,则寻找最后面的数据,即支持-c xxx格式
          if (idx == args.size() - 1) {
            throw new IllegalArgumentException(
                String.format("Missing argument for option '%s'.", arg));
          }
          idx++;
          value = args.get(idx);
        }
        if (!handle(name, value)) {
          break;
        }
        continue;
      }

      // Look for a switch.说明该key不能有value
      name = findCliOption(arg, switches);
      if (name != null) {
        if (!handle(name, null)) {
          break;
        }
        continue;
      }

      //说明该key不是spark预先设计的key,因此要异常处理
      if (!handleUnknown(arg)) {
        break;
      }
    }

    //一旦遇到没有预先设置的key时,认为后面的所有参数都是额外参数集合
    if (idx < args.size()) {
      idx++;
    }
    handleExtraArgs(args.subList(idx, args.size()));
  }

  /**
   * Callback for when an option with an argument is parsed.
   *
   * @param opt The long name of the cli option (might differ from actual command line).
   * @param value The value. This will be <i>null</i> if the option does not take a value.
   * @return Whether to continue parsing the argument list.
   */
  protected boolean handle(String opt, String value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Callback for when an unrecognized option is parsed.
   *
   * @param opt Unrecognized option from the command line.
   * @return Whether to continue parsing the argument list.
   * 说明该key不是spark预先设计的key,因此要异常处理
   * 参数是不支持的key
   */
  protected boolean handleUnknown(String opt) {
    throw new UnsupportedOperationException();
  }

  /**
   * Callback for remaining command line arguments after either {@link #handle(String, String)} or
   * {@link #handleUnknown(String)} return "false". This will be called at the end of parsing even
   * when there are no remaining arguments.
   *
   * @param extra List of remaining arguments.
   * 额外的参数值
   */
  protected void handleExtraArgs(List<String> extra) {
    throw new UnsupportedOperationException();
  }

  //在available中找到key为name的参数名
  //如果找到了,则返回匹配的第一个key的名称,如果没找到则返回null
  //例如 --conf 和-c表达同一个意思,则如果找到匹配的则返回--conf
  private String findCliOption(String name, String[][] available) {
    for (String[] candidates : available) {
      for (String candidate : candidates) {
        if (candidate.equals(name)) {
          return candidates[0];
        }
      }
    }
    return null;
  }

}
