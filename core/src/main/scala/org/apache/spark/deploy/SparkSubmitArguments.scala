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

package org.apache.spark.deploy

import java.io.{ByteArrayOutputStream, PrintStream}
import java.lang.reflect.InvocationTargetException
import java.net.URI
import java.util.{List => JList}
import java.util.jar.JarFile

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.io.Source

import org.apache.spark.deploy.SparkSubmitAction._
import org.apache.spark.launcher.SparkSubmitArgumentsParser
import org.apache.spark.util.Utils

/**
 * Parses and encapsulates arguments from the spark-submit script.
 * The env argument is used for testing.
 * 从spark-submit脚本中解析压缩的参数
 * 
 * @args 参数集合
 * @env 环境变量
 */
private[deploy] class SparkSubmitArguments(args: Seq[String], env: Map[String, String] = sys.env)
  extends SparkSubmitArgumentsParser {
  var master: String = null
  var deployMode: String = null//cluster或者client两者之一
  var executorMemory: String = null
  var executorCores: String = null
  var totalExecutorCores: String = null
  var propertiesFile: String = null //参数设置的属性配置文件路径
  var driverMemory: String = null
  var driverExtraClassPath: String = null
  var driverExtraLibraryPath: String = null
  var driverExtraJavaOptions: String = null
  var queue: String = null
  var numExecutors: String = null
  var files: String = null
  var archives: String = null
  var mainClass: String = null
  var primaryResource: String = null
  var name: String = null
  var childArgs: ArrayBuffer[String] = new ArrayBuffer[String]() //额外没有解析的参数集合
  var jars: String = null //返回值是path的url形式用逗号分割的字符串,可以是HDFS格式的
  var packages: String = null
  var repositories: String = null //maven资源集合
  var ivyRepoPath: String = null //本地的maven仓库位置
  var packagesExclusions: String = null
  var verbose: Boolean = false //true表示要打印详细信息
  var isPython: Boolean = false
  var pyFiles: String = null
  var isR: Boolean = false
  var action: SparkSubmitAction = null //SUBMIT, KILL, REQUEST_STATUS,命令的行为,三者之一
  //--conf PROP=VALUE   .配置任意的spark配置信息,一次仅能设置一个键值对
  //存储spark的属性信息
  val sparkProperties: HashMap[String, String] = new HashMap[String, String]()
  var proxyUser: String = null
  var principal: String = null
  var keytab: String = null

  // Standalone cluster mode only
  var supervise: Boolean = false
  var driverCores: String = null
  var submissionToKill: String = null
  var submissionToRequestStatusFor: String = null
  var useRest: Boolean = true // used internally

  /** Default properties present in the currently defined defaults file. 
   *  读取配置文件配置的信息
   **/
  lazy val defaultSparkProperties: HashMap[String, String] = {
    val defaultProperties = new HashMap[String, String]()
    // scalastyle:off println
    if (verbose) SparkSubmit.printStream.println(s"Using properties file: $propertiesFile")
    //读取配置文件
    Option(propertiesFile).foreach { filename =>
    Utils.getPropertiesFromFile(filename).foreach { case (k, v) =>
        defaultProperties(k) = v
        if (verbose) SparkSubmit.printStream.println(s"Adding default property: $k=$v")
      }
    }
    // scalastyle:on println
    defaultProperties
  }

  // Set parameters from command line arguments
  try {
    parse(args.toList)
  } catch {
    case e: IllegalArgumentException =>
      SparkSubmit.printErrorAndExit(e.getMessage())
  }
  // Populate `sparkProperties` map from properties file 合并配置信息
  mergeDefaultSparkProperties()
  // Remove keys that don't start with "spark." from `sparkProperties`.过滤非spark.开头的属性
  ignoreNonSparkProperties()
  // Use `sparkProperties` map along with env vars to fill in any missing parameters 设置默认属性信息,即在脚本参数中没有加载到的话,则要加载默认值
  loadEnvironmentArguments()

  validateArguments() //校验参数

  /**
   * Merge values from the default properties file with those specified through --conf.
   * When this is called, `sparkProperties` is already filled with configs from the latter.
   * 读取配置文件,将配置文件中信息添加到全局sparkProperties中,即--conf的优先级更大,配置文件中的属性不能覆盖--conf
   */
  private def mergeDefaultSparkProperties(): Unit = {
    // Use common defaults file, if not specified by user
    propertiesFile = Option(propertiesFile).getOrElse(Utils.getDefaultPropertiesFile(env))
    // Honor --conf before the defaults file
    defaultSparkProperties.foreach { case (k, v) =>
      if (!sparkProperties.contains(k)) {
        sparkProperties(k) = v
      }
    }
  }

  /**
   * Remove keys that don't start with "spark." from `sparkProperties`.
   * 删除不是以spark.开头的属性信息
   */
  private def ignoreNonSparkProperties(): Unit = {
    sparkProperties.foreach { case (k, v) =>
      if (!k.startsWith("spark.")) {
        sparkProperties -= k
        SparkSubmit.printWarning(s"Ignoring non-spark config property: $k=$v")
      }
    }
  }

  /**
   * Load arguments from environment variables, Spark properties etc.
   * 设置默认属性信息,即在脚本参数中没有加载到的话,则要加载默认值
   */
  private def loadEnvironmentArguments(): Unit = {
    master = Option(master)
      .orElse(sparkProperties.get("spark.master"))
      .orElse(env.get("MASTER"))
      .orNull
    driverExtraClassPath = Option(driverExtraClassPath)
      .orElse(sparkProperties.get("spark.driver.extraClassPath"))
      .orNull
    driverExtraJavaOptions = Option(driverExtraJavaOptions)
      .orElse(sparkProperties.get("spark.driver.extraJavaOptions"))
      .orNull
    driverExtraLibraryPath = Option(driverExtraLibraryPath)
      .orElse(sparkProperties.get("spark.driver.extraLibraryPath"))
      .orNull
    driverMemory = Option(driverMemory)
      .orElse(sparkProperties.get("spark.driver.memory"))
      .orElse(env.get("SPARK_DRIVER_MEMORY"))
      .orNull
    driverCores = Option(driverCores)
      .orElse(sparkProperties.get("spark.driver.cores"))
      .orNull
    executorMemory = Option(executorMemory)
      .orElse(sparkProperties.get("spark.executor.memory"))
      .orElse(env.get("SPARK_EXECUTOR_MEMORY"))
      .orNull
    executorCores = Option(executorCores)
      .orElse(sparkProperties.get("spark.executor.cores"))
      .orElse(env.get("SPARK_EXECUTOR_CORES"))
      .orNull
    totalExecutorCores = Option(totalExecutorCores)
      .orElse(sparkProperties.get("spark.cores.max"))
      .orNull
    name = Option(name).orElse(sparkProperties.get("spark.app.name")).orNull
    jars = Option(jars).orElse(sparkProperties.get("spark.jars")).orNull
    ivyRepoPath = sparkProperties.get("spark.jars.ivy").orNull //本地的maven仓库位置
    packages = Option(packages).orElse(sparkProperties.get("spark.jars.packages")).orNull
    packagesExclusions = Option(packagesExclusions)
      .orElse(sparkProperties.get("spark.jars.excludes")).orNull
    deployMode = Option(deployMode).orElse(env.get("DEPLOY_MODE")).orNull
    numExecutors = Option(numExecutors)
      .getOrElse(sparkProperties.get("spark.executor.instances").orNull)
    keytab = Option(keytab).orElse(sparkProperties.get("spark.yarn.keytab")).orNull
    principal = Option(principal).orElse(sparkProperties.get("spark.yarn.principal")).orNull

    // Try to set main class from JAR if no --class argument is given
    if (mainClass == null && !isPython && !isR && primaryResource != null) {
      val uri = new URI(primaryResource)
      val uriScheme = uri.getScheme()

      uriScheme match {
        case "file" =>
          try {
            val jar = new JarFile(uri.getPath)
            // Note that this might still return null if no main-class is set; we catch that later
            mainClass = jar.getManifest.getMainAttributes.getValue("Main-Class")
          } catch {
            case e: Exception =>
              SparkSubmit.printErrorAndExit(s"Cannot load main class from JAR $primaryResource")
          }
        case _ =>
          SparkSubmit.printErrorAndExit(
            s"Cannot load main class from JAR $primaryResource with URI $uriScheme. " +
            "Please specify a class through --class.")
      }
    }

    // Global defaults. These should be keep to minimum to avoid confusing behavior.
    master = Option(master).getOrElse("local[*]")

    // In YARN mode, app name can be set via SPARK_YARN_APP_NAME (see SPARK-5222)
    if (master.startsWith("yarn")) {
      name = Option(name).orElse(env.get("SPARK_YARN_APP_NAME")).orNull
    }

    // Set name from main class if not given
    name = Option(name).orElse(Option(mainClass)).orNull
    if (name == null && primaryResource != null) {
      name = Utils.stripDirectory(primaryResource)
    }

    // Action should be SUBMIT unless otherwise specified
    action = Option(action).getOrElse(SUBMIT)
  }

  /** Ensure that required fields exists. Call this only once all defaults are loaded. */
  private def validateArguments(): Unit = {
    action match {
      case SUBMIT => validateSubmitArguments()
      case KILL => validateKillArguments()
      case REQUEST_STATUS => validateStatusRequestArguments()
    }
  }

  private def validateSubmitArguments(): Unit = {
    if (args.length == 0) {
      printUsageAndExit(-1)
    }
    if (primaryResource == null) {
      SparkSubmit.printErrorAndExit("Must specify a primary resource (JAR or Python or R file)")
    }
    if (mainClass == null && SparkSubmit.isUserJar(primaryResource)) {
      SparkSubmit.printErrorAndExit("No main class set in JAR; please specify one with --class")
    }
    if (pyFiles != null && !isPython) {
      SparkSubmit.printErrorAndExit("--py-files given but primary resource is not a Python script")
    }

    if (master.startsWith("yarn")) {
      val hasHadoopEnv = env.contains("HADOOP_CONF_DIR") || env.contains("YARN_CONF_DIR")
      if (!hasHadoopEnv && !Utils.isTesting) {
        throw new Exception(s"When running with master '$master' " +
          "either HADOOP_CONF_DIR or YARN_CONF_DIR must be set in the environment.")
      }
    }
  }

  private def validateKillArguments(): Unit = {
    if (!master.startsWith("spark://") && !master.startsWith("mesos://")) {
      SparkSubmit.printErrorAndExit(
        "Killing submissions is only supported in standalone or Mesos mode!")
    }
    if (submissionToKill == null) {
      SparkSubmit.printErrorAndExit("Please specify a submission to kill.")
    }
  }

  private def validateStatusRequestArguments(): Unit = {
    if (!master.startsWith("spark://") && !master.startsWith("mesos://")) {
      SparkSubmit.printErrorAndExit(
        "Requesting submission statuses is only supported in standalone or Mesos mode!")
    }
    if (submissionToRequestStatusFor == null) {
      SparkSubmit.printErrorAndExit("Please specify a submission to request status for.")
    }
  }

  def isStandaloneCluster: Boolean = {
    master.startsWith("spark://") && deployMode == "cluster"
  }

  override def toString: String = {
    s"""Parsed arguments:
    |  master                  $master
    |  deployMode              $deployMode
    |  executorMemory          $executorMemory
    |  executorCores           $executorCores
    |  totalExecutorCores      $totalExecutorCores
    |  propertiesFile          $propertiesFile
    |  driverMemory            $driverMemory
    |  driverCores             $driverCores
    |  driverExtraClassPath    $driverExtraClassPath
    |  driverExtraLibraryPath  $driverExtraLibraryPath
    |  driverExtraJavaOptions  $driverExtraJavaOptions
    |  supervise               $supervise
    |  queue                   $queue
    |  numExecutors            $numExecutors
    |  files                   $files
    |  pyFiles                 $pyFiles
    |  archives                $archives
    |  mainClass               $mainClass
    |  primaryResource         $primaryResource
    |  name                    $name
    |  childArgs               [${childArgs.mkString(" ")}]
    |  jars                    $jars
    |  packages                $packages
    |  packagesExclusions      $packagesExclusions
    |  repositories            $repositories
    |  verbose                 $verbose
    |
    |Spark properties used, including those specified through
    | --conf and those from the properties file $propertiesFile:
    |${sparkProperties.mkString("  ", "\n  ", "\n")}
    """.stripMargin
  }

  /** Fill in values by parsing user options.
   * 处理解析出来的key和value参数 
   **/
  override protected def handle(opt: String, value: String): Boolean = {
    opt match {
      case NAME =>
        name = value

      case MASTER =>
        master = value

      case CLASS =>
        mainClass = value

      case DEPLOY_MODE =>
        if (value != "client" && value != "cluster") {
          SparkSubmit.printErrorAndExit("--deploy-mode must be either \"client\" or \"cluster\"")
        }
        deployMode = value

      case NUM_EXECUTORS =>
        numExecutors = value

      case TOTAL_EXECUTOR_CORES =>
        totalExecutorCores = value

      case EXECUTOR_CORES =>
        executorCores = value

      case EXECUTOR_MEMORY =>
        executorMemory = value

      case DRIVER_MEMORY =>
        driverMemory = value

      case DRIVER_CORES =>
        driverCores = value

      case DRIVER_CLASS_PATH =>
        driverExtraClassPath = value

      case DRIVER_JAVA_OPTIONS =>
        driverExtraJavaOptions = value

      case DRIVER_LIBRARY_PATH =>
        driverExtraLibraryPath = value

      case PROPERTIES_FILE =>
        propertiesFile = value

      case KILL_SUBMISSION =>
        submissionToKill = value
        if (action != null) {
          SparkSubmit.printErrorAndExit(s"Action cannot be both $action and $KILL.")
        }
        action = KILL

      case STATUS =>
        submissionToRequestStatusFor = value
        if (action != null) {
          SparkSubmit.printErrorAndExit(s"Action cannot be both $action and $REQUEST_STATUS.")
        }
        action = REQUEST_STATUS

      case SUPERVISE =>
        supervise = true

      case QUEUE =>
        queue = value

      case FILES =>
        files = Utils.resolveURIs(value)

      case PY_FILES =>
        pyFiles = Utils.resolveURIs(value)

      case ARCHIVES =>
        archives = Utils.resolveURIs(value)

      case JARS =>
        jars = Utils.resolveURIs(value)

      case PACKAGES =>
        packages = value

      case PACKAGES_EXCLUDE =>
        packagesExclusions = value

      case REPOSITORIES =>
        repositories = value

      case CONF =>
        value.split("=", 2).toSeq match {//--conf PROP=VALUE   .配置任意的spark配置信息,一次仅能设置一个键值对
          case Seq(k, v) => sparkProperties(k) = v
          case _ => SparkSubmit.printErrorAndExit(s"Spark config without '=': $value")
        }

      case PROXY_USER =>
        proxyUser = value

      case PRINCIPAL =>
        principal = value

      case KEYTAB =>
        keytab = value

      case HELP =>
        printUsageAndExit(0)

      case VERBOSE =>
        verbose = true

      case VERSION => //打印spark版本信息,并且退出程序
        SparkSubmit.printVersionAndExit()

      case USAGE_ERROR =>
        printUsageAndExit(1)//退出程序

      case _ =>
        throw new IllegalArgumentException(s"Unexpected argument '$opt'.")
    }
    true
  }

  /**
   * Handle unrecognized command line options.
   *
   * The first unrecognized option is treated as the "primary resource". Everything else is
   * treated as application arguments.
   * 当发现没有定义的key时候执行该方法,传入key
   */
  override protected def handleUnknown(opt: String): Boolean = {
    if (opt.startsWith("-")) {//以-开头的未定义的key,则退出程序,打印信息
      SparkSubmit.printErrorAndExit(s"Unrecognized option '$opt'.")
    }

    //说明参数不是以-开头的
    primaryResource =
      if (!SparkSubmit.isShell(opt) && !SparkSubmit.isInternal(opt)) {
        Utils.resolveURI(opt).toString
      } else {
        opt
      }
    isPython = SparkSubmit.isPython(opt)
    isR = SparkSubmit.isR(opt)
    false
  }

  //添加额外的未解析的参数集合
  override protected def handleExtraArgs(extra: JList[String]): Unit = {
    childArgs ++= extra
  }

  private def printUsageAndExit(exitCode: Int, unknownParam: Any = null): Unit = {
    // scalastyle:off println
    val outStream = SparkSubmit.printStream
    if (unknownParam != null) {
      outStream.println("Unknown/unsupported param " + unknownParam)
    }
    val command = sys.env.get("_SPARK_CMD_USAGE").getOrElse(
      """Usage: spark-submit [options] <app jar | python file> [app arguments]
        |Usage: spark-submit --kill [submission ID] --master [spark://...]
        |Usage: spark-submit --status [submission ID] --master [spark://...]""".stripMargin)
    outStream.println(command)

    val mem_mb = Utils.DEFAULT_DRIVER_MEM_MB
    outStream.println(
      s"""
        |Options:
        |  --master MASTER_URL         spark://host:port, mesos://host:port, yarn, or local.
        |  --deploy-mode DEPLOY_MODE   Whether to launch the driver program locally ("client") or
        |                              on one of the worker machines inside the cluster ("cluster")
        |                              (Default: client).
        |  --class CLASS_NAME          Your application's main class (for Java / Scala apps).主类
        |  --name NAME                 A name of your application.应用名称
        |  --jars JARS                 Comma-separated list of local jars to include on the driver
        |                              and executor classpaths.逗号拆分的本地jar包
        |  --packages                  Comma-separated list of maven coordinates of jars to include
        |                              on the driver and executor classpaths. Will search the local
        |                              maven repo, then maven central and any additional remote
        |                              repositories given by --repositories. The format for the
        |                              coordinates should be groupId:artifactId:version.逗号拆分的maven的坐标格式groupId:artifactId:version,可以从maven上加载jar包
        |  --exclude-packages          Comma-separated list of groupId:artifactId, to exclude while
        |                              resolving the dependencies provided in --packages to avoid
        |                              dependency conflicts.用逗号拆分的,排除的jar包,避免依赖冲突,格式groupId:artifactId即可
        |  --repositories              Comma-separated list of additional remote repositories to
        |                              search for the maven coordinates given with --packages.逗号拆分的maven加载--packages时候的maven资源,maven会从这些资源上查找对应的jar文件
        |  --py-files PY_FILES         Comma-separated list of .zip, .egg, or .py files to place
        |                              on the PYTHONPATH for Python apps.逗号拆分,包含.zip, .egg, or .py 文件集合,用于python程序执行
        |  --files FILES               Comma-separated list of files to be placed in the working
        |                              directory of each executor.用逗号拆分的文件集合,这些文件被存放在工作目录中,被用于执行任务时候使用
        |
        |  --conf PROP=VALUE           Arbitrary Spark configuration property.配置任意的spark配置信息,一次仅能设置一个键值对
        |  --properties-file FILE      Path to a file from which to load extra properties. If not
        |                              specified, this will look for conf/spark-defaults.conf.配置一个配置文件路径,可以加载额外的属性,该属性不是一定要有的,默认会去寻找conf/spark-defaults.conf配置文件
        |
        |  --driver-memory MEM         Memory for driver (e.g. 1000M, 2G) (Default: ${mem_mb}M).driver需要的内存设置,默认是${mem_mb}M,支持单位M G K等形式 
        |  --driver-java-options       Extra Java options to pass to the driver.传递到driver中额外的JVM属性
        |  --driver-library-path       Extra library path entries to pass to the driver.额外的jar包路径,传递到driver中
        |  --driver-class-path         Extra class path entries to pass to the driver. Note that
        |                              jars added with --jars are automatically included in the
        |                              classpath.额外的class主类传递到driver中,注意该class对应的jar已经被自动添加到--jars属性中了
        |
        |  --executor-memory MEM       Memory per executor (e.g. 1000M, 2G) (Default: 1G).executor任务执行的时候所需要的内存
        |
        |  --proxy-user NAME           User to impersonate when submitting the application.提交一个应用的时候,提交者
        |
        |  --help, -h                  Show this help message and exit
        |  --verbose, -v               Print additional debug output 打印更加详细的信息
        |  --version,                  Print the version of current Spark 打印当前spark的version
        |
        | Spark standalone with cluster deploy mode only:
        |  --driver-cores NUM          Cores for driver (Default: 1).
        |
        | Spark standalone or Mesos with cluster deploy mode only:
        |  --supervise                 If given, restarts the driver on failure.true表示当driver失败了.可以重新开启
        |  --kill SUBMISSION_ID        If given, kills the driver specified.可以去kill指定的driver
        |  --status SUBMISSION_ID      If given, requests the status of the driver specified.可以请求指定driver的状态信息
        |
        | Spark standalone and Mesos only:
        |  --total-executor-cores NUM  Total cores for all executors.设置所有的执行任务所需要的总CPU数量
        |
        | Spark standalone and YARN only:
        |  --executor-cores NUM        Number of cores per executor. (Default: 1 in YARN mode,
        |                              or all available cores on the worker in standalone mode) 每一个任务需要多少个CPU
        |
        | YARN-only:
        |  --driver-cores NUM          Number of cores used by the driver, only in cluster mode
        |                              (Default: 1).driver使用多少CPU
        |  --queue QUEUE_NAME          The YARN queue to submit to (Default: "default"). 该任务提交到yarn上哪个队列
        |  --num-executors NUM         Number of executors to launch (Default: 2).需要启动多少个任务
        |  --archives ARCHIVES         Comma separated list of archives to be extracted into the
        |                              working directory of each executor.
        |  --principal PRINCIPAL       Principal to be used to login to KDC, while running on
        |                              secure HDFS.
        |  --keytab KEYTAB             The full path to the file that contains the keytab for the
        |                              principal specified above. This keytab will be copied to
        |                              the node running the Application Master via the Secure
        |                              Distributed Cache, for renewing the login tickets and the
        |                              delegation tokens periodically.
      """.stripMargin
    )

    if (SparkSubmit.isSqlShell(mainClass)) {
      outStream.println("CLI options:")
      outStream.println(getSqlShellOptions())
    }
    // scalastyle:on println

    SparkSubmit.exitFn(exitCode)
  }

  /**
   * Run the Spark SQL CLI main class with the "--help" option and catch its output. Then filter
   * the results to remove unwanted lines.
   *
   * Since the CLI will call `System.exit()`, we install a security manager to prevent that call
   * from working, and restore the original one afterwards.
   */
  private def getSqlShellOptions(): String = {
    val currentOut = System.out
    val currentErr = System.err
    val currentSm = System.getSecurityManager()
    try {
      val out = new ByteArrayOutputStream()
      val stream = new PrintStream(out)
      System.setOut(stream)
      System.setErr(stream)

      val sm = new SecurityManager() {
        override def checkExit(status: Int): Unit = {
          throw new SecurityException()
        }

        override def checkPermission(perm: java.security.Permission): Unit = {}
      }
      System.setSecurityManager(sm)

      try {
        Utils.classForName(mainClass).getMethod("main", classOf[Array[String]])
          .invoke(null, Array(HELP))
      } catch {
        case e: InvocationTargetException =>
          // Ignore SecurityException, since we throw it above.
          if (!e.getCause().isInstanceOf[SecurityException]) {
            throw e
          }
      }

      stream.flush()

      // Get the output and discard any unnecessary lines from it.
      Source.fromString(new String(out.toByteArray())).getLines
        .filter { line =>
          !line.startsWith("log4j") && !line.startsWith("usage")
        }
        .mkString("\n")
    } finally {
      System.setSecurityManager(currentSm)
      System.setOut(currentOut)
      System.setErr(currentErr)
    }
  }
}
