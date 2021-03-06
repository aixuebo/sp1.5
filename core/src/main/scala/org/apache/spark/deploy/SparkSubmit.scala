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

import java.io.{File, PrintStream}
import java.lang.reflect.{InvocationTargetException, Modifier, UndeclaredThrowableException}
import java.net.URL
import java.security.PrivilegedExceptionAction

import scala.collection.mutable.{ArrayBuffer, HashMap, Map}

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.ivy.Ivy
import org.apache.ivy.core.LogOptions
import org.apache.ivy.core.module.descriptor._
import org.apache.ivy.core.module.id.{ArtifactId, ModuleId, ModuleRevisionId}
import org.apache.ivy.core.report.ResolveReport
import org.apache.ivy.core.resolve.ResolveOptions
import org.apache.ivy.core.retrieve.RetrieveOptions
import org.apache.ivy.core.settings.IvySettings
import org.apache.ivy.plugins.matcher.GlobPatternMatcher
import org.apache.ivy.plugins.repository.file.FileRepository
import org.apache.ivy.plugins.resolver.{FileSystemResolver, ChainResolver, IBiblioResolver}

import org.apache.spark.{SparkUserAppException, SPARK_VERSION}
import org.apache.spark.api.r.RUtils
import org.apache.spark.deploy.rest._
import org.apache.spark.util.{ChildFirstURLClassLoader, MutableURLClassLoader, Utils}


/**
 * Whether to submit, kill, or request the status of an application.
 * 对应于有三种操作,杀死,提交,请求任务的状态
 * The latter two operations are currently supported only for standalone cluster mode.
 * 最后两个状态,仅仅在standalone cluster mode下才被支持
 */
private[deploy] object SparkSubmitAction extends Enumeration {
  type SparkSubmitAction = Value
  val SUBMIT, KILL, REQUEST_STATUS = Value
}

/**
 * Main gateway of launching a Spark application.
 * 启动一个spark应于的主要入口
 * This program handles setting up the classpath with relevant Spark dependencies and provides
 * a layer over the different cluster managers and deploy modes that Spark supports.
 */
object SparkSubmit {

  // Cluster managers
  //master参数以什么开头的 --- spark的四种集群 local  spark本身的队列STANDALONE   yarn   MESOS
  private val YARN = 1 //以yarn开头
  private val STANDALONE = 2 //以spark开头
  private val MESOS = 4 //以mesos开头
  private val LOCAL = 8 //以local开头
  private val ALL_CLUSTER_MGRS = YARN | STANDALONE | MESOS | LOCAL //值是15,所有的模式,即上面的四种之一

  // Deploy modes
  private val CLIENT = 1 //客户端模式,直接启动应用的main类,比如spark_shell脚本,就是客户端模式,不用再集群上执行
  private val CLUSTER = 2
  private val ALL_DEPLOY_MODES = CLIENT | CLUSTER //值是3

  // A special jar name that indicates the class being run is inside of Spark itself, and therefore
  // no user jar is needed.
  //特别的jar包的名字,说明该运行的class是spark自己内部的类,因此不需要指定jar包
  private val SPARK_INTERNAL = "spark-internal"

  // Special primary resource names that represent shells rather than application jars.
  private val SPARK_SHELL = "spark-shell"//spark-shell的程序
  private val PYSPARK_SHELL = "pyspark-shell" //python的shell
  private val SPARKR_SHELL = "sparkr-shell" //R语言的shell
  private val SPARKR_PACKAGE_ARCHIVE = "sparkr.zip"

  private val CLASS_NOT_FOUND_EXIT_STATUS = 101

  // scalastyle:off println
  // Exposed for testing退出程序函数,
  //传入退出状态码,无返回值
  private[spark] var exitFn: Int => Unit = (exitCode: Int) => System.exit(exitCode)
  
  //警告信息输出流
  private[spark] var printStream: PrintStream = System.err
  private[spark] def printWarning(str: String): Unit = printStream.println("Warning: " + str)
  
  //打印错误信息,并且退出程序
  private[spark] def printErrorAndExit(str: String): Unit = {
    printStream.println("Error: " + str)
    printStream.println("Run with --help for usage help or --verbose for debug output")
    exitFn(1)
  }
  
  //打印spark版本信息,并且退出程序
  private[spark] def printVersionAndExit(): Unit = {
    printStream.println("""Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version %s
      /_/
                        """.format(SPARK_VERSION))
    printStream.println("Type --help for more information.")
    exitFn(0)
  }
  // scalastyle:on println

  
  def main(args: Array[String]): Unit = {
    val appArgs = new SparkSubmitArguments(args) //解析命令行参数信息
    if (appArgs.verbose) {//打印详细信息
      // scalastyle:off println
      printStream.println(appArgs)
      // scalastyle:on println
    }
    
    //根据命令的行为进行具体操作
    appArgs.action match {
      case SparkSubmitAction.SUBMIT => submit(appArgs)
      case SparkSubmitAction.KILL => kill(appArgs)
      case SparkSubmitAction.REQUEST_STATUS => requestStatus(appArgs)
    }
  }

  /**
   * Kill an existing submission using the REST protocol. Standalone and Mesos cluster mode only.
   */
  private def kill(args: SparkSubmitArguments): Unit = {
    new RestSubmissionClient(args.master)
      .killSubmission(args.submissionToKill)
  }

  /**
   * Request the status of an existing submission using the REST protocol.
   * Standalone and Mesos cluster mode only.
   */
  private def requestStatus(args: SparkSubmitArguments): Unit = {
    new RestSubmissionClient(args.master)
      .requestSubmissionStatus(args.submissionToRequestStatusFor)
  }

  /**
   * Submit the application using the provided parameters.
   *
   * This runs in two steps. First, we prepare the launch environment by setting up
   * the appropriate classpath, system properties, and application arguments for
   * running the child main class based on the cluster manager and the deploy mode.
   * Second, we use this launch environment to invoke the main method of the child
   * main class.
   */
  private def submit(args: SparkSubmitArguments): Unit = {
    val (childArgs, childClasspath, sysProps, childMainClass) = prepareSubmitEnvironment(args)

    def doRunMain(): Unit = {
      if (args.proxyUser != null) {
        val proxyUser = UserGroupInformation.createProxyUser(args.proxyUser,
          UserGroupInformation.getCurrentUser())
        try {
          proxyUser.doAs(new PrivilegedExceptionAction[Unit]() {
            override def run(): Unit = {
              runMain(childArgs, childClasspath, sysProps, childMainClass, args.verbose)
            }
          })
        } catch {
          case e: Exception =>
            // Hadoop's AuthorizationException suppresses the exception's stack trace, which
            // makes the message printed to the output by the JVM not very helpful. Instead,
            // detect exceptions with empty stack traces here, and treat them differently.
            if (e.getStackTrace().length == 0) {
              // scalastyle:off println
              printStream.println(s"ERROR: ${e.getClass().getName()}: ${e.getMessage()}")
              // scalastyle:on println
              exitFn(1)
            } else {
              throw e
            }
        }
      } else {
        runMain(childArgs, childClasspath, sysProps, childMainClass, args.verbose)
      }
    }

     // In standalone cluster mode, there are two submission gateways:
     //   (1) The traditional Akka gateway using o.a.s.deploy.Client as a wrapper
     //   (2) The new REST-based gateway introduced in Spark 1.3
     // The latter is the default behavior as of Spark 1.3, but Spark submit will fail over
     // to use the legacy gateway if the master endpoint turns out to be not a REST server.
    if (args.isStandaloneCluster && args.useRest) {
      try {
        // scalastyle:off println
        printStream.println("Running Spark using the REST application submission protocol.")
        // scalastyle:on println
        doRunMain()
      } catch {
        // Fail over to use the legacy submission gateway
        case e: SubmitRestConnectionException =>
          printWarning(s"Master endpoint ${args.master} was not a REST server. " +
            "Falling back to legacy submission gateway instead.")
          args.useRest = false
          submit(args)
      }
    // In all other modes, just run the main class as prepared
    } else {
      doRunMain()
    }
  }

  /**
   * Prepare the environment for submitting an application.
   * This returns a 4-tuple:
   *   (1) the arguments for the child process执行主类需要的参数
   *   (2) a list of classpath entries for the child,需要的jar包集合,需要加入到classpath中
   *   (3) a map of system properties, and 需要的系统参数
   *   (4) the main class for the child 需要执行的主类,该类包含main方法
   * Exposed for testing.
   */
  private[deploy] def prepareSubmitEnvironment(args: SparkSubmitArguments)
      : (Seq[String], Seq[String], Map[String, String], String) = {
    // Return values 四个返回值
    val childArgs = new ArrayBuffer[String]()//运行命令属性
    val childClasspath = new ArrayBuffer[String]()//classPath的全路径
    val sysProps = new HashMap[String, String]()//系统属性
    var childMainClass = ""

    // Set the cluster manager
    //spark的四种集群 local  spark本身的队列STANDALONE   yarn   MESOS
    val clusterManager: Int = args.master match {
      case m if m.startsWith("yarn") => YARN
      case m if m.startsWith("spark") => STANDALONE
      case m if m.startsWith("mesos") => MESOS
      case m if m.startsWith("local") => LOCAL
      case _ => printErrorAndExit("Master must start with yarn, spark, mesos, or local"); -1
    }

    // Set the deploy mode; default is client mode
    //cluster或者client两者之一
    var deployMode: Int = args.deployMode match {
      case "client" | null => CLIENT //默认是clinet模式
      case "cluster" => CLUSTER
      case _ => printErrorAndExit("Deploy mode must be either client or cluster"); -1
    }

    // Because "yarn-cluster" and "yarn-client" encapsulate both the master
    // and deploy mode, we have some logic to infer the master and deploy mode
    // from each other if only one is specified, or exit early if they are at odds.
    //yarn的时候,args.master一定是yarn-cluster或者yarn-client
    if (clusterManager == YARN) {
      if (args.master == "yarn-standalone") {//老版本是这个名字,因此改成yarn-cluster
        printWarning("\"yarn-standalone\" is deprecated. Use \"yarn-cluster\" instead.")
        args.master = "yarn-cluster"
      }
      (args.master, args.deployMode) match {
        case ("yarn-cluster", null) =>
          deployMode = CLUSTER
        case ("yarn-cluster", "client") =>
          printErrorAndExit("Client deploy mode is not compatible with master \"yarn-cluster\"") //说明配置有问题,客户端模式与集群模式不匹配
        case ("yarn-client", "cluster") =>
          printErrorAndExit("Cluster deploy mode is not compatible with master \"yarn-client\"") //说明配置有问题,客户端模式与集群模式不匹配
        case (_, mode) =>
          args.master = "yarn-" + Option(mode).getOrElse("client")
      }

      // Make sure YARN is included in our build if we're trying to use it 确保有这个类
      if (!Utils.classIsLoadable("org.apache.spark.deploy.yarn.Client") && !Utils.isTesting) {
        printErrorAndExit(
          "Could not load YARN classes. " +
          "This copy of Spark may not have been compiled with YARN support.")
      }
    }

    // Update args.deployMode if it is null. It will be passed down as a Spark property later.
    //如果参数中模式没有设置,即为null,则这块设置client/cluster
    (args.deployMode, deployMode) match {
      case (null, CLIENT) => args.deployMode = "client"
      case (null, CLUSTER) => args.deployMode = "cluster"
      case _ =>
    }

    //是yarn队列还是mesos队列管理资源
    val isYarnCluster = clusterManager == YARN && deployMode == CLUSTER
    val isMesosCluster = clusterManager == MESOS && deployMode == CLUSTER

    // Resolve maven dependencies if there are any and add classpath to jars. Add them to py-files
    // too for packages that include Python code
    //不需要的jar包,内容就是maven的group集合,用逗号拆分即可
    val exclusions: Seq[String] =
      if (!StringUtils.isBlank(args.packagesExclusions)) {
        args.packagesExclusions.split(",")
      } else {
        Nil
      }
    
    //返回用逗号拆分的最终需要的maven坐标集合,包括的依赖的jar包,并且去下载该jar包
    /**
    参数是要下载的maven坐标集合,maven资源集合,下载后存储在什么地方,要刨除哪些group的包不去下载
    返回:是一个路径集合,集合用逗号拆分,这些集合是已经下载下来的jar包
    **/
    val resolvedMavenCoordinates = SparkSubmitUtils.resolveMavenCoordinates(args.packages,
      Option(args.repositories), Option(args.ivyRepoPath), exclusions = exclusions)
    if (!StringUtils.isBlank(resolvedMavenCoordinates)) {//说明下载到了jar包集合,要进行合并
      args.jars = mergeFileLists(args.jars, resolvedMavenCoordinates)//合并后本地的jar包集合就多了一部分,这一部分就是从maven上下载的集合
      if (args.isPython) {
        args.pyFiles = mergeFileLists(args.pyFiles, resolvedMavenCoordinates)
      }
    }

    // install any R packages that may have been passed through --jars or --packages.
    // Spark Packages may contain R source code inside the jar.
    if (args.isR && !StringUtils.isBlank(args.jars)) {
      RPackageUtils.checkAndBuildRPackage(args.jars, printStream, args.verbose)
    }

    // Require all python files to be local, so we can add them to the PYTHONPATH
    // In YARN cluster mode, python files are distributed as regular files, which can be non-local
    if (args.isPython && !isYarnCluster) {
      if (Utils.nonLocalPaths(args.primaryResource).nonEmpty) {
        printErrorAndExit(s"Only local python files are supported: $args.primaryResource")
      }
      val nonLocalPyFiles = Utils.nonLocalPaths(args.pyFiles).mkString(",")
      if (nonLocalPyFiles.nonEmpty) {
        printErrorAndExit(s"Only local additional python files are supported: $nonLocalPyFiles")
      }
    }

    // Require all R files to be local
    if (args.isR && !isYarnCluster) {
      if (Utils.nonLocalPaths(args.primaryResource).nonEmpty) {
        printErrorAndExit(s"Only local R files are supported: $args.primaryResource")
      }
    }

    // The following modes are not supported or applicable
    (clusterManager, deployMode) match {
      case (MESOS, CLUSTER) if args.isPython =>
        printErrorAndExit("Cluster deploy mode is currently not supported for python " +
          "applications on Mesos clusters.")
      case (STANDALONE, CLUSTER) if args.isPython =>
        printErrorAndExit("Cluster deploy mode is currently not supported for python " +
          "applications on standalone clusters.")
      case (STANDALONE, CLUSTER) if args.isR =>
        printErrorAndExit("Cluster deploy mode is currently not supported for R " +
          "applications on standalone clusters.")
      case (LOCAL, CLUSTER) =>
        printErrorAndExit("Cluster deploy mode is not compatible with master \"local\"")
      case (_, CLUSTER) if isShell(args.primaryResource) =>
        printErrorAndExit("Cluster deploy mode is not applicable to Spark shells.")//资源是否是shell启动的spark,shell启动的spark只能是local模式,不能是集群模式
      case (_, CLUSTER) if isSqlShell(args.mainClass) =>
        printErrorAndExit("Cluster deploy mode is not applicable to Spark SQL shell.")//class是否是org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver,即spark sql这个类也是不支持集群模式的
      case (_, CLUSTER) if isThriftServer(args.mainClass) =>
        printErrorAndExit("Cluster deploy mode is not applicable to Spark Thrift server.")//class是org.apache.spark.sql.hive.thriftserver.HiveThriftServer2的时候,也是不支持集群模式的
      case _ =>
    }

    // If we're running a python app, set the main class to our specific python runner
    if (args.isPython && deployMode == CLIENT) {
      if (args.primaryResource == PYSPARK_SHELL) {
        args.mainClass = "org.apache.spark.api.python.PythonGatewayServer"
      } else {
        // If a python file is provided, add it to the child arguments and list of files to deploy.
        // Usage: PythonAppRunner <main python file> <extra python files> [app arguments]
        args.mainClass = "org.apache.spark.deploy.PythonRunner"
        args.childArgs = ArrayBuffer(args.primaryResource, args.pyFiles) ++ args.childArgs
        if (clusterManager != YARN) {
          // The YARN backend distributes the primary file differently, so don't merge it.
          args.files = mergeFileLists(args.files, args.primaryResource)
        }
      }
      if (clusterManager != YARN) {
        // The YARN backend handles python files differently, so don't merge the lists.
        args.files = mergeFileLists(args.files, args.pyFiles)
      }
      if (args.pyFiles != null) {
        sysProps("spark.submit.pyFiles") = args.pyFiles
      }
    }

    // In YARN mode for an R app, add the SparkR package archive to archives
    // that can be distributed with the job
    if (args.isR && clusterManager == YARN) {
      val rPackagePath = RUtils.localSparkRPackagePath
      if (rPackagePath.isEmpty) {
        printErrorAndExit("SPARK_HOME does not exist for R application in YARN mode.")
      }
      val rPackageFile =
        RPackageUtils.zipRLibraries(new File(rPackagePath.get), SPARKR_PACKAGE_ARCHIVE)
      if (!rPackageFile.exists()) {
        printErrorAndExit(s"$SPARKR_PACKAGE_ARCHIVE does not exist for R application in YARN mode.")
      }
      val localURI = Utils.resolveURI(rPackageFile.getAbsolutePath)

      // Assigns a symbol link name "sparkr" to the shipped package.
      args.archives = mergeFileLists(args.archives, localURI.toString + "#sparkr")
    }

    // If we're running a R app, set the main class to our specific R runner
    if (args.isR && deployMode == CLIENT) {
      if (args.primaryResource == SPARKR_SHELL) {
        args.mainClass = "org.apache.spark.api.r.RBackend"
      } else {
        // If a R file is provided, add it to the child arguments and list of files to deploy.
        // Usage: RRunner <main R file> [app arguments]
        args.mainClass = "org.apache.spark.deploy.RRunner"
        args.childArgs = ArrayBuffer(args.primaryResource) ++ args.childArgs
        args.files = mergeFileLists(args.files, args.primaryResource)
      }
    }

    if (isYarnCluster && args.isR) {
      // In yarn-cluster mode for a R app, add primary resource to files
      // that can be distributed with the job
      args.files = mergeFileLists(args.files, args.primaryResource)
    }

    // Special flag to avoid deprecation warnings at the client
    sysProps("SPARK_SUBMIT") = "true"

    // A list of rules to map each argument to system properties or command-line options in
    // each deploy mode; we iterate through these below
    //一个规则集合,每一个规则属性值对应从命令行或者系统属性中获取对应的值,并且要校验master和mode是否符合标准
    val options = List[OptionAssigner](

      // All cluster managers
      OptionAssigner(args.master, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES, sysProp = "spark.master"),
      OptionAssigner(args.deployMode, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES,
        sysProp = "spark.submit.deployMode"),
      OptionAssigner(args.name, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES, sysProp = "spark.app.name"),
      OptionAssigner(args.jars, ALL_CLUSTER_MGRS, CLIENT, sysProp = "spark.jars"),
      OptionAssigner(args.ivyRepoPath, ALL_CLUSTER_MGRS, CLIENT, sysProp = "spark.jars.ivy"),
      OptionAssigner(args.driverMemory, ALL_CLUSTER_MGRS, CLIENT,
        sysProp = "spark.driver.memory"),
      OptionAssigner(args.driverExtraClassPath, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES,
        sysProp = "spark.driver.extraClassPath"),
      OptionAssigner(args.driverExtraJavaOptions, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES,
        sysProp = "spark.driver.extraJavaOptions"),
      OptionAssigner(args.driverExtraLibraryPath, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES,
        sysProp = "spark.driver.extraLibraryPath"),

      // Yarn client only
      OptionAssigner(args.queue, YARN, CLIENT, sysProp = "spark.yarn.queue"),
      OptionAssigner(args.numExecutors, YARN, ALL_DEPLOY_MODES,
        sysProp = "spark.executor.instances"),
      OptionAssigner(args.files, YARN, CLIENT, sysProp = "spark.yarn.dist.files"),
      OptionAssigner(args.archives, YARN, CLIENT, sysProp = "spark.yarn.dist.archives"),
      OptionAssigner(args.principal, YARN, CLIENT, sysProp = "spark.yarn.principal"),
      OptionAssigner(args.keytab, YARN, CLIENT, sysProp = "spark.yarn.keytab"),

      // Yarn cluster only
      OptionAssigner(args.name, YARN, CLUSTER, clOption = "--name"),
      OptionAssigner(args.driverMemory, YARN, CLUSTER, clOption = "--driver-memory"),
      OptionAssigner(args.driverCores, YARN, CLUSTER, clOption = "--driver-cores"),
      OptionAssigner(args.queue, YARN, CLUSTER, clOption = "--queue"),
      OptionAssigner(args.executorMemory, YARN, CLUSTER, clOption = "--executor-memory"),
      OptionAssigner(args.executorCores, YARN, CLUSTER, clOption = "--executor-cores"),
      OptionAssigner(args.files, YARN, CLUSTER, clOption = "--files"),
      OptionAssigner(args.archives, YARN, CLUSTER, clOption = "--archives"),
      OptionAssigner(args.jars, YARN, CLUSTER, clOption = "--addJars"),
      OptionAssigner(args.principal, YARN, CLUSTER, clOption = "--principal"),
      OptionAssigner(args.keytab, YARN, CLUSTER, clOption = "--keytab"),

      // Other options
      OptionAssigner(args.executorCores, STANDALONE | YARN, ALL_DEPLOY_MODES,
        sysProp = "spark.executor.cores"),
      OptionAssigner(args.executorMemory, STANDALONE | MESOS | YARN, ALL_DEPLOY_MODES,
        sysProp = "spark.executor.memory"),
      OptionAssigner(args.totalExecutorCores, STANDALONE | MESOS, ALL_DEPLOY_MODES,
        sysProp = "spark.cores.max"),
      OptionAssigner(args.files, LOCAL | STANDALONE | MESOS, ALL_DEPLOY_MODES,
        sysProp = "spark.files"),
      OptionAssigner(args.jars, STANDALONE | MESOS, CLUSTER, sysProp = "spark.jars"),
      OptionAssigner(args.driverMemory, STANDALONE | MESOS, CLUSTER,
        sysProp = "spark.driver.memory"),
      OptionAssigner(args.driverCores, STANDALONE | MESOS, CLUSTER,
        sysProp = "spark.driver.cores"),
      OptionAssigner(args.supervise.toString, STANDALONE | MESOS, CLUSTER,
        sysProp = "spark.driver.supervise"),
      OptionAssigner(args.ivyRepoPath, STANDALONE, CLUSTER, sysProp = "spark.jars.ivy")
    )

    // In client mode, launch the application main class directly 客户端模式,直接启动应用的main类
    // In addition, add the main application jar and any added jars (if any) to the classpath 如果主类是用户自定义的,因此要有jar包要添加到classpath下
    if (deployMode == CLIENT) {
      childMainClass = args.mainClass
      if (isUserJar(args.primaryResource)) {//true表示是用户自定义资源
        childClasspath += args.primaryResource //追加该jar包到classpath下
      }
      if (args.jars != null) { childClasspath ++= args.jars.split(",") }//追加本地的classPath
      if (args.childArgs != null) { childArgs ++= args.childArgs }
    }

    // Map all arguments to command-line options or system properties for our chosen mode
    for (opt <- options) {
      if (opt.value != null &&
          (deployMode & opt.deployMode) != 0 &&
          (clusterManager & opt.clusterManager) != 0) {//校验每一个属性是否符合master和mode的配置
        if (opt.clOption != null) { childArgs += (opt.clOption, opt.value) }
        if (opt.sysProp != null) { sysProps.put(opt.sysProp, opt.value) }
      }
    }

    // Add the application jar automatically so the user doesn't have to call sc.addJar
    // For YARN cluster mode, the jar is already distributed on each node as "app.jar"
    // For python and R files, the primary resource is already distributed as a regular file
    if (!isYarnCluster && !args.isPython && !args.isR) {
      var jars = sysProps.get("spark.jars").map(x => x.split(",").toSeq).getOrElse(Seq.empty)
      if (isUserJar(args.primaryResource)) {
        jars = jars ++ Seq(args.primaryResource)
      }
      sysProps.put("spark.jars", jars.mkString(","))
    }

    // In standalone cluster mode, use the REST client to submit the application (Spark 1.3+).
    // All Spark parameters are expected to be passed to the client through system properties.
    if (args.isStandaloneCluster) {
      if (args.useRest) {
        childMainClass = "org.apache.spark.deploy.rest.RestSubmissionClient"
        childArgs += (args.primaryResource, args.mainClass)
      } else {
        // In legacy standalone cluster mode, use Client as a wrapper around the user class
        childMainClass = "org.apache.spark.deploy.Client"
        if (args.supervise) { childArgs += "--supervise" }
        Option(args.driverMemory).foreach { m => childArgs += ("--memory", m) }
        Option(args.driverCores).foreach { c => childArgs += ("--cores", c) }
        childArgs += "launch"
        childArgs += (args.master, args.primaryResource, args.mainClass)
      }
      if (args.childArgs != null) {
        childArgs ++= args.childArgs
      }
    }

    // Let YARN know it's a pyspark app, so it distributes needed libraries.
    if (clusterManager == YARN) {
      if (args.isPython) {
        sysProps.put("spark.yarn.isPython", "true")
      }
      if (args.principal != null) {
        require(args.keytab != null, "Keytab must be specified when the keytab is specified")
        UserGroupInformation.loginUserFromKeytab(args.principal, args.keytab)
      }
    }

    // In yarn-cluster mode, use yarn.Client as a wrapper around the user class
    if (isYarnCluster) {
      childMainClass = "org.apache.spark.deploy.yarn.Client"
      if (args.isPython) {
        childArgs += ("--primary-py-file", args.primaryResource)
        if (args.pyFiles != null) {
          childArgs += ("--py-files", args.pyFiles)
        }
        childArgs += ("--class", "org.apache.spark.deploy.PythonRunner")
      } else if (args.isR) {
        val mainFile = new Path(args.primaryResource).getName
        childArgs += ("--primary-r-file", mainFile)
        childArgs += ("--class", "org.apache.spark.deploy.RRunner")
      } else {
        if (args.primaryResource != SPARK_INTERNAL) {
          childArgs += ("--jar", args.primaryResource)
        }
        childArgs += ("--class", args.mainClass)
      }
      if (args.childArgs != null) {
        args.childArgs.foreach { arg => childArgs += ("--arg", arg) }
      }
    }

    if (isMesosCluster) {
      assert(args.useRest, "Mesos cluster mode is only supported through the REST submission API")
      childMainClass = "org.apache.spark.deploy.rest.RestSubmissionClient"
      childArgs += (args.primaryResource, args.mainClass)
      if (args.childArgs != null) {
        childArgs ++= args.childArgs
      }
    }

    // Load any properties specified through --conf and the default properties file
    for ((k, v) <- args.sparkProperties) {
      sysProps.getOrElseUpdate(k, v)
    }

    // Ignore invalid spark.driver.host in cluster modes.
    if (deployMode == CLUSTER) {
      sysProps -= "spark.driver.host"
    }

    // Resolve paths in certain spark properties
    val pathConfigs = Seq(
      "spark.jars",
      "spark.files",
      "spark.yarn.jar",
      "spark.yarn.dist.files",
      "spark.yarn.dist.archives")
    pathConfigs.foreach { config =>
      // Replace old URIs with resolved URIs, if they exist 老值替换成新值
      sysProps.get(config).foreach { oldValue =>
        sysProps(config) = Utils.resolveURIs(oldValue)
      }
    }

    // Resolve and format python file paths properly before adding them to the PYTHONPATH.
    // The resolving part is redundant in the case of --py-files, but necessary if the user
    // explicitly sets `spark.submit.pyFiles` in his/her default properties file.
    sysProps.get("spark.submit.pyFiles").foreach { pyFiles =>
      val resolvedPyFiles = Utils.resolveURIs(pyFiles)
      val formattedPyFiles = PythonRunner.formatPaths(resolvedPyFiles).mkString(",")
      sysProps("spark.submit.pyFiles") = formattedPyFiles
    }

    (childArgs, childClasspath, sysProps, childMainClass)
  }

  /**
   * Run the main method of the child class using the provided launch environment.
   *
   * Note that this main class will not be the one provided by the user if we're
   * running cluster deploy mode or python applications.
   * 运行主函数
   *
   * 将childClasspath让classloader加载
   * 将sysProps的信息添加到System.setProperty中
   * 执行childMainClass类的main方法,childArgs就是main方法的args内容
   */
  private def runMain(
      childArgs: Seq[String],//主类需要的参数集合
      childClasspath: Seq[String],//需要的jar文件集合
      sysProps: Map[String, String],//需要的环境变量
      childMainClass: String,//执行的主类
      verbose: Boolean): Unit = {
    // scalastyle:off println
    if (verbose) {//详细打印日志
      printStream.println(s"Main class:\n$childMainClass")
      printStream.println(s"Arguments:\n${childArgs.mkString("\n")}")
      printStream.println(s"System properties:\n${sysProps.mkString("\n")}")
      printStream.println(s"Classpath elements:\n${childClasspath.mkString("\n")}")
      printStream.println("\n")
    }
    // scalastyle:on println

    //executor在加载类的时候是否优先使用用户自定义的JAR包，而不是Spark带有的JAR包。此功能可以用于解决Spark依赖包和用户依赖包之间的冲突。目前，该属性只是一项试验功能。
    val loader =
      if (sysProps.getOrElse("spark.driver.userClassPathFirst", "false").toBoolean) {
        new ChildFirstURLClassLoader(new Array[URL](0),
          Thread.currentThread.getContextClassLoader)
      } else {
        new MutableURLClassLoader(new Array[URL](0),
          Thread.currentThread.getContextClassLoader)
      }
    Thread.currentThread.setContextClassLoader(loader)

    //加载本地的jar文件到classpath中
    for (jar <- childClasspath) {
      addJarToClasspath(jar, loader)
    }

    //将属性添加到系统环境中
    for ((key, value) <- sysProps) {
      System.setProperty(key, value)
    }

    var mainClass: Class[_] = null

    try {
      mainClass = Utils.classForName(childMainClass)
    } catch {
      case e: ClassNotFoundException =>
        e.printStackTrace(printStream)
        if (childMainClass.contains("thriftserver")) {
          // scalastyle:off println
          printStream.println(s"Failed to load main class $childMainClass.")
          printStream.println("You need to build Spark with -Phive and -Phive-thriftserver.")
          // scalastyle:on println
        }
        System.exit(CLASS_NOT_FOUND_EXIT_STATUS)
    }

    // SPARK-4170
    if (classOf[scala.App].isAssignableFrom(mainClass)) {
      printWarning("Subclasses of scala.App may not work correctly. Use a main() method instead.")
    }
    
    //调用class的main方法,参数是字符串Array类型的class,并且main方法不允许是静态的方法
    val mainMethod = mainClass.getMethod("main", new Array[String](0).getClass)
    if (!Modifier.isStatic(mainMethod.getModifiers)) {
      throw new IllegalStateException("The main method in the given main class must be static")
    }

    def findCause(t: Throwable): Throwable = t match {
      case e: UndeclaredThrowableException =>
        if (e.getCause() != null) findCause(e.getCause()) else e
      case e: InvocationTargetException =>
        if (e.getCause() != null) findCause(e.getCause()) else e
      case e: Throwable =>
        e
    }

    //执行main方法,传递参数集合
    try {
      mainMethod.invoke(null, childArgs.toArray)
    } catch {
      case t: Throwable =>
        findCause(t) match {
          case SparkUserAppException(exitCode) =>
            System.exit(exitCode)

          case t: Throwable =>
            throw t
        }
    }
  }

  //加载本地的jar文件到classpath中
  private def addJarToClasspath(localJar: String, loader: MutableURLClassLoader) {
    val uri = Utils.resolveURI(localJar)
    uri.getScheme match {
      case "file" | "local" =>
        val file = new File(uri.getPath)
        if (file.exists()) {
          loader.addURL(file.toURI.toURL)
        } else {
          printWarning(s"Local jar $file does not exist, skipping.")
        }
      case _ =>
        printWarning(s"Skip remote jar $uri.")
    }
  }

  /**
   * Return whether the given primary resource represents a user jar.
   * true表示用户自定义资源
   */
  private[deploy] def isUserJar(res: String): Boolean = {
    !isShell(res) && !isPython(res) && !isInternal(res) && !isR(res)
  }

  /**
   * Return whether the given primary resource represents a shell.
   * 资源是否是shell启动的spark
   */
  private[deploy] def isShell(res: String): Boolean = {
    (res == SPARK_SHELL || res == PYSPARK_SHELL || res == SPARKR_SHELL)
  }

  /**
   * Return whether the given main class represents a sql shell.
   */
  private[deploy] def isSqlShell(mainClass: String): Boolean = {
    mainClass == "org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver"
  }

  /**
   * Return whether the given main class represents a thrift server.
   * 主类是否是thrift服务
   */
  private def isThriftServer(mainClass: String): Boolean = {
    mainClass == "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2"
  }

  /**
   * Return whether the given primary resource requires running python.
   * 传入的资源是否是python资源
   */
  private[deploy] def isPython(res: String): Boolean = {
    res != null && res.endsWith(".py") || res == PYSPARK_SHELL
  }

  /**
   * Return whether the given primary resource requires running R.
   * 传入的资源是否是R资源
   */
  private[deploy] def isR(res: String): Boolean = {
    res != null && res.endsWith(".R") || res == SPARKR_SHELL
  }

  //传入的资源是否是spark-internal,即spark内部资源
  private[deploy] def isInternal(res: String): Boolean = {
    res == SPARK_INTERNAL
  }

  /**
   * Merge a sequence of comma-separated file lists, some of which may be null to indicate
   * no files, into a single comma-separated string.
   * 参数是一个集合,集合的每一个元素都可以用逗号拆分的字符串
   * 目的是合并两个集合,例如第一个集合是本地拥有的jar包集合,第二个参数是从maven上下载的jar包集合路径
   * 作用是将两个路径集合,合并成一个大的路径集合字符串
   * 返回:
   * 1.null表示没有任何文件,要进行过滤掉
   * 2.非null,表示每一个元素按照逗号拆分后,然后合并,然后最终形成一个总的按照逗号拆分的字符串
   */
  private def mergeFileLists(lists: String*): String = {
    val merged = lists.filterNot(StringUtils.isBlank)
                      .flatMap(_.split(","))
                      .mkString(",")
    if (merged == "") null else merged
  }
}

/** Provides utility functions to be used inside SparkSubmit. */
private[spark] object SparkSubmitUtils {

  // Exposed for testing
  var printStream = SparkSubmit.printStream

  /**
   * Represents a Maven Coordinate 定义一个maven坐标
   * @param groupId the groupId of the coordinate
   * @param artifactId the artifactId of the coordinate
   * @param version the version of the coordinate
   */
  private[deploy] case class MavenCoordinate(groupId: String, artifactId: String, version: String) {
    override def toString: String = s"$groupId:$artifactId:$version"
  }

/**
 * Extracts maven coordinates from a comma-delimited string. Coordinates should be provided
 * in the format `groupId:artifactId:version` or `groupId/artifactId:version`.
 * @param coordinates Comma-delimited string of maven coordinates
 * @return Sequence of Maven coordinates
 * 从参数中提取maven坐标集合Seq[MavenCoordinate]
 * 1.按照逗号拆分
 * 2.将坐标中/转换成:
 * 3.按照:拆分成3部分
 *
 * 参数字符串按照,拆分,每一组是一个maven坐标,分别用:或者/进行拆分
 * 例如redis.clients:jedis:2.5.1,mysql/mysql-connector-java/5.1.29
 */
  def extractMavenCoordinates(coordinates: String): Seq[MavenCoordinate] = {
    coordinates.split(",").map { p =>
      val splits = p.replace("/", ":").split(":")
      require(splits.length == 3, s"Provided Maven Coordinates must be in the form " +
        s"'groupId:artifactId:version'. The coordinate provided is: $p")//必须是3部分
      require(splits(0) != null && splits(0).trim.nonEmpty, s"The groupId cannot be null or " +
        s"be whitespace. The groupId provided is: ${splits(0)}")
      require(splits(1) != null && splits(1).trim.nonEmpty, s"The artifactId cannot be null or " +
        s"be whitespace. The artifactId provided is: ${splits(1)}")
      require(splits(2) != null && splits(2).trim.nonEmpty, s"The version cannot be null or " +
        s"be whitespace. The version provided is: ${splits(2)}")
      new MavenCoordinate(splits(0), splits(1), splits(2))
    }
  }

  /** Path of the local Maven cache. 获取本地maven位置*/
  private[spark] def m2Path: File = {
    if (Utils.isTesting) {
      // test builds delete the maven cache, and this can cause flakiness
      new File("dummy", ".m2" + File.separator + "repository")
    } else {
      new File(System.getProperty("user.home"), ".m2" + File.separator + "repository")
    }
  }

  /**
   * Extracts maven coordinates from a comma-delimited string
   * @param remoteRepos Comma-delimited string of remote repositories,maven资源库集合,用逗号拆分
   * @param ivySettings The Ivy settings for this session
   * @return A ChainResolver used by Ivy to search for and resolve dependencies.
   * 连接资源库资源
   */
  def createRepoResolvers(remoteRepos: Option[String], ivySettings: IvySettings): ChainResolver = {
    // We need a chain resolver if we want to check multiple repositories
    val cr = new ChainResolver
    cr.setName("list")

    val repositoryList = remoteRepos.getOrElse("")
    // add any other remote repositories other than maven central
    if (repositoryList.trim.nonEmpty) {
      repositoryList.split(",").zipWithIndex.foreach { case (repo, i) =>
        val brr: IBiblioResolver = new IBiblioResolver
        brr.setM2compatible(true)
        brr.setUsepoms(true)
        brr.setRoot(repo)
        brr.setName(s"repo-${i + 1}")
        cr.add(brr)
        // scalastyle:off println
        printStream.println(s"$repo added as a remote repository with the name: ${brr.getName}")
        // scalastyle:on println
      }
    }

    val localM2 = new IBiblioResolver
    localM2.setM2compatible(true)
    localM2.setRoot(m2Path.toURI.toString)
    localM2.setUsepoms(true)
    localM2.setName("local-m2-cache")
    cr.add(localM2)

    val localIvy = new FileSystemResolver
    val localIvyRoot = new File(ivySettings.getDefaultIvyUserDir, "local")
    localIvy.setLocal(true)
    localIvy.setRepository(new FileRepository(localIvyRoot))
    val ivyPattern = Seq("[organisation]", "[module]", "[revision]", "[type]s",
      "[artifact](-[classifier]).[ext]").mkString(File.separator)
    localIvy.addIvyPattern(localIvyRoot.getAbsolutePath + File.separator + ivyPattern)
    localIvy.setName("local-ivy-cache")
    cr.add(localIvy)

    // the biblio resolver resolves POM declared dependencies
    val br: IBiblioResolver = new IBiblioResolver
    br.setM2compatible(true)
    br.setUsepoms(true)
    br.setName("central")
    cr.add(br)

    val sp: IBiblioResolver = new IBiblioResolver
    sp.setM2compatible(true)
    sp.setUsepoms(true)
    sp.setRoot("http://dl.bintray.com/spark-packages/maven")
    sp.setName("spark-packages")
    cr.add(sp)
    cr
  }

  /**
   * Output a comma-delimited list of paths for the downloaded jars to be added to the classpath
   * (will append to jars in SparkSubmit).
   * @param artifacts Sequence of dependencies that were resolved and retrieved
   * @param cacheDirectory directory where jars are cached
   * @return a comma-delimited list of paths for the dependencies
   * 输出是一个路径集合,集合用逗号拆分,这些集合是已经下载下来的jar包
   */
  def resolveDependencyPaths(
      artifacts: Array[AnyRef],
      cacheDirectory: File): String = {
    artifacts.map { artifactInfo =>
      val artifact = artifactInfo.asInstanceOf[Artifact].getModuleRevisionId
      cacheDirectory.getAbsolutePath + File.separator +
        s"${artifact.getOrganisation}_${artifact.getName}-${artifact.getRevision}.jar"
    }.mkString(",")
  }

  /** Adds the given maven coordinates to Ivy's module descriptor. */
  def addDependenciesToIvy(
      md: DefaultModuleDescriptor,
      artifacts: Seq[MavenCoordinate],
      ivyConfName: String): Unit = {
    artifacts.foreach { mvn =>
      val ri = ModuleRevisionId.newInstance(mvn.groupId, mvn.artifactId, mvn.version)
      val dd = new DefaultDependencyDescriptor(ri, false, false)
      dd.addDependencyConfiguration(ivyConfName, ivyConfName)
      // scalastyle:off println
      printStream.println(s"${dd.getDependencyId} added as a dependency")
      // scalastyle:on println
      md.addDependency(dd)
    }
  }

  /** Add exclusion rules for dependencies already included in the spark-assembly 
   * 添加一个过滤规则,将spark本身装在的jar文件以及依赖文件过滤掉,因为spark环境里面已经存在了,不需要额外的jar包加载了
   **/
  def addExclusionRules(
      ivySettings: IvySettings,
      ivyConfName: String,
      md: DefaultModuleDescriptor): Unit = {
    // Add scala exclusion rule 过滤scala相关的类库
    md.addExcludeRule(createExclusion("*:scala-library:*", ivySettings, ivyConfName))

    // We need to specify each component explicitly, otherwise we miss spark-streaming-kafka and
    // other spark-streaming utility components. Underscore is there to differentiate between
    // spark-streaming_2.1x and spark-streaming-kafka-assembly_2.1x
    val components = Seq("bagel_", "catalyst_", "core_", "graphx_", "hive_", "mllib_", "repl_",
      "sql_", "streaming_", "yarn_", "network-common_", "network-shuffle_", "network-yarn_")

      //过滤spark自身的jar以及相关依赖
    components.foreach { comp =>
      md.addExcludeRule(createExclusion(s"org.apache.spark:spark-$comp*:*", ivySettings,
        ivyConfName))
    }
  }

  /** A nice function to use in tests as well. Values are dummy strings. */
  def getModuleDescriptor: DefaultModuleDescriptor = DefaultModuleDescriptor.newDefaultInstance(
    ModuleRevisionId.newInstance("org.apache.spark", "spark-submit-parent", "1.0"))

  /**
   * Resolves any dependencies that were supplied through maven coordinates
   * 分解任何maven的依赖
   * @param coordinates Comma-delimited string of maven coordinates用逗号分隔需要的maven坐标集合
   * @param remoteRepos Comma-delimited string of remote repositories other than maven central 用逗号分隔的maven资源中心
   * @param ivyPath The path to the local ivy repository 本地的maven仓库位置
   * @param exclusions Exclusions to apply when resolving transitive dependencies 当解析依赖关系的时候,要过滤的maven坐标集合
   * @return The comma-delimited path to the jars of the given maven artifacts including their
   *         transitive dependencies
   *  返回用逗号拆分的最终需要的maven坐标集合,包括的依赖的jar包
   *  真正去下载这些jar包,返回是一个路径集合,集合用逗号拆分,这些集合是已经下载下来的jar包
   */
  def resolveMavenCoordinates(
      coordinates: String,//需要的jar包的maven坐标集合
      remoteRepos: Option[String],//远程仓库集合
      ivyPath: Option[String],//本地maven仓库的资源位置
      exclusions: Seq[String] = Nil,//不需要的jar包集合
      isTest: Boolean = false): String = {
    if (coordinates == null || coordinates.trim.isEmpty) {//不需要任何maven依赖的jar包,因此返回空
      ""
    } else {
      val sysOut = System.out
      try {
        // To prevent ivy from logging to system out
        System.setOut(printStream)
        val artifacts = extractMavenCoordinates(coordinates) //从参数中提取maven坐标集合Seq[MavenCoordinate],该集合是所需要的jar包集合
        // Default configuration name for ivy
        val ivyConfName = "default"
        // set ivy settings for location of cache
        val ivySettings: IvySettings = new IvySettings
        // Directories for caching downloads through ivy and storing the jars when maven coordinates
        // are supplied to spark-submit
        val alternateIvyCache = ivyPath.getOrElse("")//本地maven资源库的位置
        val packagesDirectory: File =
          if (alternateIvyCache == null || alternateIvyCache.trim.isEmpty) {
            new File(ivySettings.getDefaultIvyUserDir, "jars")//设置默认路径C:\Users\Lenovo\.ivy2\jars
          } else {
            ivySettings.setDefaultIvyUserDir(new File(alternateIvyCache))//设置资源路径
            ivySettings.setDefaultCache(new File(alternateIvyCache, "cache"))
            new File(alternateIvyCache, "jars")//设置maven的资源路径/jar
          }
        // scalastyle:off println
        printStream.println(
          s"Ivy Default Cache set to: ${ivySettings.getDefaultCache.getAbsolutePath}")
        printStream.println(s"The jars for the packages stored in: $packagesDirectory")
        // scalastyle:on println
        // create a pattern matcher
        ivySettings.addMatcher(new GlobPatternMatcher)
        // create the dependency resolvers 加载资源
        val repoResolver = createRepoResolvers(remoteRepos, ivySettings)
        ivySettings.addResolver(repoResolver)
        ivySettings.setDefaultResolver(repoResolver.getName)

        val ivy = Ivy.newInstance(ivySettings)
        // Set resolve options to download transitive dependencies as well
        val resolveOptions = new ResolveOptions
        resolveOptions.setTransitive(true)
        val retrieveOptions = new RetrieveOptions
        // Turn downloading and logging off for testing
        if (isTest) {
          resolveOptions.setDownload(false)
          resolveOptions.setLog(LogOptions.LOG_QUIET)
          retrieveOptions.setLog(LogOptions.LOG_QUIET)
        } else {
          resolveOptions.setDownload(true)
        }

        // A Module descriptor must be specified. Entries are dummy strings
        val md = getModuleDescriptor
        // clear ivy resolution from previous launches. The resolution file is usually at
        // ~/.ivy2/org.apache.spark-spark-submit-parent-default.xml. In between runs, this file
        // leads to confusion with Ivy when the files can no longer be found at the repository
        // declared in that file/
        val mdId = md.getModuleRevisionId
        val previousResolution = new File(ivySettings.getDefaultCache,
          s"${mdId.getOrganisation}-${mdId.getName}-$ivyConfName.xml")
        if (previousResolution.exists) previousResolution.delete

        md.setDefaultConf(ivyConfName)

        // Add exclusion rules for Spark and Scala Library
        addExclusionRules(ivySettings, ivyConfName, md)
        // add all supplied maven artifacts as dependencies
        addDependenciesToIvy(md, artifacts, ivyConfName)
        exclusions.foreach { e =>
          md.addExcludeRule(createExclusion(e + ":*", ivySettings, ivyConfName))
        }
        // resolve dependencies
        val rr: ResolveReport = ivy.resolve(md, resolveOptions)
        if (rr.hasError) {
          throw new RuntimeException(rr.getAllProblemMessages.toString)
        }
        // retrieve all resolved dependencies
        ivy.retrieve(rr.getModuleDescriptor.getModuleRevisionId,
          packagesDirectory.getAbsolutePath + File.separator +
            "[organization]_[artifact]-[revision].[ext]",
          retrieveOptions.setConfs(Array(ivyConfName)))
        resolveDependencyPaths(rr.getArtifacts.toArray, packagesDirectory)
      } finally {
        System.setOut(sysOut)
      }
    }
  }

  //创建一个maven的依赖相关jar路径,该路径会被过滤掉
  private[deploy] def createExclusion(
      coords: String,
      ivySettings: IvySettings,
      ivyConfName: String): ExcludeRule = {
    val c = extractMavenCoordinates(coords)(0)
    val id = new ArtifactId(new ModuleId(c.groupId, c.artifactId), "*", "*", "*")
    val rule = new DefaultExcludeRule(id, ivySettings.getMatcher("glob"), null)
    rule.addConfiguration(ivyConfName)
    rule
  }

}

/**
 * Provides an indirection layer for passing arguments as system properties or flags to
 * the user's driver program or to downstream launcher tools.
 * 定义个规则
 */
private case class OptionAssigner(
    value: String,//具体的属性值
    clusterManager: Int,//该属性应该匹配的集群队列master什么是合法的
    deployMode: Int,//该属性应该匹配的集群队列mode什么是合法的
    clOption: String = null,//该属性从命令行中获取对应的值,该命令行的key是什么
    sysProp: String = null)//该属性从环境中获取对应的值,该环境的key是什么
