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

package org.apache.spark.sql.hive.client

import java.io.File
import java.lang.reflect.InvocationTargetException
import java.net.{URL, URLClassLoader}
import java.util

import scala.language.reflectiveCalls
import scala.util.Try

import org.apache.commons.io.{FileUtils, IOUtils}

import org.apache.spark.Logging
import org.apache.spark.deploy.SparkSubmitUtils
import org.apache.spark.util.Utils

import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.hive.HiveContext

/** Factory for `IsolatedClientLoader` with specific versions of hive.
  * 为给定的hive版本号,创建一个IsolatedClientLoader
  **/
private[hive] object IsolatedClientLoader {
  /**
   * Creates isolated Hive client loaders by downloading the requested version from maven.
   * 从maven上下载一些jar包
   */
  def forVersion(
      version: String,//hive的版本号
      config: Map[String, String] = Map.empty,//需要的配置信息
      ivyPath: Option[String] = None,//本地maven仓库的资源位置
      sharedPrefixes: Seq[String] = Seq.empty,
      barrierPrefixes: Seq[String] = Seq.empty): IsolatedClientLoader = synchronized {
    val resolvedVersion = hiveVersion(version)
    val files = resolvedVersions.getOrElseUpdate(resolvedVersion,
      downloadVersion(resolvedVersion, ivyPath)) //从缓存中获取该版本对应要加载的jar的url集合,如果第一次出现该hive版本,则下载jar包到本地,并且返回本地路径集合
    new IsolatedClientLoader(
      version = hiveVersion(version),
      execJars = files,
      config = config,
      sharedPrefixes = sharedPrefixes,
      barrierPrefixes = barrierPrefixes)
  }

  //通过hive的版本字符串,转换成hive版本对象
  def hiveVersion(version: String): HiveVersion = version match {
    case "12" | "0.12" | "0.12.0" => hive.v12
    case "13" | "0.13" | "0.13.0" | "0.13.1" => hive.v13
    case "14" | "0.14" | "0.14.0" => hive.v14
    case "1.0" | "1.0.0" => hive.v1_0
    case "1.1" | "1.1.0" => hive.v1_1
    case "1.2" | "1.2.0" | "1.2.1" => hive.v1_2
  }

  //下载hive版本需要的jar包---返回的就是下载到本地的jar包文件集合
  private def downloadVersion(version: HiveVersion, ivyPath: Option[String]): Seq[URL] = {
    val hiveArtifacts = version.extraDeps ++
      Seq("hive-metastore", "hive-exec", "hive-common", "hive-serde")
        .map(a => s"org.apache.hive:$a:${version.fullVersion}") ++ //追加hive的若干个需要的jar包,因为版本号也是已知的,因此可以有maven全路径
      Seq("com.google.guava:guava:14.0.1",
        "org.apache.hadoop:hadoop-client:2.4.0") //追加hadoop和google的包

    //真正从maven上去下载jar包到本地
    val classpath = quietly {
      SparkSubmitUtils.resolveMavenCoordinates(
        hiveArtifacts.mkString(","),
        Some("http://www.datanucleus.org/downloads/maven2"),
        ivyPath,
        exclusions = version.exclusions)//不下载该jar包
    }
    val allFiles = classpath.split(",").map(new File(_)).toSet //获取所有的文件集合

    // TODO: Remove copy logic.
    val tempDir = Utils.createTempDir(namePrefix = s"hive-${version}")//为hive的版本号创建一个临时文件夹
    allFiles.foreach(f => FileUtils.copyFileToDirectory(f, tempDir))//将maven下载的所有文件copy到该临时文件夹下
    tempDir.listFiles().map(_.toURI.toURL) //返回本地的临时文件的内容集合
  }

  //缓存,为每一个hive的版本,缓存要从maven上加载哪些jar集合
  private def resolvedVersions = new scala.collection.mutable.HashMap[HiveVersion, Seq[URL]]
}

/**
 * Creates a Hive `ClientInterface` using a classloader that works according to the following rules:
 *  - Shared classes: Java, Scala, logging, and Spark classes are delegated to `baseClassLoader`
 *    allowing the results of calls to the `ClientInterface` to be visible externally.
 *  - Hive classes: new instances are loaded from `execJars`.  These classes are not
 *    accessible externally due to their custom loading.
 *  - ClientWrapper: a new copy is created for each instance of `IsolatedClassLoader`.
 *    This new instance is able to see a specific version of hive without using reflection. Since
 *    this is a unique instance, it is not visible externally other than as a generic
 *    `ClientInterface`, unless `isolationOn` is set to `false`.
 *
 * @param version The version of hive on the classpath.  used to pick specific function signatures
 *                that are not compatible across versions.
 * @param execJars A collection of jar files that must include hive and hadoop. 执行hive的jar包集合
 * @param config   A set of options that will be added to the HiveConf of the constructed client.将要添加到HiveConf上的配置信息
 * @param isolationOn When true, custom versions of barrier classes will be constructed.  Must be
 *                    true unless loading the version of hive that is on Sparks classloader.
 * @param rootClassLoader The system root classloader. Must not know about Hive classes.
 * @param baseClassLoader The spark classloader that is used to load shared classes.
 * 为不同版本创建一个hive的实例对象
 */
private[hive] class IsolatedClientLoader(
    val version: HiveVersion,//当前使用的hive的版本号
    val execJars: Seq[URL] = Seq.empty,//执行hive的jar包集合
    val config: Map[String, String] = Map.empty,//将要添加到HiveConf上的配置信息
    val isolationOn: Boolean = true,
    val rootClassLoader: ClassLoader = ClassLoader.getSystemClassLoader.getParent.getParent,
    val baseClassLoader: ClassLoader = Thread.currentThread().getContextClassLoader,
    val sharedPrefixes: Seq[String] = Seq.empty,
    val barrierPrefixes: Seq[String] = Seq.empty)
  extends Logging {

  // Check to make sure that the root classloader does not know about Hive.必须确保root不能知道hive
  assert(Try(rootClassLoader.loadClass("org.apache.hadoop.hive.conf.HiveConf")).isFailure)

  /** All jars used by the hive specific classloader. */
  protected def allJars = execJars.toArray

  protected def isSharedClass(name: String): Boolean =
    name.contains("slf4j") ||
    name.contains("log4j") ||
    name.startsWith("org.apache.spark.") ||
    name.startsWith("scala.") ||
    (name.startsWith("com.google") && !name.startsWith("com.google.cloud")) ||
    name.startsWith("java.lang.") ||
    name.startsWith("java.net") ||
    sharedPrefixes.exists(name.startsWith)

  /** True if `name` refers to a spark class that must see specific version of Hive. */
  protected def isBarrierClass(name: String): Boolean =
    name.startsWith(classOf[ClientWrapper].getName) ||
    name.startsWith(classOf[Shim].getName) ||
    barrierPrefixes.exists(name.startsWith)

  protected def classToPath(name: String): String =
    name.replaceAll("\\.", "/") + ".class"

  /** The classloader that is used to load an isolated version of Hive. */
  protected val classLoader: ClassLoader = new URLClassLoader(allJars, rootClassLoader) {//使用rootClassLoader类加载器加载这些jar文件
    override def loadClass(name: String, resolve: Boolean): Class[_] = {
      val loaded = findLoadedClass(name)
      if (loaded == null) doLoadClass(name, resolve) else loaded
    }

    def doLoadClass(name: String, resolve: Boolean): Class[_] = {
      val classFileName = name.replaceAll("\\.", "/") + ".class"
      if (isBarrierClass(name) && isolationOn) {
        // For barrier classes, we construct a new copy of the class.
        val bytes = IOUtils.toByteArray(baseClassLoader.getResourceAsStream(classFileName))
        logDebug(s"custom defining: $name - ${util.Arrays.hashCode(bytes)}")
        defineClass(name, bytes, 0, bytes.length)
      } else if (!isSharedClass(name)) {
        logDebug(s"hive class: $name - ${getResource(classToPath(name))}")
        super.loadClass(name, resolve)
      } else {
        // For shared classes, we delegate to baseClassLoader.
        logDebug(s"shared class: $name")
        baseClassLoader.loadClass(name)
      }
    }
  }

  // Pre-reflective instantiation setup.
  logDebug("Initializing the logger to avoid disaster...")
  Thread.currentThread.setContextClassLoader(classLoader)

  /** The isolated client interface to Hive. */
  val client: ClientInterface = try {
    classLoader
      .loadClass(classOf[ClientWrapper].getName)
      .getConstructors.head //找到第一个构造函数
      .newInstance(version, config, classLoader)
      .asInstanceOf[ClientInterface]
  } catch {
    case e: InvocationTargetException =>
      if (e.getCause().isInstanceOf[NoClassDefFoundError]) {
        val cnf = e.getCause().asInstanceOf[NoClassDefFoundError]
        throw new ClassNotFoundException(
          s"$cnf when creating Hive client using classpath: ${execJars.mkString(", ")}\n" +
           "Please make sure that jars for your version of hive and hadoop are included in the " +
          s"paths passed to ${HiveContext.HIVE_METASTORE_JARS}.")
      } else {
        throw e
      }
  } finally {
    Thread.currentThread.setContextClassLoader(baseClassLoader)
  }
}
