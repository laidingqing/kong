
package com.kong.eos.serving.core.utils

import java.io.Serializable
import java.net.URLClassLoader

import akka.event.slf4j.SLF4JLogging
import com.kong.eos.sdk.pipeline.aggregation.cube.DimensionType
import com.kong.eos.sdk.pipeline.aggregation.operator.Operator
import com.kong.eos.sdk.pipeline.input.Input
import com.kong.eos.sdk.pipeline.output.Output
import com.kong.eos.sdk.pipeline.transformation.Parser
import com.kong.eos.serving.core.exception.ServingCoreException
import org.reflections.Reflections

import scala.collection.JavaConversions._

class ReflectionUtils extends SLF4JLogging {

  def tryToInstantiate[C](classAndPackage: String, block: Class[_] => C): C = {
    val clazMap: Map[String, String] = getClasspathMap
    val finalClazzToInstance = clazMap.getOrElse(classAndPackage, classAndPackage)
    try {
      val clazz = Class.forName(finalClazzToInstance)
      block(clazz)
    } catch {
      case cnfe: ClassNotFoundException =>
        throw ServingCoreException.create(
          "Class with name " + classAndPackage + " Cannot be found in the classpath.", cnfe)
      case ie: InstantiationException =>
        throw ServingCoreException.create("Class with name " + classAndPackage + " cannot be instantiated", ie)
      case e: Exception =>
        throw ServingCoreException.create("Generic error trying to instantiate " + classAndPackage, e)
    }
  }

  def instantiateParameterizable[C](clazz: Class[_], properties: Map[String, Serializable]): C =
    clazz.getDeclaredConstructor(classOf[Map[String, Serializable]]).newInstance(properties).asInstanceOf[C]

  def printClassPath(cl: ClassLoader): Unit = {
    val urls = cl.asInstanceOf[URLClassLoader].getURLs()
    urls.foreach(url => log.debug(url.getFile))
  }

  lazy val getClasspathMap: Map[String, String] = {
    val reflections = new Reflections("com.ckmro.kongcloud")

    try {
      log.debug("#######")
      log.debug("####### SPARK MUTABLE_URL_CLASS_LOADER:")
      log.debug(getClass.getClassLoader.toString)
      printClassPath(getClass.getClassLoader)
      log.debug("#######")
      log.debug("####### APP_CLASS_LOADER / SYSTEM CLASSLOADER:")
      log.debug(ClassLoader.getSystemClassLoader().toString)
      printClassPath(ClassLoader.getSystemClassLoader())
      log.debug("#######")
      log.debug("####### EXTRA_CLASS_LOADER:")
      log.debug(getClass.getClassLoader.getParent.getParent.toString)
      printClassPath(getClass.getClassLoader.getParent.getParent)
    } catch {
      case e: Exception => //nothing
    }

    val inputs = reflections.getSubTypesOf(classOf[Input]).toList
    val dimensionTypes = reflections.getSubTypesOf(classOf[DimensionType]).toList
    val operators = reflections.getSubTypesOf(classOf[Operator]).toList
    val outputs = reflections.getSubTypesOf(classOf[Output]).toList
    val parsers = reflections.getSubTypesOf(classOf[Parser]).toList
    val plugins = inputs ++ dimensionTypes ++ operators ++ outputs ++ parsers
    val result = plugins map (t => t.getSimpleName -> t.getCanonicalName) toMap

    log.debug("#######")
    log.debug("####### Plugins to be loaded:")
    result.foreach {
      case (simpleName: String, canonicalName: String) => log.debug(s"$canonicalName")
    }

    result
  }
}