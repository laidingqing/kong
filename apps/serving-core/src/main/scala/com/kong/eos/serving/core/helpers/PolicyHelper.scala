
package com.kong.eos.serving.core.helpers

import java.io.{File, Serializable}

import akka.event.slf4j.SLF4JLogging
import com.kong.eos.serving.core.models.policy.PolicyModel
import com.kong.eos.serving.core.constants.AppConstant
import com.kong.eos.serving.core.models.policy.{PolicyElementModel, PolicyModel}
import com.kong.eos.serving.core.utils.ReflectionUtils
import com.typesafe.config.Config

import scala.collection.JavaConversions._

object PolicyHelper extends SLF4JLogging {

  lazy val ReflectionUtils = new ReflectionUtils

  def jarsFromPolicy(apConfig: PolicyModel): Seq[File] =
    apConfig.userPluginsJars.filter(!_.jarPath.isEmpty).map(_.jarPath).distinct.map(filePath => new File(filePath))

  def getSparkConfigFromPolicy(policy: PolicyModel): Map[String, String] =
    policy.sparkConf.flatMap { sparkProperty =>
      if (sparkProperty.sparkConfKey.isEmpty || sparkProperty.sparkConfValue.isEmpty)
        None
      else Option((sparkProperty.sparkConfKey, sparkProperty.sparkConfValue))
    }.toMap

  def getSparkConfigs(elements: Seq[PolicyElementModel], methodName: String, suffix: String): Map[String, String] = {
    log.info("Initializing reflection")
    elements.flatMap(o => {
      val classType = o.configuration.getOrElse(AppConstant.CustomTypeKey, o.`type`).toString
      val clazzToInstance = ReflectionUtils.getClasspathMap.getOrElse(classType + suffix, o.`type` + suffix)
      val clazz = Class.forName(clazzToInstance)
      clazz.getMethods.find(p => p.getName == methodName) match {
        case Some(method) =>
          method.setAccessible(true)
          method.invoke(clazz, o.configuration.asInstanceOf[Map[String, Serializable]])
            .asInstanceOf[Seq[(String, String)]]
        case None =>
          Seq.empty[(String, String)]
      }
    }).toMap
  }

  def getSparkConfFromProps(clusterConfig: Config): Map[String, String] =
    clusterConfig.entrySet()
      .filter(_.getKey.startsWith("spark.")).toSeq
      .map(e => (e.getKey, e.getValue.unwrapped.toString)).toMap

}
