
package com.kong.eos.serving.core.utils

import java.io.File

import com.kong.eos.serving.core.config.KongCloudConfig
import com.kong.eos.serving.core.constants.AppConstant
import com.kong.eos.serving.core.helpers.JarsHelper
import com.kong.eos.serving.core.models.policy.PolicyModel

import scala.util.Try

case class ClusterSparkFilesUtils(policy: PolicyModel, hdfs: HdfsUtils) extends CheckpointUtils {

  private val hdfsConfig = KongCloudConfig.getHdfsConfig.get
  private val host = hdfsConfig.getString(AppConstant.HdfsMaster)
  private val port = hdfsConfig.getInt(AppConstant.HdfsPort)

  def uploadDriverFile(driverJarPath: String): String = {
    val driverJar =
      JarsHelper.findDriverByPath(new File(
        Try(KongCloudConfig.getDetailConfig.get.getString(AppConstant.DriverPackageLocation))
        .getOrElse(AppConstant.DefaultDriverPackageLocation))).head

    log.info(s"Uploading Sparta Driver jar ($driverJar) to HDFS cluster .... ")

    val driverJarPathParsed = driverJarPath.replace("hdfs://", "") + {
      if(driverJarPath.endsWith("/")) "" else "/"
    }
    hdfs.write(driverJar.getAbsolutePath, driverJarPathParsed, overwrite = true)

    val uploadedFilePath = if(isHadoopEnvironmentDefined) s"hdfs://$driverJarPathParsed${driverJar.getName}"
    else s"hdfs://$host:$port$driverJarPathParsed${driverJar.getName}"

    log.info(s"Uploaded Sparta Driver jar to HDFS path: $uploadedFilePath")

    uploadedFilePath
  }
}
