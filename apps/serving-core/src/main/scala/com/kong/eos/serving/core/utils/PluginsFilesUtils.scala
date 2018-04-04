
package com.kong.eos.serving.core.utils

import java.io.File
import java.net.URL
import java.util.{Calendar, UUID}

import akka.event.slf4j.SLF4JLogging
import com.kong.eos.serving.core.helpers.JarsHelper
import org.apache.commons.io.FileUtils

trait PluginsFilesUtils extends SLF4JLogging {

  def addPluginsToClassPath(pluginsFiles: Array[String]): Unit = {
    log.info(pluginsFiles.mkString(","))
    pluginsFiles.foreach(filePath => {
      log.info(s"Adding to classpath plugin file: $filePath")
      if (filePath.startsWith("/") || filePath.startsWith("file://")) addFromLocal(filePath)
      if (filePath.startsWith("hdfs")) addFromHdfs(filePath)
      if (filePath.startsWith("http")) addFromHttp(filePath)
    })
  }

  private def addFromLocal(filePath: String): Unit = {
    log.info(s"Getting file from local: $filePath")
    val file = new File(filePath.replace("file://", ""))
    JarsHelper.addToClasspath(file)
  }

  private def addFromHdfs(fileHdfsPath: String): Unit = {
    log.info(s"Getting file from HDFS: $fileHdfsPath")
    val inputStream = HdfsUtils().getFile(fileHdfsPath)
    val fileName = fileHdfsPath.split("/").last
    log.info(s"HDFS file name is $fileName")
    val file = new File(s"/tmp/sparta/userjars/${UUID.randomUUID().toString}/$fileName")
    log.info(s"Downloading HDFS file to local file system: ${file.getAbsoluteFile}")
    FileUtils.copyInputStreamToFile(inputStream, file)
    JarsHelper.addToClasspath(file)
  }

  private def addFromHttp(fileURI: String): Unit = {
    log.info(s"Getting file from HTTP: $fileURI")
    val tempFile = File.createTempFile(s"sparta-plugin-${Calendar.getInstance().getTimeInMillis}", ".jar")
    val url = new URL(fileURI)
    FileUtils.copyURLToFile(url, tempFile)
    JarsHelper.addToClasspath(tempFile)
  }
}
