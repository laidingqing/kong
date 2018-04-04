
package com.kong.eos.serving.api.utils

import java.io.{BufferedOutputStream, File, FileOutputStream}
import java.net.InetAddress
import java.text.DecimalFormat
import java.util.function.Predicate

import akka.event.slf4j.SLF4JLogging
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.core.config.KongCloudConfig
import com.kong.eos.serving.core.models.files.KongCloudFile
import com.kong.eos.serving.api.constants.HttpConstant
import spray.http.BodyPart

import scala.util.{Failure, Success, Try}

trait FileActorUtils extends SLF4JLogging {

  //The dir where the files will be saved
  val targetDir: String
  val apiPath: String

  //Regexp for name validation
  val patternFileName: Option[Predicate[String]] = None

  def deleteFiles(): Try[_] =
    Try {
      val directory = new File(targetDir)
      if (directory.exists && directory.isDirectory)
        directory.listFiles.filter(_.isFile).toList.foreach { file =>
          if (patternFileName.isEmpty || (patternFileName.isDefined && patternFileName.get.test(file.getName)))
            file.delete()
        }
    }

  def deleteFile(fileName: String): Try[_] =
    Try {
      val plugin = new File(s"$targetDir/$fileName")
      if (plugin.exists && !plugin.isDirectory)
        plugin.delete()
    }

  def browseDirectory(): Try[Seq[KongCloudFile]] =
    Try {
      val directory = new File(targetDir)
      if (directory.exists && directory.isDirectory) {
        directory.listFiles.filter(_.isFile).toList.flatMap { file =>
          if (patternFileName.isEmpty || (patternFileName.isDefined && patternFileName.get.test(file.getName)))
            Option(KongCloudFile(file.getName, s"$url/${file.getName}", file.getAbsolutePath,
              sizeToMbFormat(file.length())))
          else None
        }
      } else Seq.empty[KongCloudFile]
    }

  def uploadFiles(files: Seq[BodyPart]): Try[Seq[KongCloudFile]] =
    Try {
      files.flatMap { file =>
        val fileNameOption = file.filename.orElse(file.name.orElse {
          log.warn(s"Is necessary one file name to upload files")
          None
        })
        fileNameOption.flatMap { fileName =>
          if (patternFileName.isEmpty || (patternFileName.isDefined && patternFileName.get.test(fileName))) {
            val localMachineDir = s"$targetDir/$fileName"

            Try(saveFile(file.entity.data.toByteArray, localMachineDir)) match {
              case Success(newFile) =>
                Option(KongCloudFile(fileName, s"$url/$fileName", localMachineDir, sizeToMbFormat(newFile.length())))
              case Failure(e) =>
                log.error(s"Error saving file in path $localMachineDir", e)
                None
            }
          } else {
            log.warn(s"$fileName is Not a valid file name")
            None
          }
        }
      }
    }

  private def sizeToMbFormat(size: Long): String = {
    val formatter = new DecimalFormat("####.##")
    s"${formatter.format(size.toDouble / (1024 * 1024))} MB"
  }

  private def saveFile(array: Array[Byte], fileName: String): File = {
    log.info(s"Saving file to: $fileName")
    new File(fileName).getParentFile.mkdirs
    val bos = new BufferedOutputStream(new FileOutputStream(fileName))
    bos.write(array)
    bos.close()
    new File(fileName)
  }

  private def url: String = {
    val host = Try(InetAddress.getLocalHost.getHostName).getOrElse(KongCloudConfig.apiConfig.get.getString("host"))
    val port = KongCloudConfig.apiConfig.get.getInt("port")

    s"http://$host:$port/${HttpConstant.KongCloudRootPath}/$apiPath"
  }
}
