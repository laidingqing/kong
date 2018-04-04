
package com.kong.eos.serving.core.helpers

import java.io.File
import java.lang.reflect.Method
import java.net.{URL, URLClassLoader}

import akka.event.slf4j.SLF4JLogging

object JarsHelper extends SLF4JLogging {

  /**
   * Finds files that are the driver application.
   * @param path base path when it starts to scan in order to find plugins.
   * @return a list of jars.
   */
  def findDriverByPath(path : File) : Seq[File] = {
    if (path.exists) {
      val these = path.listFiles()
      val good =
        these.filter(f => f.getName.toLowerCase.contains("driver") &&
          f.getName.toLowerCase.contains("sparta") &&
          f.getName.endsWith(".jar"))

      good ++ these.filter(_.isDirectory).flatMap(path => findDriverByPath(path))
    } else {
      log.warn(s"The file ${path.getName} not exists.")
      Seq()
    }
  }

  /**
   * Adds a file to the classpath of the application.
   * @param file to add in the classpath.
   */
  def addToClasspath(file : File) : Unit = {
    if (file.exists) {
      val method : Method = classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[URL])

      method.setAccessible(true)
      method.invoke(getClass.getClassLoader, file.toURI.toURL)
    } else {
      log.warn(s"The file ${file.getName} not exists.")
    }
  }

}
