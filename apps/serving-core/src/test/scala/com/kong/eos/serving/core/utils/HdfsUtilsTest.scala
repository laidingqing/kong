
package com.kong.eos.serving.core.utils

import java.io.{FileNotFoundException, InputStream}

import org.apache.hadoop.fs.{FileSystem, _}
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

import scala.util.{Failure, Try}

@RunWith(classOf[JUnitRunner])
class HdfsUtilsTest extends FlatSpec with ShouldMatchers with MockitoSugar {

  val fileSystem: FileSystem = mock[FileSystem]

  val utils = new HdfsUtils(fileSystem, "stratio")

  "hdfs utils" should "getfiles from a path" in {
    val expected = Array(mock[FileStatus])
    when(fileSystem.listStatus(new Path("myTestPath"))).thenReturn(expected)
    val result = utils.getFiles("myTestPath")
    result should be(expected)
  }

  it should "return single file as inputStream" in {
    val expected: InputStream = mock[FSDataInputStream]
    when(fileSystem.open(new Path("testFile"))).thenReturn(expected.asInstanceOf[FSDataInputStream])
    val result: InputStream = utils.getFile("testFile")
    result should be(expected)
  }

  it should "write" in {
    val result = Try(utils.write("from", "to", true)) match {
      case Failure(ex: Throwable) => ex
    }
    result.isInstanceOf[FileNotFoundException] should be(true)
  }

  it should "write without override" in {
    val result = Try(utils.write("from", "to", false)) match {
      case Failure(ex: Throwable) => ex
    }
    result.isInstanceOf[FileNotFoundException] should be(true)
  }
}


