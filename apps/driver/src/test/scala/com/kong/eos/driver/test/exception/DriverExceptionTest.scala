
package com.kong.eos.driver.test.exception

import com.kong.eos.driver.exception.DriverException
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DriverExceptionTest extends FlatSpec with ShouldMatchers {

  "DriverException" should "return a Throwable" in {

    val msg = "my custom exception"

    val ex = DriverException(msg)

    ex.getMessage should be(msg)
  }
  it should "return a exception with the msg and a cause" in {

    val msg = "my custom exception"

    val ex = DriverException(msg, new RuntimeException("cause"))

    ex.getCause.getMessage should be("cause")
  }

}
