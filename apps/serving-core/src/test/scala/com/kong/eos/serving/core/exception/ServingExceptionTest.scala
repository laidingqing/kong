
package com.kong.eos.serving.core.exception

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}

@RunWith(classOf[JUnitRunner])
class ServingExceptionTest extends WordSpec with Matchers {

  "A ServingException" should {
    "create an exception with message" in {
      ServingCoreException.create("message").getMessage should be("message")
    }
    "create an exception with message and a cause" in {
      val cause = new IllegalArgumentException("any exception")
      val exception = ServingCoreException.create("message", cause)
      exception.getMessage should be("message")
      exception.getCause should be theSameInstanceAs(cause)
    }
  }
}
