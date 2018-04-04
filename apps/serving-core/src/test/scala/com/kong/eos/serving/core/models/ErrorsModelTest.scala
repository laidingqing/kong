
package com.kong.eos.serving.core.models

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}

@RunWith(classOf[JUnitRunner])
class ErrorsModelTest extends WordSpec with Matchers {

  val error = new ErrorModel("100", "Error 100", None, None)

  "ErrorModel" should {

    "toString method should return the number of the error and the error" in {
      val res = ErrorModel.toString(error)
      res should be ("""{"i18nCode":"100","message":"Error 100"}""")
    }

    "toError method should return the number of the error and the error" in {
      val res = ErrorModel.toErrorModel(
        """
          |{
          | "i18nCode": "100",
          | "message": "Error 100"
          |}
        """.stripMargin)
      res should be (error)
    }
  }
}
