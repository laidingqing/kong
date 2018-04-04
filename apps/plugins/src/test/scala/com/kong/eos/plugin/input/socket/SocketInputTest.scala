
package com.kong.eos.plugin.input.socket

import java.io.{Serializable => JSerializable}

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class SocketInputTest extends WordSpec {

  "A SocketInput" should {
    "instantiate successfully with parameters" in {
      new SocketInput(Map("hostname" -> "localhost", "port" -> 9999).mapValues(_.asInstanceOf[JSerializable]))
    }
    "fail without parameters" in {
      intercept[IllegalStateException] {
        new SocketInput(Map())
      }
    }
    "fail with bad port argument" in {
      intercept[IllegalStateException] {
        new SocketInput(Map("hostname" -> "localhost", "port" -> "BADPORT").mapValues(_.asInstanceOf[JSerializable]))
      }
    }
  }
}
