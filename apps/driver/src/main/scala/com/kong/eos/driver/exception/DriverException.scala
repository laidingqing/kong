
package com.kong.eos.driver.exception

case class DriverException(msg: String) extends RuntimeException(msg)

object DriverException {

  def apply(msg: String, cause: Throwable): Throwable = new DriverException(msg).initCause(cause)
}
