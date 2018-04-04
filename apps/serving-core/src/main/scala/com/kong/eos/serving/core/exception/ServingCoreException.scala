
package com.kong.eos.serving.core.exception

class ServingCoreException(msg: String) extends RuntimeException(msg)

object ServingCoreException {

  def create(msg: String): ServingCoreException = new ServingCoreException(msg)

  def create(msg: String, cause: Throwable): Throwable = new ServingCoreException(msg).initCause(cause)
}
