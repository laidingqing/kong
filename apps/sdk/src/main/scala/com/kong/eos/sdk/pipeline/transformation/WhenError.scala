package com.kong.eos.sdk.pipeline.transformation

object WhenError extends Enumeration {

  type WhenError = Value
  val Error, Discard, Null = Value

}
