package com.kong.eos.sdk.pipeline.output

/**
  * Spark SQL Save mode Enum
  */
object SaveModeEnum extends Enumeration {

  type CloudSaveMode = Value

  // Append 追加信息
  val Append = Value("Append")

  // ErrorIfExists出现错误后，抛出错误
  val ErrorIfExists = Value("ErrorIfExists")

  // Ignore 如果存在则忽略
  val Ignore = Value("Ignore")

  // Overwrite 覆盖
  val Overwrite = Value("Overwrite")

  //
  val Upsert = Value("Upsert")

  val allSaveModes = Seq(Append, ErrorIfExists, Ignore, Overwrite, Upsert)
}
