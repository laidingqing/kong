package com.kong.eos.sdk.pipeline.filter

import org.json4s.JsonAST.JValue

case class FilterModel(field: String,
                       `type`: String,
                       value: Option[JValue] = None,
                       fieldValue: Option[String] = None,
                       fieldType: Option[String] = None)
