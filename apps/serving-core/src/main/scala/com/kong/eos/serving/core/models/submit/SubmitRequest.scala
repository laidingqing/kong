package com.kong.eos.serving.core.models.submit

case class SubmitRequest(id: String,
                         driverClass: String,
                         driverFile: String,
                         master: String,
                         submitArguments: Map[String, String],
                         sparkConfigurations: Map[String, String],
                         driverArguments: Map[String, String],
                         executionMode: String,
                         killUrl: String,
                         sparkHome: Option[String] = None
                        )
