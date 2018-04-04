
package com.kong.eos.serving.core.models.submit

case class SubmissionResponse(action: String,
                              message: Option[String],
                              serverSparkVersion: String,
                              submissionId: String,
                              success: Boolean)
