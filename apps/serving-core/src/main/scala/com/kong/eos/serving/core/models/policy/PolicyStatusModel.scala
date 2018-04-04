
package com.kong.eos.serving.core.models.policy

import com.kong.eos.serving.core.models.enumerators.PolicyStatusEnum

case class PolicyStatusModel(id: String,
                             status: PolicyStatusEnum.Value,
                             submissionId: Option[String] = None,
                             submissionStatus: Option[String] = None,
                             statusInfo: Option[String] = None,
                             name: Option[String] = None,
                             description: Option[String] = None,
                             lastExecutionMode: Option[String] = None,
                             lastError: Option[PolicyErrorModel] = None,
                             resourceManagerUrl: Option[String] = None,
                             marathonId: Option[String] = None)
