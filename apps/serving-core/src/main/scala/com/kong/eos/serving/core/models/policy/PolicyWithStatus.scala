
package com.kong.eos.serving.core.models.policy

import com.kong.eos.serving.core.models.enumerators.PolicyStatusEnum

case class PolicyWithStatus(status: PolicyStatusEnum.Value,
                            policy: PolicyModel,
                            submissionId: Option[String] = None,
                            statusInfo: Option[String] = None,
                            lastExecutionMode: Option[String] = None,
                            lastError: Option[PolicyErrorModel] = None)
