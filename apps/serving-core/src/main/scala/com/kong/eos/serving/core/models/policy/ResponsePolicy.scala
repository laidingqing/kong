package com.kong.eos.serving.core.models.policy

import scala.util.Try

case class ResponsePolicy(policy: Try[PolicyModel])
