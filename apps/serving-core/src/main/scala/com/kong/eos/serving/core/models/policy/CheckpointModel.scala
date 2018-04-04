
package com.kong.eos.serving.core.models.policy

case class CheckpointModel(timeDimension: String,
                          granularity: String,
                          interval: Option[Integer],
                          timeAvailability: Long)