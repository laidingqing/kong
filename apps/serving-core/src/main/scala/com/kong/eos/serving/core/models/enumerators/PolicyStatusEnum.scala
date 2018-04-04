
package com.kong.eos.serving.core.models.enumerators

/**
 * Possible states that a policy could be when it was run.
 *
 * Launched: Sparta performs a spark-submit to the cluster.
 * Starting: SpartaJob tries to start the job.
 * Started: if the job was successfully started and the receiver is running.
 * Failed: if the lifecycle fails.
 * Stopping: Sparta sends a stop signal to the job to stop it gracefully.
 * Stopped: the job is stopped.
 */
object PolicyStatusEnum extends Enumeration {

  type status = Value

  val Launched = Value("Launched")
  val Starting = Value("Starting")
  val Started = Value("Started")
  val Failed = Value("Failed")
  val Stopping = Value("Stopping")
  val Stopped = Value("Stopped")
  val Finished = Value("Finished")
  val Killed = Value("Killed")
  val NotStarted = Value("NotStarted")
  val Uploaded = Value("Uploaded")
  val NotDefined = Value("NotDefined")
}
