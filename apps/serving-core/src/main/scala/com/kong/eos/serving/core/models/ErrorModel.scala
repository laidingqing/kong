
package com.kong.eos.serving.core.models

import org.json4s.native.Serialization._

/**
 * An ErrorDto represents an error that will be sent as response to the frontend.
 *
 * @param i18nCode          with the code of the error that will be translated.
 * @param message           that describes the exception.
 * @param stackTraceElement with the stackTrace of the exception.
 * @param params            with values that could be needed by the frontend.
 */
case class ErrorModel(i18nCode: String,
                      message: String,
                      subErrorModels: Option[Seq[ErrorModel]] = None,
                      stackTraceElement: Option[Seq[StackTraceElement]] = None,
                      params: Option[Map[Any, Any]] = None) {}

object ErrorModel extends KongCloudSerializer {

  val CodeExistsFragmentWithName = "100"
  val CodeNotExistsFragmentWithId = "101"
  val CodeNotExistsFragmentWithName = "102"
  val CodeNotExistsFragmentWithType = "103"
  val CodeErrorGettingAllFragments = "104"
  val CodeErrorDeletingAllFragments = "105"
  val CodeExistsPolicyWithName = "200"
  val CodeNotExistsPolicyWithId = "201"
  val CodeNotExistsPolicyWithName = "202"
  val CodeErrorCreatingPolicy = "203"
  val CodeErrorDeletingPolicy = "204"
  val CodeErrorUpdatingPolicy = "205"
  val CodeErrorUpdatingExecution = "206"
  val CodeNotExistsTemplateWithName = "300"
  val CodePolicyIsWrong = "305"
  val ValidationError = "400"
  val ErrorForPolicyNotFound = "401"
  val CodeUnknown = "666"

  val ValidationError_There_is_at_least_one_cube_without_name = "4000"
  val ValidationError_There_is_at_least_one_cube_without_dimensions = "4001"
  val ValidationError_The_policy_needs_at_least_one_cube_or_one_trigger_or_raw_data_or_transformations_with_save = "4003"
  val ValidationError_There_is_at_least_one_cube_with_a_bad_output = "4004"
  val ValidationError_There_is_at_least_one_cube_with_triggers_with_a_bad_output = "4005"
  val ValidationError_There_is_at_least_one_stream_trigger_with_a_bad_output = "4006"
  val ValidationError_There_is_at_least_one_trigger_with_a_bad_window_attribute = "4007"
  val ValidationError_Raw_data_with_a_bad_output = "4008"
  val ValidationError_Raw_data_with_bad_data_field = "4009"
  val ValidationError_Raw_data_with_bad_time_field = "4010"
  val ValidationError_Raw_data_with_bad_table_name = "4011"
  val ValidationError_Transformations_with_a_bad_output = "4012"

  def toString(errorModel: ErrorModel): String = write(errorModel)

  def toErrorModel(json: String): ErrorModel = read[ErrorModel](json)
}