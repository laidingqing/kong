
package com.kong.eos.driver.stage

import java.io.Serializable

import akka.event.slf4j.SLF4JLogging
import com.kong.eos.sdk.pipeline.output.Output
import com.kong.eos.sdk.pipeline.transformation.Parser
import com.kong.eos.serving.core.constants.AppConstant
import com.kong.eos.serving.core.models.policy.{PhaseEnum, TransformationModel}
import com.kong.eos.serving.core.utils.ReflectionUtils
import com.kong.eos.driver.writer.{TransformationsWriterHelper, WriterOptions}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.DStream

import scala.util.{Failure, Success, Try}

trait ParserStage extends BaseStage {
  this: ErrorPersistor =>

  def parserStage(refUtils: ReflectionUtils,
                  schemas: Map[String, StructType]): (Seq[Parser], Option[WriterOptions]) =
    (policy.transformations.get.transformationsPipe.map(parser => createParser(parser, refUtils, schemas)),
      policy.transformations.get.writer.map(writer => WriterOptions(
        writer.outputs,
        writer.saveMode,
        writer.tableName,
        getAutoCalculatedFields(writer.autoCalculatedFields),
        writer.partitionBy,
        writer.primaryKey
      )))

  private[driver] def createParser(model: TransformationModel,
                           refUtils: ReflectionUtils,
                           schemas: Map[String, StructType]): Parser = {
    val classType = model.configuration.getOrElse(AppConstant.CustomTypeKey, model.`type`).toString
    val errorMessage = s"Something gone wrong creating the parser: $classType. Please re-check the policy."
    val okMessage = s"Parser: $classType created correctly."
    generalTransformation(PhaseEnum.Parser, okMessage, errorMessage) {
      val outputFieldsNames = model.outputFieldsTransformed.map(_.name)
      val schema = schemas.getOrElse(model.order.toString, throw new Exception("Can not find transformation schema"))
      refUtils.tryToInstantiate[Parser](classType + Parser.ClassSuffix, (c) =>
        c.getDeclaredConstructor(
          classOf[Integer],
          classOf[Option[String]],
          classOf[Seq[String]],
          classOf[StructType],
          classOf[Map[String, Serializable]])
          .newInstance(model.order, model.inputField, outputFieldsNames, schema, model.configuration)
          .asInstanceOf[Parser])
    }
  }
}

object ParserStage extends SLF4JLogging {

  def executeParsers(row: Row, parsers: Seq[Parser]): Seq[Row] =
    if (parsers.size == 1) parseEvent(row, parsers.head)
    else parseEvent(row, parsers.head).flatMap(eventParsed => executeParsers(eventParsed, parsers.drop(1)))

  def parseEvent(row: Row, parser: Parser): Seq[Row] =
    Try {
      parser.parse(row)
    } match {
      case Success(eventParsed) =>
        eventParsed
      case Failure(exception) =>
        val error = s"Failure[Parser]: ${row.mkString(",")} | Message: ${exception.getLocalizedMessage}" +
          s" | Parser: ${parser.getClass.getSimpleName}"
        log.error(error, exception)
        Seq.empty[Row]
    }

  def applyParsers(input: DStream[Row],
                   parsers: Seq[Parser],
                   schema: StructType,
                   outputs: Seq[Output],
                   writerOptions: Option[WriterOptions]): DStream[Row] = {
    val transformedData = if (parsers.isEmpty) input
    else input.flatMap(row => executeParsers(row, parsers))

    writerOptions.foreach(options =>
      TransformationsWriterHelper.writeTransformations(transformedData, schema, outputs, options))
    transformedData
  }
}
