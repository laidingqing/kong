
package com.kong.eos.serving.core.models.policy

import com.kong.eos.sdk.pipeline.transformation.Parser
import com.kong.eos.sdk.properties.JsoneyString

case class TransformationModel(`type`: String,
                               order: Integer,
                               inputField: Option[String] = None,
                               outputFields: Seq[OutputFieldsModel],
                               configuration: Map[String, JsoneyString] = Map()) {

  val outputFieldsTransformed = outputFields.map(field =>
    OutputFieldsTransformedModel(field.name,
      field.`type`.getOrElse(Parser.TypesFromParserClass.getOrElse(`type`.toLowerCase, Parser.DefaultOutputType))
    ))
}

case class OutputFieldsModel(name: String, `type`: Option[String] = None)

case class OutputFieldsTransformedModel(name: String, `type`: String)
