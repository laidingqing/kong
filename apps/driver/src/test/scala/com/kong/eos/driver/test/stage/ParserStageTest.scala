
package com.kong.eos.driver.test.stage

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.input.Input
import com.kong.eos.sdk.pipeline.transformation.Parser
import com.kong.eos.sdk.properties.JsoneyString
import com.kong.eos.serving.core.models.policy.{OutputFieldsModel, PolicyModel, TransformationModel, TransformationsModel}
import com.kong.eos.serving.core.utils.ReflectionUtils
import com.kong.eos.driver.stage.{LogError, ParserStage}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.junit.runner.RunWith
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, ShouldMatchers}

@RunWith(classOf[JUnitRunner])
class ParserStageTest extends FlatSpec with ShouldMatchers with MockitoSugar {

  case class TestStage(policy: PolicyModel) extends ParserStage with LogError

  lazy val ReflectionUtils = new ReflectionUtils


  def mockPolicy: PolicyModel = {
    val policy = mock[PolicyModel]
    when(policy.storageLevel).thenReturn(Some("StorageLevel"))
    when(policy.id).thenReturn(Some("id"))
    policy
  }

  "ParserStage" should "Generate an empty seq with no transformations" in {
    val policy = mockPolicy
    val reflection = mock[ReflectionUtils]
    val transformations = mock[TransformationsModel]
    when(policy.transformations).thenReturn(Some(transformations))
    when(transformations.transformationsPipe).thenReturn(Seq.empty)
    when(transformations.writer).thenReturn(None)
    val (result, _) = TestStage(policy).parserStage(reflection, Map.empty)

    result should be(Seq.empty)
  }

  "ParserStage" should "Generate a single parser" in {
    val policy = mockPolicy
    val transformations = mock[TransformationsModel]
    when(policy.transformations).thenReturn(Some(transformations))
    when(transformations.writer).thenReturn(None)
    val outputFieldModel = OutputFieldsModel("output", Some("long"))
    val input = Some("input")
    val transformationSchema = Seq(outputFieldModel)
    val configuration: Map[String, JsoneyString] = Map("conf" -> JsoneyString("prop"))
    val transformation = TransformationModel("Test", 1, input, transformationSchema, configuration = configuration)
    val outputScheme = StructType(Seq.empty)
    when(transformations.transformationsPipe).thenReturn(Seq(transformation))

    val (result, _) = TestStage(policy).parserStage(ReflectionUtils, Map("1" -> outputScheme))

    result.size should be(1)
    val parser = result.head.asInstanceOf[TestParser]
    parser.getOrder should be(1)
    parser.inputField should be(input)
    parser.outputFields should be(List("output"))
    parser.schema should be(outputScheme)
    parser.properties should be(configuration)
  }

  "ParserStage" should "Generate a two parsers" in {
    val policy = mockPolicy
    val transformations = mock[TransformationsModel]
    when(policy.transformations).thenReturn(Some(transformations))
    when(transformations.writer).thenReturn(None)
    val outputFieldModel = OutputFieldsModel("output", Some("long"))
    val input = Some("input")
    val transformationSchema = Seq(outputFieldModel)
    val configuration: Map[String, JsoneyString] = Map("conf" -> JsoneyString("prop"))
    val transformation = TransformationModel("Test", 1, input, transformationSchema, configuration = configuration)
    val secondTransformation =
      TransformationModel("Test", 2, input, transformationSchema, configuration = configuration)
    val outputScheme = StructType(Seq.empty)
    when(transformations.transformationsPipe).thenReturn(Seq(transformation, secondTransformation))

    val (result, _) = TestStage(policy).parserStage(ReflectionUtils, Map("1" -> outputScheme, "2" -> outputScheme))

    result.size should be(2)
    val parser = result(1).asInstanceOf[TestParser]
    parser.getOrder should be(2)
    parser.inputField should be(input)
    parser.outputFields should be(List("output"))
    parser.schema should be(outputScheme)
    parser.properties should be(configuration)
  }

  "ParserStage" should "Fail when reflectionUtils don't behave correctly" in {
    val reflection = mock[ReflectionUtils]
    val policy = mockPolicy
    val transformations = mock[TransformationsModel]
    when(policy.transformations).thenReturn(Some(transformations))
    when(transformations.writer).thenReturn(None)
    val outputFieldModel = OutputFieldsModel("output", Some("long"))
    val input = Some("input")
    val transformationSchema = Seq(outputFieldModel)
    val configuration: Map[String, JsoneyString] = Map("conf" -> JsoneyString("prop"))
    val transformation = TransformationModel("Test", 1, input, transformationSchema, configuration = configuration)
    val outputScheme = StructType(Seq.empty)
    val myInputClass = mock[Input]
    when(reflection.tryToInstantiate(any(), any())).thenReturn(myInputClass)
    when(transformations.transformationsPipe).thenReturn(Seq(transformation))

    the[IllegalArgumentException] thrownBy {
      TestStage(policy).parserStage(reflection, Map("1" -> outputScheme))
    } should have message "Something gone wrong creating the parser: Test. Please re-check the policy."
  }

  "ParserStage" should "Fail gracefully with bad input" in {
    val reflection = mock[ReflectionUtils]
    val policy = mockPolicy
    val transformations = mock[TransformationsModel]
    when(policy.transformations).thenReturn(Some(transformations))
    when(transformations.writer).thenReturn(None)
    val outputFieldModel = OutputFieldsModel("output", Some("long"))
    val input = Some("input")
    val transformationSchema = Seq(outputFieldModel)
    val configuration: Map[String, JsoneyString] = Map("conf" -> JsoneyString("prop"))
    val transformation = TransformationModel("Test", 1, input, transformationSchema, configuration = configuration)
    val outputScheme = StructType(Seq.empty)
    when(reflection.tryToInstantiate(any(), any())).thenThrow(new RuntimeException("Fake"))
    when(transformations.transformationsPipe).thenReturn(Seq(transformation))

    the[IllegalArgumentException] thrownBy {
      TestStage(policy).parserStage(reflection, Map("1" -> outputScheme))
    } should have message "Something gone wrong creating the parser: Test. Please re-check the policy."
  }

  it should "parse a event" in {
    val parser: Parser = mock[Parser]
    val event: Row = mock[Row]
    val parsedEvent = Seq(mock[Row])
    when(parser.parse(event)).thenReturn(parsedEvent)

    val result = ParserStage.parseEvent(event, parser)
    result should be(parsedEvent)
  }
  it should "return none if a parse Event fails" in {
    val parser: Parser = mock[Parser]
    val event: Row = mock[Row]
    when(parser.parse(event)).thenThrow(new RuntimeException("testEx"))

    val result = ParserStage.parseEvent(event, parser)
    result should be(Seq.empty)
  }
}

class TestParser(val order: Integer,
                 val inputField: Option[String],
                 val outputFields: Seq[String],
                 val schema: StructType,
                 override val properties: Map[String, JSerializable])
  extends Parser(order, inputField, outputFields, schema, properties) {

  override def parse(data: Row): Seq[Row] = throw new RuntimeException("Fake implementation")
}