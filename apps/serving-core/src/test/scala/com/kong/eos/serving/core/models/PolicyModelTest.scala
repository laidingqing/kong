
package com.kong.eos.serving.core.models

import com.kong.eos.sdk.pipeline.aggregation.cube.DimensionType
import com.kong.eos.sdk.pipeline.input.Input
import com.kong.eos.sdk.properties.JsoneyString
import com.kong.eos.serving.core
import com.kong.eos.serving.core.exception.ServingCoreException
import com.kong.eos.serving.core.models.policy._
import com.kong.eos.serving.core.models.policy.cube.{CubeModel, DimensionModel, OperatorModel}
import com.kong.eos.serving.core.models.policy.fragment.{FragmentElementModel, FragmentType}
import com.kong.eos.serving.core.models.policy.writer.WriterModel
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

@RunWith(classOf[JUnitRunner])
class PolicyModelTest extends WordSpec with Matchers with MockitoSugar {

  val rawData = None
  val fragmentModel = new FragmentElementModel(
    Some("id"),
    "",
    "fragmentType",
    "name",
    "description",
    "shortDescription",
    PolicyElementModel("name", "type", Map()))

  val fragmentType = FragmentType

  val transformations = Option(TransformationsModel(Seq(TransformationModel("Morphlines",
    0,
    Some(Input.RawDataKey),
    Seq(OutputFieldsModel("out1"), OutputFieldsModel("out2")),
   Map("removeInputField" -> JsoneyString.apply("true")
  )))))

  val dimensionModel = Seq(
    DimensionModel(
      "dimensionName",
      "field1",
      DimensionType.IdentityName,
      DimensionType.DefaultDimensionClass,
      configuration = Some(Map())),
    DimensionModel(
      "dimensionName2",
      "field2",
      "minute",
      "DateTime",
      Option("61s"),
      configuration = Some(Map())))

  val operators = Seq(OperatorModel("Count", "countoperator", Map()
  ))

  val wrongDimensionModel = Seq(
    DimensionModel(
      "dimensionName2",
      "field2",
      "9s",
      "DateTime",
      Option("minute"),
      configuration = Some(Map())))

  val wrongComputeLastModel = Seq(
    DimensionModel(
      "dimensionName2",
      "field2",
      "minute",
      "DateTime",
      Option("59s"),
      configuration = Some(Map())))

  val cubeWriter = WriterModel(outputs = Seq("mongo"))
  val cubes = Seq(core.models.policy.cube.CubeModel("cube1", dimensionModel, operators: Seq[OperatorModel], cubeWriter))

  val outputs = Seq(PolicyElementModel("mongo", "MongoDb", Map()))
  val input = Some(PolicyElementModel("kafka", "Kafka", Map()))
  val policy = PolicyModel(id = None,
    storageLevel = PolicyModel.storageDefaultValue,
    name = "testpolicy",
    description = "whatever",
    sparkStreamingWindow = PolicyModel.sparkStreamingWindow,
    checkpointPath = Option("test/test"),
    rawData,
    transformations,
    streamTriggers = Seq(),
    cubes,
    input,
    outputs,
    fragments = Seq(),
    userPluginsJars = Seq(),
    remember = None,
    sparkConf = Seq(),
    initSqlSentences = Seq(),
    autoDeleteCheckpoint = None
  )

  "AggregationPoliciesValidator" should {
    "not throw an exception if the policy is well formed" in {
      PolicyValidator.validateDto(policy)
    }
  }

  val wrongPrecisionCubes = Seq(CubeModel("cube1", wrongDimensionModel, operators: Seq[OperatorModel], cubeWriter))
  val wrongComputeLastCubes = Seq(core.models.policy.cube.CubeModel("cube1", wrongComputeLastModel, operators: Seq[OperatorModel], cubeWriter))

  "AggregationPoliciesValidator" when {
    "there is a cube without name" should {
      "throw an exception because the policy is not well formed" in {
        val wrongCubeWithNoName = Seq(core.models.policy.cube.CubeModel("", wrongComputeLastModel, operators: Seq[OperatorModel], cubeWriter))
        val wrongCubeNamePolicy = policy.copy(cubes = wrongCubeWithNoName)
        val exceptionThrown = the[ServingCoreException] thrownBy {
          PolicyValidator.validateDto(wrongCubeNamePolicy)
        }
        val awfullyWrongErrorModel= ErrorModel.toErrorModel(exceptionThrown.getMessage)
        awfullyWrongErrorModel.i18nCode shouldBe ErrorModel.ValidationError
        awfullyWrongErrorModel.subErrorModels shouldBe defined
        val seqErrors= awfullyWrongErrorModel.subErrorModels.get
        seqErrors.exists(_.i18nCode == ErrorModel.ValidationError_There_is_at_least_one_cube_without_name) shouldBe true
      }
    }
  }

  "AggregationPoliciesValidator" when {
    "there is neither a cube, nor a trigger nor raw-data nor transformation" should {
      "throw an exception because the policy is not well formed" in {
        val emptyPolicy= policy.copy(transformations = Option(TransformationsModel(Seq(), None)), cubes = Seq())
        val exceptionThrown = the[ServingCoreException] thrownBy {
          PolicyValidator.validateDto(emptyPolicy)
        }
        val awfullyWrongErrorModel= ErrorModel.toErrorModel(exceptionThrown.getMessage)
        awfullyWrongErrorModel.i18nCode shouldBe ErrorModel.ValidationError
        awfullyWrongErrorModel.subErrorModels shouldBe defined
        val seqErrors= awfullyWrongErrorModel.subErrorModels.get
        seqErrors.exists(_.i18nCode ==
          ErrorModel.
            ValidationError_The_policy_needs_at_least_one_cube_or_one_trigger_or_raw_data_or_transformations_with_save
        ) shouldBe true
      }
    }
  }

  "AggregationPoliciesValidator" when {
    "a RawDataModel has no data and time field and no table name" should {
      "throw exceptions because is not correctly filled up " in {
        val exceptionThrown = the[ServingCoreException] thrownBy {
          val rawDataModel= RawDataModel("","", writer= WriterModel(Seq("output1"),
            tableName = Option("FakeTable")), Map.empty)
          val policyWrongRawData = policy.copy(rawData=Option(rawDataModel),
            outputs = Seq(PolicyElementModel("output1","output2")))
          PolicyValidator.validateDto(policyWrongRawData)
        }
        val awfullyWrongErrorModel= ErrorModel.toErrorModel(exceptionThrown.getMessage)
        awfullyWrongErrorModel.i18nCode shouldBe ErrorModel.ValidationError
        awfullyWrongErrorModel.subErrorModels shouldBe defined
        val seqErrors= awfullyWrongErrorModel.subErrorModels.get
        seqErrors.exists(_.i18nCode == ErrorModel.ValidationError_Raw_data_with_a_bad_output) shouldBe false
        seqErrors.exists(_.i18nCode == ErrorModel.ValidationError_Raw_data_with_bad_table_name) shouldBe false
        seqErrors.exists(_.i18nCode == ErrorModel.ValidationError_Raw_data_with_bad_data_field) shouldBe true
        seqErrors.exists(_.i18nCode == ErrorModel.ValidationError_Raw_data_with_bad_time_field) shouldBe true
      }
    }
  }

}

