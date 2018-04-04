
package com.kong.eos.driver.test.stage

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.kong.eos.sdk.properties.JsoneyString
import com.kong.eos.serving.core.models.policy.writer.{AutoCalculatedFieldModel, WriterModel}
import com.kong.eos.serving.core.models.policy.{PolicyModel, RawDataModel}
import com.kong.eos.driver.stage.{LogError, RawDataStage}
import org.junit.runner.RunWith
import org.mockito.Mockito.when
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpecLike, ShouldMatchers}

@RunWith(classOf[JUnitRunner])
class RawStageTest
  extends TestKit(ActorSystem("RawStageTest"))
    with FlatSpecLike with ShouldMatchers with MockitoSugar {

  case class TestRawData(policy: PolicyModel) extends RawDataStage with LogError

  def mockPolicy: PolicyModel = {
    val policy = mock[PolicyModel]
    when(policy.id).thenReturn(Some("id"))
    policy
  }

  "rawDataStage" should "Generate a raw data" in {
    val field = "field"
    val timeField = "time"
    val tableName = Some("table")
    val outputs = Seq("output")
    val partitionBy = Some("field")
    val autocalculateFields = Seq(AutoCalculatedFieldModel())
    val configuration = Map.empty[String, JsoneyString]

    val policy = mockPolicy
    val rawData = mock[RawDataModel]
    val writerModel = mock[WriterModel]

    when(policy.rawData).thenReturn(Some(rawData))
    when(rawData.dataField).thenReturn(field)
    when(rawData.timeField).thenReturn(timeField)
    when(rawData.writer).thenReturn(writerModel)
    when(writerModel.tableName).thenReturn(tableName)
    when(writerModel.outputs).thenReturn(outputs)
    when(writerModel.partitionBy).thenReturn(partitionBy)
    when(writerModel.autoCalculatedFields).thenReturn(autocalculateFields)
    when(rawData.configuration).thenReturn(configuration)

    val result = TestRawData(policy).rawDataStage()

    result.timeField should be(timeField)
    result.dataField should be(field)
    result.writerOptions.tableName should be(tableName)
    result.writerOptions.partitionBy should be(partitionBy)
    result.configuration should be(configuration)
    result.writerOptions.outputs should be(outputs)
  }

  "rawDataStage" should "Fail with bad table name" in {
    val field = "field"
    val timeField = "time"
    val tableName = None
    val outputs = Seq("output")
    val partitionBy = Some("field")
    val configuration = Map.empty[String, JsoneyString]

    val policy = mockPolicy
    val rawData = mock[RawDataModel]
    val writerModel = mock[WriterModel]

    when(policy.rawData).thenReturn(Some(rawData))
    when(rawData.dataField).thenReturn(field)
    when(rawData.timeField).thenReturn(timeField)
    when(rawData.writer).thenReturn(writerModel)
    when(writerModel.tableName).thenReturn(tableName)
    when(writerModel.outputs).thenReturn(outputs)
    when(writerModel.partitionBy).thenReturn(partitionBy)
    when(rawData.configuration).thenReturn(configuration)


    the[IllegalArgumentException] thrownBy {
      TestRawData(policy).rawDataStage()
    } should have message "Something gone wrong saving the raw data. Please re-check the policy."
  }

}
