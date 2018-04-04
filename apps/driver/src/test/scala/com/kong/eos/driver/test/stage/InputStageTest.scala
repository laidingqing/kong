
package com.kong.eos.driver.test.stage

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.kong.eos.sdk.pipeline.input.Input
import com.kong.eos.sdk.pipeline.output.Output
import com.kong.eos.sdk.properties.JsoneyString
import com.kong.eos.serving.core.models.policy.{PolicyElementModel, PolicyModel}
import com.kong.eos.serving.core.utils.ReflectionUtils
import com.kong.eos.driver.stage.{InputStage, LogError, ZooKeeperError}
import org.apache.curator.framework.CuratorFramework
import org.apache.spark.sql.Row
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.junit.runner.RunWith
import org.mockito.Matchers.{any, eq => mockEq}
import org.mockito.Mockito.{when, _}
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpecLike, ShouldMatchers}

@RunWith(classOf[JUnitRunner])
class InputStageTest extends TestKit(ActorSystem("InputStageTest"))
    with FlatSpecLike with ShouldMatchers with MockitoSugar {

  case class TestInput(policy: PolicyModel) extends InputStage with LogError

  case class TestInputZK(policy: PolicyModel, curatorFramework: CuratorFramework) extends InputStage with ZooKeeperError

  def mockPolicy: PolicyModel = {
    val policy = mock[PolicyModel]
    when(policy.storageLevel).thenReturn(Some("StorageLevel"))
    when(policy.id).thenReturn(Some("id"))
    policy
  }

  "inputStage" should "Generate a input" in {
    val policy = mockPolicy
    val input = mock[PolicyElementModel]
    val ssc = mock[StreamingContext]
    val reflection = mock[ReflectionUtils]
    val myInputClass = mock[Input]
    when(policy.input).thenReturn(Some(input))
    when(input.name).thenReturn("input")
    when(input.`type`).thenReturn("Input")
    when(input.configuration).thenReturn(Map.empty[String, JsoneyString])
    when(reflection.tryToInstantiate(mockEq("InputInput"), any())).thenReturn(myInputClass)

    val result = TestInput(policy).createInput(ssc, reflection)

    verify(reflection).tryToInstantiate(mockEq("InputInput"), any())
    result should be(myInputClass)
  }

  "inputStage" should "Fail gracefully with bad input" in {
    val policy = mockPolicy
    val input = mock[PolicyElementModel]
    val ssc = mock[StreamingContext]
    val reflection = mock[ReflectionUtils]
    when(policy.input).thenReturn(Some(input))
    when(input.name).thenReturn("input")
    when(input.`type`).thenReturn("Input")
    when(reflection.tryToInstantiate(mockEq("InputInput"), any())).thenThrow(new RuntimeException("Fake"))

    the[IllegalArgumentException] thrownBy {
      TestInput(policy).createInput(ssc, reflection)
    } should have message "Something gone wrong creating the input: input. Please re-check the policy."
  }

  "inputStage" should "Fail when reflectionUtils don't behave correctly" in {
    val policy = mockPolicy
    val input = mock[PolicyElementModel]
    val ssc = mock[StreamingContext]
    val reflection = mock[ReflectionUtils]
    val output = mock[Output]

    when(policy.input).thenReturn(Some(input))
    when(input.name).thenReturn("input")
    when(input.`type`).thenReturn("Input")
    when(reflection.tryToInstantiate(mockEq("InputInput"), any())).thenReturn(output)

    the[IllegalArgumentException] thrownBy {
      TestInput(policy).createInput(ssc, reflection)
    } should have message "Something gone wrong creating the input: input. Please re-check the policy."


  }

  "inputStreamStage" should "Generate a inputStream" in {
    val policy = mockPolicy
    val input = mock[PolicyElementModel]
    val ssc = mock[StreamingContext]
    val inputClass = mock[Input]
    val row = mock[DStream[Row]]
    val reflection = mock[ReflectionUtils]
    when(policy.input).thenReturn(Some(input))
    when(input.name).thenReturn("input")
    when(input.`type`).thenReturn("Input")
    when(input.configuration).thenReturn(Map.empty[String, JsoneyString])
    when(reflection.tryToInstantiate(mockEq("InputInput"), any())).thenReturn(inputClass)
    when(inputClass.initStream(ssc, policy.storageLevel.get)).thenReturn(row)

    val result = TestInput(policy).inputStreamStage(ssc, inputClass)

    verify(inputClass).initStream(ssc, "StorageLevel")
    result should be(row)
  }

  "inputStreamStage" should "Fail gracefully with bad input" in {
    val policy = mockPolicy
    val input = mock[PolicyElementModel]
    val ssc = mock[StreamingContext]
    val inputClass = mock[Input]
    val reflection = mock[ReflectionUtils]
    when(policy.input).thenReturn(Some(input))
    when(input.name).thenReturn("input")
    when(input.`type`).thenReturn("Input")
    when(input.configuration).thenReturn(Map.empty[String, JsoneyString])
    when(reflection.tryToInstantiate(mockEq("InputInput"), any())).thenReturn(inputClass)
    when(inputClass.initStream(ssc, policy.storageLevel.get)).thenThrow(new RuntimeException("Fake"))

    the[IllegalArgumentException] thrownBy {
      TestInput(policy).inputStreamStage(ssc, inputClass)
    } should have message "Something gone wrong creating the input stream for: input."

    verify(inputClass).initStream(ssc, "StorageLevel")

  }

}
