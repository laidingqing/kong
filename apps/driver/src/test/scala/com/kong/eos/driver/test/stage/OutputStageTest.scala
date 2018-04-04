
package com.kong.eos.driver.test.stage

import com.kong.eos.sdk.pipeline.input.Input
import com.kong.eos.sdk.pipeline.output.Output
import com.kong.eos.serving.core.models.policy.{PolicyElementModel, PolicyModel}
import com.kong.eos.serving.core.utils.ReflectionUtils
import com.kong.eos.driver.stage.{LogError, OutputStage}
import org.junit.runner.RunWith
import org.mockito.Matchers.{any, eq => mockEq}
import org.mockito.Mockito.{when, _}
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, ShouldMatchers}

@RunWith(classOf[JUnitRunner])
class OutputStageTest extends FlatSpec with ShouldMatchers with MockitoSugar {

  case class TestStage(policy: PolicyModel) extends OutputStage with LogError

  def mockPolicy: PolicyModel = {
    val policy = mock[PolicyModel]
    when(policy.storageLevel).thenReturn(Some("StorageLevel"))
    when(policy.id).thenReturn(Some("id"))
    policy
  }

  "OutputStage" should "Generate an empty list with no policies" in {
    val policy = mockPolicy
    val reflection = mock[ReflectionUtils]
    when(policy.outputs).thenReturn(Seq.empty)

    val result = TestStage(policy).outputStage(reflection)

    result should be(List.empty)
  }

  "OutputStage" should "Generate an output " in {
    val policy = mockPolicy
    val reflection = mock[ReflectionUtils]
    val outputs = Seq(PolicyElementModel("output", "Output", Map.empty))
    val outputClass = mock[Output]
    when(policy.outputs).thenReturn(outputs)
    when(reflection.tryToInstantiate(mockEq("OutputOutput"), any())).thenReturn(outputClass)

    val result = TestStage(policy).outputStage(reflection)
    verify(reflection).tryToInstantiate(mockEq("OutputOutput"), any())
    result should be(List(outputClass))
  }

  "OutputStage" should "Fail gracefully with bad input" in {
    val policy = mockPolicy
    val reflection = mock[ReflectionUtils]
    val outputs = Seq(PolicyElementModel("output", "Output", Map.empty))
    when(policy.outputs).thenReturn(outputs)
    when(reflection.tryToInstantiate(any(), any())).thenThrow(new RuntimeException("Fake"))

    the[IllegalArgumentException] thrownBy {
      TestStage(policy).outputStage(reflection)
    } should have message "Something gone wrong creating the output: output. Please re-check the policy."
  }


  "OutputStage" should "Fail when reflectionUtils don't behave correctly" in {
    val policy = mockPolicy
    val reflection = mock[ReflectionUtils]
    val outputs = Seq(PolicyElementModel("output", "Output", Map.empty))
    val myInputClass = mock[Input]
    when(policy.outputs).thenReturn(outputs)
    when(reflection.tryToInstantiate(any(), any())).thenReturn(myInputClass)

    the[IllegalArgumentException] thrownBy {
      TestStage(policy).outputStage(reflection)
    } should have message "Something gone wrong creating the output: output. Please re-check the policy."
  }

  "OutputStage" should "Generate a list of output for multiple Outputs " in {
    val policy = mockPolicy
    val reflection = mock[ReflectionUtils]
    val outputs = Seq(
      PolicyElementModel("output", "Output", Map.empty),
      PolicyElementModel("output", "OtherOutput", Map.empty)
    )
    val outputClass = mock[Output]
    when(policy.outputs).thenReturn(outputs)
    when(reflection.tryToInstantiate(mockEq("OutputOutput"), any())).thenReturn(outputClass)
    when(reflection.tryToInstantiate(mockEq("OtherOutputOutput"), any())).thenReturn(outputClass)

    val result = TestStage(policy).outputStage(reflection)
    verify(reflection).tryToInstantiate(mockEq("OutputOutput"), any())
    verify(reflection).tryToInstantiate(mockEq("OtherOutputOutput"), any())
    result should be(List(outputClass, outputClass))
  }

  "OutputStage" should "Filter outputs " in {
    val policy = mockPolicy
    val reflection = mock[ReflectionUtils]
    val firstOutput = PolicyElementModel("output", "Output", Map.empty)
    val secondOutput = PolicyElementModel("outputOne", "OtherOutput", Map.empty)
    val outputs = Seq(firstOutput, secondOutput)
    val outputClass = mock[Output]
    when(policy.outputs).thenReturn(outputs)
    when(reflection.tryToInstantiate(mockEq("OutputOutput"), any())).thenReturn(outputClass)
    when(reflection.tryToInstantiate(mockEq("OtherOutputOutput"), any())).thenReturn(outputClass)

    val spyResult = spy(TestStage(policy))
    val result = spyResult.outputStage(reflection)

    verify(reflection).tryToInstantiate(mockEq("OutputOutput"), any())
    verify(reflection).tryToInstantiate(mockEq("OtherOutputOutput"), any())
    verify(spyResult).createOutput(firstOutput, reflection)
    verify(spyResult).createOutput(secondOutput, reflection)
    result should be(List(outputClass, outputClass))
  }


}
