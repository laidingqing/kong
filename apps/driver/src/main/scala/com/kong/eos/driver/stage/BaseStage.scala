
package com.kong.eos.driver.stage

import akka.event.slf4j.SLF4JLogging
import com.kong.eos.serving.core.models.enumerators.PolicyStatusEnum.NotDefined
import com.kong.eos.serving.core.models.policy.{PhaseEnum, PolicyErrorModel, PolicyModel, PolicyStatusModel}
import com.kong.eos.serving.core.utils.PolicyStatusUtils
import com.kong.eos.driver.utils.StageUtils
import org.apache.curator.framework.CuratorFramework

import scala.util.{Failure, Success, Try}


trait ErrorPersistor {
  def persistError(error: PolicyErrorModel): Unit
}

trait ZooKeeperError extends ErrorPersistor with PolicyStatusUtils {

  def policy: PolicyModel

  def persistError(error: PolicyErrorModel): Unit =
    updateStatus(PolicyStatusModel(policy.id.get, NotDefined, None, None, lastError = Some(error)))

  def clearError(): Unit =
    clearLastError(policy.id.get)
}

trait LogError extends ErrorPersistor with SLF4JLogging {
  def persistError(error: PolicyErrorModel): Unit = log.error(s"This error was not saved to ZK : $error")
}

trait BaseStage extends SLF4JLogging with StageUtils {
  this: ErrorPersistor =>
  def policy: PolicyModel

  def generalTransformation[T](code: PhaseEnum.Value, okMessage: String, errorMessage: String)
                              (f: => T): T = {
    Try(f) match {
      case Success(result) =>
        log.info(okMessage)
        result
      case Failure(ex) => throw logAndCreateEx(code, ex, policy, errorMessage)
    }
  }

  def logAndCreateEx(code: PhaseEnum.Value,
                      ex: Throwable,
                      policy: PolicyModel,
                      message: String
                    ): IllegalArgumentException = {
    val originalMsg = ex.getCause match {
      case _: ClassNotFoundException => "The component couldn't be found in classpath. Please check the type."
      case exception: Throwable => exception.toString
      case _ => ex.toString
    }
    val policyError = PolicyErrorModel(message, code, originalMsg)
    log.error("An error was detected : {}", policyError)
    Try {
      persistError(policyError)
    } recover {
      case e => log.error(s"Error while persisting error: $policyError", e)
    }
    new IllegalArgumentException(message, ex)
  }

}
