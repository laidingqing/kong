
package com.kong.eos.driver.utils

import com.kong.eos.serving.core.models.enumerators.PolicyStatusEnum._
import com.kong.eos.serving.core.models.policy.{PolicyModel, PolicyStatusModel}
import com.kong.eos.serving.core.utils.PolicyStatusUtils
import com.kong.eos.driver.factory.SparkContextFactory._
import org.apache.curator.framework.recipes.cache.NodeCache

import scala.util.{Failure, Success, Try}

trait LocalListenerUtils extends PolicyStatusUtils {

  def killLocalContextListener(policy: PolicyModel, name: String): Unit = {
    log.info(s"Listener added to ${policy.name} with id: ${policy.id.get}")
    addListener(policy.id.get, (policyStatus: PolicyStatusModel, nodeCache: NodeCache) => {
      synchronized {
        if (policyStatus.status == Stopping) {
          try {
            log.info("Stopping message received from Zookeeper")
            closeContexts(policy.id.get)
          } finally {
            Try(nodeCache.close()) match {
              case Success(_) =>
                log.info("Node cache closed correctly")
              case Failure(e) =>
                log.error(s"The nodeCache in Zookeeper is not closed correctly", e)
            }
          }
        }
      }
    })
  }

  private[driver] def closeContexts(policyId: String): Unit = {
    val information = "The Context have been stopped correctly in the local listener"
    log.info(information)
    updateStatus(PolicyStatusModel(id = policyId, status = Stopped, statusInfo = Some(information)))
    destroySparkContext()
  }
}
