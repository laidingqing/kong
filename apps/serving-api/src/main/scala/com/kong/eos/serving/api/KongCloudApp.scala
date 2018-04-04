
package com.kong.eos.serving.api

import akka.event.slf4j.SLF4JLogging
import com.kong.eos.serving.api.helpers.KongCloudHelper
import com.kong.eos.serving.core.config.KongCloudConfig
import com.kong.eos.serving.core.constants.AppConstant
import com.kong.eos.serving.api.helpers.KongCloudHelper

/**
 * Entry point of the application.
 */
object KongCloudApp extends App with SLF4JLogging {

  KongCloudConfig.initMainConfig()
  KongCloudConfig.initApiConfig()
  KongCloudHelper.initCloudAPI(AppConstant.ConfigAppName)
}
