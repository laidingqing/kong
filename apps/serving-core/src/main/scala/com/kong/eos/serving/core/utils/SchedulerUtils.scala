
package com.kong.eos.serving.core.utils

import akka.actor.Cancellable
import akka.event.slf4j.SLF4JLogging
import com.kong.eos.sdk.utils.AggregationTime
import com.kong.eos.serving.core.config.KongCloudConfig
import com.kong.eos.serving.core.constants.AppConstant._
import scala.concurrent.duration._


import scala.util.Try

trait SchedulerUtils extends SLF4JLogging {
  import scala.concurrent.ExecutionContext.Implicits.global
  def scheduleOneTask(timeProperty: String, defaultTime: String)(f: ⇒ Unit): Cancellable = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val delay = Try(KongCloudConfig.getDetailConfig.get.getString(timeProperty)).toOption
      .flatMap(x => if (x == "") None else Some(x)).getOrElse(defaultTime)

    log.info(s"Starting scheduler task in $timeProperty with time: $delay")
    SchedulerSystem.scheduler.scheduleOnce(AggregationTime.parseValueToMilliSeconds(delay) milli)(f)
  }

  def scheduleTask(initTimeProperty: String,
                   defaultInitTime: String,
                   intervalTimeProperty: String,
                   defaultIntervalTime: String
                  )(f: ⇒ Unit): Cancellable = {
    val initialDelay = Try(KongCloudConfig.getDetailConfig.get.getString(initTimeProperty)).toOption
      .flatMap(x => if (x == "") None else Some(x)).getOrElse(defaultInitTime)

    val interval = Try(KongCloudConfig.getDetailConfig.get.getString(intervalTimeProperty)).toOption
      .flatMap(x => if (x == "") None else Some(x)).getOrElse(defaultIntervalTime)

    log.info(s"Starting scheduler tasks with delay $initTimeProperty with time: $initialDelay and interval " +
      s"$intervalTimeProperty with time: $interval")

    SchedulerSystem.scheduler.schedule(
      AggregationTime.parseValueToMilliSeconds(initialDelay) milli,
      AggregationTime.parseValueToMilliSeconds(interval) milli)(f)
  }
}
