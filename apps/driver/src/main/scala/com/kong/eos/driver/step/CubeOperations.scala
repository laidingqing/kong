
package com.kong.eos.driver.step

import akka.event.slf4j.SLF4JLogging
import com.kong.eos.sdk.pipeline.aggregation.cube.{DimensionValue, DimensionValuesTime, InputFields, TimeConfig}
import com.kong.eos.sdk.pipeline.schema.TypeOp
import com.kong.eos.sdk.utils.AggregationTime
import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.DStream
import org.joda.time.DateTime

import scala.util.{Failure, Success, Try}

/**
 * This class is necessary because we need test extractDimensionsAggregations with Spark testSuite for Dstreams.
 *
 * @param cube that will be contain the current cube.
 */

case class CubeOperations(cube: Cube) extends SLF4JLogging {

  private final val UpdatedValues = 1

  /**
   * Extract a modified stream that will be needed to calculate aggregations.
   *
   * @param inputStream with the original stream of data.
   * @return a modified stream after join dimensions, cubes and operations.
   */
  def extractDimensionsAggregations(inputStream: DStream[Row]): DStream[(DimensionValuesTime, InputFields)] = {
    inputStream.mapPartitions(rows => rows.flatMap(row => Try {
      val dimensionValues = for {
        dimension <- cube.dimensions
        value = row.get(cube.initSchema.fieldIndex(dimension.field))
        (precision, dimValue) = dimension.dimensionType.precisionValue(dimension.precisionKey, value)
      } yield DimensionValue(dimension, TypeOp.transformValueByTypeOp(precision.typeOp, dimValue))

      cube.expiringDataConfig match {
        case None =>
          (DimensionValuesTime(cube.name, dimensionValues), InputFields(row, UpdatedValues))
        case Some(expiringDataConfig) =>
          val eventTime = extractEventTime(dimensionValues)
          val timeDimension = expiringDataConfig.timeDimension
          (DimensionValuesTime(cube.name, dimensionValues, Option(TimeConfig(eventTime, timeDimension))),
            InputFields(row, UpdatedValues))
      }
    } match {
      case Success(dimensionValuesTime) =>
        Some(dimensionValuesTime)
      case Failure(exception) =>
        val error = s"Failure[Aggregations]: ${row.toString} | ${exception.getLocalizedMessage}"
        log.error(error, exception)
        None
    }), true)
  }

  private[driver] def extractEventTime(dimensionValues: Seq[DimensionValue]) = {

    val timeDimension = cube.expiringDataConfig.get.timeDimension
    val dimensionsDates =
      dimensionValues.filter(dimensionValue => dimensionValue.dimension.name == timeDimension)

    if (dimensionsDates.isEmpty) getDate
    else AggregationTime.getMillisFromSerializable(dimensionsDates.head.value)
  }

  private[driver] def getDate: Long = {
    val checkpointGranularity = cube.expiringDataConfig.get.granularity

    AggregationTime.truncateDate(DateTime.now(), checkpointGranularity)
  }
}
