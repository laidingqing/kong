
package com.kong.eos.driver.writer

import java.sql.{Date, Timestamp}

import akka.event.slf4j.SLF4JLogging
import com.kong.eos.sdk.pipeline.aggregation.cube.{DimensionValue, DimensionValuesTime, MeasuresValues}
import com.kong.eos.sdk.pipeline.output.Output
import com.kong.eos.sdk.pipeline.schema.TypeOp
import com.kong.eos.driver.factory.SparkContextFactory
import com.kong.eos.driver.step.Cube
import org.apache.spark.sql._
import org.apache.spark.streaming.dstream.DStream

object CubeWriterHelper extends SLF4JLogging {

  def writeCube(cube: Cube, outputs: Seq[Output], stream: DStream[(DimensionValuesTime, MeasuresValues)]): Unit = {
    stream.map { case (dimensionValuesTime, measuresValues) =>
      toRow(cube, dimensionValuesTime, measuresValues)
    }.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val sparkSession = SparkContextFactory.sparkSessionInstance
        val cubeDf = sparkSession.createDataFrame(rdd, cube.schema)
        val extraOptions = Map(Output.TableNameKey -> cube.name)
        val cubeAutoCalculatedFieldsDf = WriterHelper.write(cubeDf, cube.writerOptions, extraOptions, outputs)

        TriggerWriterHelper.writeTriggers(cubeAutoCalculatedFieldsDf, cube.triggers, cube.name, outputs)
      } else log.debug("Empty event received")
    })
  }

  private[driver] def toRow(cube: Cube, dimensionValuesT: DimensionValuesTime, measures: MeasuresValues): Row = {
    val measuresSorted = measuresValuesSorted(measures.values)
    val rowValues = dimensionValuesT.timeConfig match {
      case None =>
        val dimensionValues = dimensionsValuesSorted(dimensionValuesT.dimensionValues)

        dimensionValues ++ measuresSorted
      case Some(timeConfig) =>
        val timeValue = Seq(timeFromDateType(timeConfig.eventTime, cube.dateType))
        val dimFilteredByTime = filterDimensionsByTime(dimensionValuesT.dimensionValues, timeConfig.timeDimension)
        val dimensionValues = dimensionsValuesSorted(dimFilteredByTime) ++ timeValue
        val measuresValuesWithTime = measuresSorted

        dimensionValues ++ measuresValuesWithTime
    }

    Row.fromSeq(rowValues)
  }

  private[driver] def dimensionsValuesSorted(dimensionValues: Seq[DimensionValue]): Seq[Any] =
    dimensionValues.sorted.map(dimVal => dimVal.value)

  private[driver] def measuresValuesSorted(measures: Map[String, Option[Any]]): Seq[Any] =
    measures.toSeq.sortWith(_._1 < _._1).map(measure => measure._2.getOrElse(null))

  private[driver] def filterDimensionsByTime(dimensionValues: Seq[DimensionValue],
                                             timeDimension: String): Seq[DimensionValue] =
    dimensionValues.filter(dimensionValue => dimensionValue.dimension.name != timeDimension)

  private[driver] def timeFromDateType(time: Long, dateType: TypeOp.Value): Any = {
    dateType match {
      case TypeOp.Date | TypeOp.DateTime => new Date(time)
      case TypeOp.Long => time
      case TypeOp.Timestamp => new Timestamp(time)
      case _ => time.toString
    }
  }
}