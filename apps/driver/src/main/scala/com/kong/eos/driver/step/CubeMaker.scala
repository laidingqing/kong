
package com.kong.eos.driver.step

import com.kong.eos.sdk.pipeline.aggregation.cube.{DimensionValuesTime, MeasuresValues}
import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.DStream

/**
 * It builds a pre-calculated DataCube with dimension/s, cube/s and operation/s defined by the user in the policy.
 * Steps:
 * From a event stream it builds a Seq[(Seq[DimensionValue],Map[String, JSerializable])] with all needed data.
 * For each cube it calculates aggregations taking the stream calculated in the previous step.
 * Finally, it returns a modified stream with pre-calculated data encapsulated in a UpdateMetricOperation.
 * This final stream will be used mainly by outputs.
 * @param cubes that will be contain how the data will be aggregate.
 */
case class CubeMaker(cubes: Seq[Cube]) {

  /**
   * It builds the DataCube calculating aggregations.
   * @param inputStream with the original stream of data.
   * @return the built Cube.
   */
  def setUp(inputStream: DStream[Row]): Seq[(String, DStream[(DimensionValuesTime, MeasuresValues)])] = {
    cubes.map(cube => {
      val currentCube = CubeOperations(cube)
      val extractedDimensionsStream = currentCube.extractDimensionsAggregations(inputStream)
      (cube.name, cube.aggregate(extractedDimensionsStream))
    })
  }
}
