
package com.kong.eos.sdk.pipeline.aggregation.cube

import org.apache.spark.sql.Row

case class InputFields(fieldsValues: Row, newValues: Int)
