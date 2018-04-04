
package com.kong.eos.driver.writer

import akka.event.slf4j.SLF4JLogging
import com.kong.eos.driver.schema.SchemaHelper
import com.kong.eos.sdk.pipeline.output.Output
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import scala.util.{Failure, Success, Try}

object WriterHelper extends SLF4JLogging {

  def write(dataFrame: DataFrame,
            writerOptions: WriterOptions,
            extraSaveOptions: Map[String, String],
            outputs: Seq[Output]): DataFrame = {
    val saveOptions = extraSaveOptions ++
      writerOptions.tableName.fold(Map.empty[String, String]) { outputTableName =>
        Map(Output.TableNameKey -> outputTableName)
      } ++
      writerOptions.partitionBy.fold(Map.empty[String, String]) { partition =>
        Map(Output.PartitionByKey -> partition)
      } ++
      writerOptions.primaryKey.fold(Map.empty[String, String]) { key =>
        Map(Output.PrimaryKey -> key)
      }
    val outputTableName = saveOptions.getOrElse(Output.TableNameKey, "")
    val outputTablePrimaryKey = saveOptions.getOrElse(Output.PrimaryKey, "")

    val autoCalculatedFieldsDf = DataFrameModifierHelper.applyAutoCalculateFields(dataFrame,
        writerOptions.autoCalculateFields,
        StructType(dataFrame.schema.fields ++ SchemaHelper.getStreamWriterPkFieldsMetadata(writerOptions.primaryKey)))

    autoCalculatedFieldsDf.printSchema()

    writerOptions.outputs.foreach(outputName =>
      outputs.find(output => output.name == outputName) match {
        case Some(outputWriter) => Try {
          outputWriter.save(autoCalculatedFieldsDf, writerOptions.saveMode, saveOptions)
        } match {
          case Success(_) =>
            log.debug(s"Data stored in $outputTableName")
          case Failure(e) =>
            log.error(s"Something goes wrong. Table: $outputTableName")
            log.error(s"Schema. ${autoCalculatedFieldsDf.schema}")
            log.error(s"Head element. ${autoCalculatedFieldsDf.head}")
            log.error(s"Error message : ${e.getMessage}")
        }
        case None => log.error(s"The output added : $outputName not match in the outputs")
      })
    autoCalculatedFieldsDf
  }

}
