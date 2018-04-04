
package com.kong.eos.driver.writer

import com.kong.eos.sdk.pipeline.autoCalculations.AutoCalculatedField
import com.kong.eos.sdk.pipeline.output.SaveModeEnum


case class WriterOptions(outputs: Seq[String] = Seq.empty[String],
                         saveMode: SaveModeEnum.Value = SaveModeEnum.Append,
                         tableName: Option[String] = None,
                         autoCalculateFields: Seq[AutoCalculatedField] = Seq.empty[AutoCalculatedField],
                         partitionBy: Option[String] = None,
                         primaryKey: Option[String] = None)
