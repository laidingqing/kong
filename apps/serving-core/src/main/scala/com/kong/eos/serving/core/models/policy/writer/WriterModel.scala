
package com.kong.eos.serving.core.models.policy.writer

import com.kong.eos.sdk.pipeline.output.SaveModeEnum

case class WriterModel(outputs: Seq[String] = Seq.empty[String],
                       dateType: Option[String] = None,
                       autoCalculatedFields: Seq[AutoCalculatedFieldModel] = Seq.empty[AutoCalculatedFieldModel],
                       saveMode: SaveModeEnum.Value = SaveModeEnum.Append,
                       tableName: Option[String] = None,
                       partitionBy: Option[String] = None,
                       primaryKey: Option[String] = None)
