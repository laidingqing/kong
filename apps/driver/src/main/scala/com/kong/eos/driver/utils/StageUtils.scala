
package com.kong.eos.driver.utils

import com.kong.eos.sdk.pipeline.autoCalculations
import com.kong.eos.sdk.pipeline.autoCalculations._
import com.kong.eos.serving.core.models.policy.writer.AutoCalculatedFieldModel


trait StageUtils {

  private[driver] def getAutoCalculatedFields(autoCalculatedFields: Seq[AutoCalculatedFieldModel])
  : Seq[AutoCalculatedField] =
    autoCalculatedFields.map(model =>
      autoCalculations.AutoCalculatedField(
        model.fromNotNullFields.map(fromNotNullFieldsModel =>
          FromNotNullFields(Field(fromNotNullFieldsModel.field.name, fromNotNullFieldsModel.field.outputType))),
        model.fromPkFields.map(fromPkFieldsModel =>
          FromPkFields(Field(fromPkFieldsModel.field.name, fromPkFieldsModel.field.outputType))),
        model.fromFields.map(fromFieldModel =>
          FromFields(Field(fromFieldModel.field.name, fromFieldModel.field.outputType), fromFieldModel.fromFields)),
        model.fromFixedValue.map(fromFixedValueModel =>
          FromFixedValue(Field(fromFixedValueModel.field.name, fromFixedValueModel.field.outputType),
            fromFixedValueModel.value))
      )
    )
}
