
package com.kong.eos.serving.core.models.policy.writer

case class AutoCalculatedFieldModel(
                                     fromNotNullFields: Option[FromNotNullFieldsModel] = None,
                                     fromPkFields: Option[FromPkFieldsModel] = None,
                                     fromFields: Option[FromFieldsModel] = None,
                                     fromFixedValue: Option[FromFixedValueModel] = None
                                  )
