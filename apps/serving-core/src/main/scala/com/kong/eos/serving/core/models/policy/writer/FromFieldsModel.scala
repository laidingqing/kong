
package com.kong.eos.serving.core.models.policy.writer

import com.kong.eos.serving.core.models.policy.cube.FieldModel

case class FromFieldsModel(field : FieldModel, fromFields: Seq[String])

