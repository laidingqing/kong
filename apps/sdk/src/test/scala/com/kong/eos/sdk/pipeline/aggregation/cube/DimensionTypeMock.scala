/*
 * Copyright (C) 2015 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kong.eos.sdk.pipeline.aggregation.cube

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.schema.TypeOp

class DimensionTypeMock(prop: Map[String, JSerializable]) extends DimensionType {

  override val operationProps: Map[String, JSerializable] = prop

  override val properties: Map[String, JSerializable] = prop

  override val defaultTypeOperation = TypeOp.String

  override def precisionValue(keyName: String, value: Any): (Precision, Any) = {
    val precision = DimensionType.getIdentity(getTypeOperation, defaultTypeOperation)
    (precision, TypeOp.transformValueByTypeOp(precision.typeOp, value))
  }

  override def precision(keyName: String): Precision =
    DimensionType.getIdentity(getTypeOperation, defaultTypeOperation)
}
