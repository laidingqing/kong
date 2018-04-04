package com.kong.eos.serving.core.models.policy

import com.kong.eos.serving.core.models.policy.writer.WriterModel

case class TransformationsModel(transformationsPipe: Seq[TransformationModel], writer: Option[WriterModel] = None)
