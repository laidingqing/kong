
package com.kong.eos.serving.core.models.files

import scala.util.Try

case class KongFilesResponse(files: Try[Seq[KongCloudFile]])

