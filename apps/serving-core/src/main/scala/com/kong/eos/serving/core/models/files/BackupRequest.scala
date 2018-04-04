
package com.kong.eos.serving.core.models.files

case class BackupRequest(fileName: String, deleteAllBefore: Boolean = false)
