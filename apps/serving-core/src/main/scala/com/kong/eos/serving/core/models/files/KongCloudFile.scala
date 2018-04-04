
package com.kong.eos.serving.core.models.files

import com.kong.eos.serving.core.models.info.AppInfo

case class KongCloudFile(fileName: String,
                         uri: String,
                         localPath: String,
                         size: String,
                         version: Option[AppInfo] = None)
