
package com.kong.eos.serving.core.models.info

case class AppInfo(pomVersion: String,
                   profileId: String,
                   buildTimestamp: String,
                   devContact: String,
                   supportContact: String,
                   license: String)
