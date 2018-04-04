package com.kong.eos.serving.core.storage

trait MongoClientUtils {
  var mongoClient = StorageFactoryHolder.getInstance()
}
