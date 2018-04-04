package com.kong.eos.serving.core.storage

import akka.event.slf4j.SLF4JLogging
import com.kong.eos.serving.core.config.KongCloudConfig
import com.kong.eos.serving.core.constants.AppConstant
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import com.typesafe.config.Config

import scala.util.Try

object StorageFactoryHolder extends SLF4JLogging {

  private var mongoClient: Option[MongoClient] = None

  def getInstance(config: Option[Config] = KongCloudConfig.getMongoConfig): MongoClient = {
    mongoClient match {
      case None => {
        var mongoURI = Try(config.get.getString(AppConstant.MongoURI)).getOrElse(AppConstant.MongoDefaultURI)
        MongoClient(MongoClientURI(mongoURI))
      }
      case Some(client) => client
    }
  }

}
