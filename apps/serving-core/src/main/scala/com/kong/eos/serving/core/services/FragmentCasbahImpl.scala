package com.kong.eos.serving.core.services

import akka.event.slf4j.SLF4JLogging
import com.kong.eos.serving.core.config.KongCloudConfig
import com.kong.eos.serving.core.constants.DatabaseConstant
import com.kong.eos.serving.core.exception.ServingCoreException
import com.kong.eos.serving.core.models.policy.fragment.FragmentElementModel
import com.kong.eos.serving.core.storage.{MongoConversions, StorageFactoryHolder}
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.Imports._

import scala.util.{Failure, Success, Try}

class FragmentCasbahImpl(val dbName: String) extends SLF4JLogging with FragmentCasbahService with MongoConversions{

  val Fragment_Collection_Name = "fragments"

  var mongoClient = StorageFactoryHolder.getInstance()
  val mongoDB = mongoClient(dbName)

  val writeConcern:WriteConcern = {
    mongoClient.setWriteConcern(WriteConcern.Safe)
    mongoClient.getWriteConcern
  }

  /**
    * 创建Fragment.
    * @param fragment
    * @return
    */
  def createFragment(fragment: FragmentElementModel): Try[FragmentElementModel] = try{
    val mongos = fragmentElementModelToMogo(fragment)
    mongoDB(Fragment_Collection_Name).insert(mongos)
    Success(fragment)
  }catch {
    case e: Exception => {
      log.error("db", e)
      Failure(new ServingCoreException("db.error"))
    }
  }

  def findAllFragment(userId: String): Try[List[FragmentElementModel]] = try{
    val query = MongoDBObject("userId" -> userId)
    val fragments = for {
      x <- mongoDB(Fragment_Collection_Name).find(query)
    } yield {
      mongoToFragmentElementModel(x)
    }
    Success(fragments.toList)
  }catch {
    case e: Exception => {
      log.error("db", e)
      Failure(new ServingCoreException("db.error"))
    }
  }

  def findFragmentsByType(fragmentType: String, userId: String): Try[List[FragmentElementModel]] = try{
    val query = MongoDBObject("userId" -> userId, "fragmentType" -> fragmentType)
    val fragments = for {
      x <- mongoDB(Fragment_Collection_Name).find(query)
    } yield {
      mongoToFragmentElementModel(x)
    }
    Success(fragments.toList)
  }catch {
    case e: Exception => {
      log.error("db", e)
      Failure(new ServingCoreException("db.error"))
    }
  }

  def findFragmentByTypeAndId(fragmentType: String, id: String): Try[Option[FragmentElementModel]] = try{
    val query = MongoDBObject("fragmentType" -> fragmentType, "_id" -> id)
    val fragment = for {
      x <- mongoDB(Fragment_Collection_Name).findOne(query)
    } yield {
      mongoToFragmentElementModel(x)
    }
    Success(fragment)
  }catch {
    case e: Exception => {
      log.error("db", e)
      Failure(new ServingCoreException("db.error"))
    }
  }


  def deleteFragmentById(id: String): Try[Unit] = try{
    val query = MongoDBObject("_id" -> id)
    val result = mongoDB(Fragment_Collection_Name).remove(query)

    Success(result)
  }catch {
    case e: Exception => {
      log.error("db", e)
      Failure(new ServingCoreException("db.error"))
    }
  }

  def deleteFragmentByType(userId: String, fragmentType: String): Try[Unit] = try{
    val query = MongoDBObject("userId" -> userId, "fragmentType" -> fragmentType)
    val result = mongoDB(Fragment_Collection_Name).remove(query)

    Success(result)
  }catch {
    case e: Exception => {
      log.error("db", e)
      Failure(new ServingCoreException("db.error"))
    }
  }

  def updateFragment(userId: String, fragment: FragmentElementModel): Try[FragmentElementModel] = try{
    val query = MongoDBObject("userId" -> userId, "_id" -> fragment.id)
    val result = mongoDB(Fragment_Collection_Name).findOne(query) match {
      case Some(userDBObject) => {
        val set = $set("name" -> fragment.name)
        mongoDB(Fragment_Collection_Name).update(userDBObject, set)
        fragment
      }
      case None => throw new ServingCoreException("error update.")
    }

    Success(result)
  }catch {
    case e: Exception => {
      log.error("db", e)
      Failure(new ServingCoreException("db.error"))
    }
  }

}

trait FragmentCasbahService{
  /*create a fragment by user*/
  def createFragment(fragment: FragmentElementModel): Try[FragmentElementModel]
  /*find all fragments by user*/
  def findAllFragment(userId: String): Try[List[FragmentElementModel]]
  /*find all fragments by type and user*/
  def findFragmentsByType(fragmentType: String, userId: String): Try[List[FragmentElementModel]]
  /*find fragment by type and id and user*/
  def findFragmentByTypeAndId(fragmentType: String, id: String): Try[Option[FragmentElementModel]]
  /*delete fragment by id*/
  def deleteFragmentById(id: String): Try[Unit]
  /*delete fragment by type*/
  def deleteFragmentByType(userId: String, fragmentType: String): Try[Unit]

  def updateFragment(userId: String, fragment: FragmentElementModel): Try[FragmentElementModel]
}

object FragmentCasbahService{
  val config = KongCloudConfig.getMongoConfig
  val dbName = Try(config.get.getString("dbName")).getOrElse("sparta")
  private val casbahFragmentService = new FragmentCasbahImpl(dbName)
  def apply(): FragmentCasbahService = casbahFragmentService
}