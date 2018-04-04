package com.kong.eos.serving.core.storage

import com.kong.eos.serving.core.models.policy.PolicyElementModel
import com.kong.eos.serving.core.models.policy.fragment.FragmentElementModel
import com.mongodb.DBObject
import com.mongodb.casbah.Imports.DBObject
import com.mongodb.casbah.commons.MongoDBObject
import org.bson.types.ObjectId

trait MongoConversions {

  /**
    * FragmentElementModel to MongoDB
    * @param obj
    * @return
    */
  def fragmentElementModelToMogo(obj: FragmentElementModel): DBObject = {
    val db = MongoDBObject.newBuilder

    db += "_id" -> obj.id
    db += "fragmentType" -> obj.fragmentType
    db += "name" -> obj.name
    db += "description" -> obj.description
    db += "shortDescription" -> obj.shortDescription

    db += "element" -> policyElementModelToMongo(obj.element)

    db.result()

  }

  /**
    * mapper db object to model
    * @param obj
    * @return
    */
  def mongoToFragmentElementModel(obj: DBObject): FragmentElementModel = {

    val elementObj = obj.get("element").asInstanceOf[DBObject]

    val element = PolicyElementModel(
      name = elementObj.get("name").asInstanceOf[String],
      `type` = elementObj.get("fragmentType").asInstanceOf[String],
      configuration = Map()//TODO Map configuration
    )

    FragmentElementModel(
      id = Some(obj.get("_id").asInstanceOf[String]),
      userId = obj.get("userId").asInstanceOf[String],
      fragmentType = obj.get("fragmentType").asInstanceOf[String],
      name = obj.get("name").asInstanceOf[String],
      description = obj.get("description").asInstanceOf[String],
      shortDescription = obj.get("shortDescription").asInstanceOf[String],
      element = element
    )
  }

  private def policyElementModelToMongo(obj: PolicyElementModel): DBObject = {
    val db = MongoDBObject.newBuilder
    db += "name" -> obj.name
    db += "type" -> obj.`type`
    db += "configuration" -> obj.configuration

    db.result()
  }

}
