
package com.kong.eos.serving.core.models.dto

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * destroy's class
  * @deprecated
  */
object LoggedUser{

  implicit def jsonToDto(stringJson: String): Option[LoggedUser] = {
    if (stringJson.trim.isEmpty) None
    else {
      implicit val json = new ObjectMapper().readTree(stringJson)

      Some(LoggedUser(getValue(LoggedUserConstant.infoIdTag), getValue(LoggedUserConstant.infoNameTag),
        getValue(LoggedUserConstant.infoMailTag, Some(LoggedUserConstant.dummyMail)),
        getValue(LoggedUserConstant.infoGroupIDTag), getArrayValues(LoggedUserConstant.infoGroupsTag),
        getArrayValues(LoggedUserConstant.infoRolesTag)))
    }
  }

  private def getValue(tag: String, defaultElse: Option[String]= None)(implicit json: JsonNode) : String = {
    Option(json.findValue(tag)) match {
      case Some(jsonValue) =>
        defaultElse match{
          case Some(value) => Try(jsonValue.asText()).getOrElse(value)
          case None => Try(jsonValue.asText()).get
        }
      case None =>
        defaultElse match {
          case Some(value) => value
          case None => ""
        }
    }
  }

  private def getArrayValues(tag:String)(implicit jsonNode: JsonNode): Seq[String] = {
    Option(jsonNode.findValue(tag)) match {
      case Some(roles: ArrayNode) => roles.asScala.map(x => x.asText()).toSeq
      case None => Seq.empty[String]
    }
  }
}

case class LoggedUser(id: String, name: String, email: String, gid: String, groups:Seq[String], roles: Seq[String]){
  def isAuthorized(securityEnabled: Boolean, allowedRoles: Seq[String] = LoggedUserConstant.allowedRoles): Boolean = {
    if(securityEnabled){
      roles.intersect(allowedRoles).nonEmpty
    } else true
  }

}
