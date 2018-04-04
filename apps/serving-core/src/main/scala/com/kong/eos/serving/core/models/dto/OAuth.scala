package com.kong.eos.serving.core.models.dto

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.kong.eos.serving.core.models.dto.OAuth2.OAuth2Info

import scala.util.Try


object OAuth2Constants {
  val ClientId = "client_id"
  val ExpiresIn = "exp"
  val UserName = "user_name"
  val UserId   = "user_id"

  val AnonymousUser = OAuth2Info(0, "Anonymous", "")
}

object OAuth2 {
  case class OAuth2Info(expire: Long, userName: String, clientId: String, userId: String = ""){

    def apply(expire: Long, userName: String, clientId: String, userId: String = ""): OAuth2Info = new OAuth2Info(expire, userName, clientId, userId)

    def isAuthorized(securityEnabled: Boolean): Boolean = {
      if(securityEnabled){
        true
      } else true
    }


  }


  implicit def jsonToDto(stringJson: String): Option[OAuth2Info] = {
    if (stringJson.trim.isEmpty) None
    else {
      implicit val json = new ObjectMapper().readTree(stringJson)
      Some(OAuth2Info(getLongValue("expire"), getValue("userName"), getValue("clientId")))
    }
  }

  def getValue(tag: String, defaultElse: Option[String] = None)(implicit json: JsonNode): String = {
    Option(json.findValue(tag)) match {
      case Some(jsonValue) =>
        defaultElse match {
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

  def getLongValue(tag: String)(implicit jsonNode: JsonNode): Long = {
    Option(jsonNode.findValue(tag)) match {
      case Some(jsonValue) => Try(jsonValue.asLong()).getOrElse(0)
      case None => 0
    }
  }
}