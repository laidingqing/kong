package com.kong.eos.serving.api.oauth2

import com.kong.eos.serving.core.models.dto.OAuth2.OAuth2Info
import com.kong.eos.serving.core.models.dto.OAuth2Constants
import spray.httpx.SprayJsonSupport
import spray.json.{DefaultJsonProtocol, DeserializationException, JsArray, JsNumber, JsObject, JsString, JsValue, RootJsonFormat}

object OAuth2InfoProtocol extends DefaultJsonProtocol with SprayJsonSupport {

  implicit val oauth2InfoFormat = jsonFormat4(OAuth2Info)

  implicit object OAuth2InfoJsonFormat extends RootJsonFormat[OAuth2Info] {

    def write(a: OAuth2Info): JsValue = JsObject(
      "expire" -> JsNumber(a.expire),
      "userName" -> JsString(a.userName),
      "clientId" -> JsString(a.clientId)
    )

    def read(value: JsValue) = {
      value.asJsObject.getFields(OAuth2Constants.ExpiresIn, OAuth2Constants.UserName, OAuth2Constants.ClientId) match {
        case Seq(JsNumber(expiresIn), JsString(userName), JsArray(authorities), JsString(clientId), JsArray(scope)) =>
          new OAuth2Info(expiresIn.longValue(), userName, clientId)
        case _ => throw new DeserializationException("OAuth2Info expected")
      }
    }
  }

}