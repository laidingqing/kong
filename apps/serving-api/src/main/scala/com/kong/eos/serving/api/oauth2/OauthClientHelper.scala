package com.kong.eos.serving.api.oauth2

import com.kong.eos.serving.core.models.dto.OAuth2._
import com.kong.eos.serving.core.models.dto.OAuth2Constants
import com.fasterxml.jackson.databind.ObjectMapper

object OauthClientHelper {

  def parseTokenRs(tokenResponse: String): Option[OAuth2Info] = {
    implicit val json = new ObjectMapper().readTree(tokenResponse)
    Some(OAuth2Info(getLongValue(OAuth2Constants.ExpiresIn), getValue(OAuth2Constants.UserName), getValue(OAuth2Constants.ClientId)))
  }

}
