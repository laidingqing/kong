
package com.kong.eos.serving.core.models.dto

object LoggedUserConstant {
  val infoNameTag = "cn"
  val infoIdTag = "id"
  val infoMailTag = "mail"
  val infoRolesTag = "roles"
  val infoGroupIDTag= "gidNumber"
  val infoGroupsTag= "groups"

  val dummyMail = "email@email.com"

  val AnonymousUser = LoggedUser("*", "Anonymous", dummyMail,"0",Seq.empty[String],Seq.empty[String])

  val allowedRoles = Seq("FullAdministrator","management_admin","sparta","sparta_zk")
}
