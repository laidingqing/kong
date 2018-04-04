package com.kong.eos.serving.api.service.http

import java.io.File

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import com.kong.eos.sdk.exception.MockException
import com.kong.eos.serving.api.actor.MetadataActor
import com.kong.eos.serving.api.actor.MetadataActor._
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.core.config.KongCloudConfig
import com.kong.eos.serving.core.constants.{AkkaConstant, AppConstant}
import com.kong.eos.serving.core.models.dto.{LoggedUserConstant, OAuth2Constants}
import com.kong.eos.serving.core.models.files.{BackupRequest, KongCloudFile, KongFilesResponse}
import org.junit.runner.RunWith
import org.mockito.Mockito.when
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import spray.http.HttpEntity.NonEmpty
import spray.http.{HttpEntity, _}
import spray.httpx.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._
import spray.routing.HttpService
import spray.routing.directives.RouteDirectives.complete

import scala.util.{Failure, Success, Try}

@RunWith(classOf[JUnitRunner])
class MetadataHttpServiceTest extends WordSpec
  with MetadataHttpService
  with HttpServiceBaseTest
  with MockitoSugar {

  val metadataTestProbe= TestProbe()
  val dummyUser = OAuth2Constants.AnonymousUser

  override implicit val actors: Map[String, ActorRef] = Map(
    AkkaConstant.MetadataActorName -> metadataTestProbe.ref
  )

  override val supervisor: ActorRef = testProbe.ref


  "MetadataHttpService.buildBackup" when {
    "everything goes right" should {
      "create a ZK backup" in {
        val fileResponse = Seq(KongCloudFile("backup",
          "/etc/sds/sparta/backup","/etc/sds/sparta/backup","251"))
        startAutopilot(KongFilesResponse(Success(fileResponse)))
        Get(s"/${HttpConstant.MetadataPath}/backup/build") ~> routes(dummyUser) ~> check {
          testProbe.expectMsgType[BuildBackup.type]
          status should be(StatusCodes.OK)
          responseAs[Seq[KongCloudFile]] should equal(fileResponse)
        }
      }
    }
    "there is an error" should {
      "return a 500 error" in {
        startAutopilot(BackupResponse(Failure(new MockException)))
        Get(s"/${HttpConstant.MetadataPath}/backup/build") ~> routes(dummyUser) ~> check {
          testProbe.expectMsgType[BuildBackup.type]
          status should be(StatusCodes.InternalServerError)
        }
      }
    }
  }

  "MetadataHttpService.executeBackup" when {
    "everything goes right" should {
      "restore a ZK backup" in {
        startAutopilot(BackupResponse(Success("OK")))
        Post(s"/${HttpConstant.MetadataPath}/backup", BackupRequest("backup1")) ~> routes(dummyUser) ~> check {
          testProbe.expectMsgType[ExecuteBackup]
          status should be(StatusCodes.OK)
        }
      }
    }
    "there is an error" should {
      "return a 500 error" in {
        startAutopilot(BackupResponse(Failure(new MockException())))
        Post(s"/${HttpConstant.MetadataPath}/backup", BackupRequest("backup1")) ~> routes(dummyUser) ~> check {
          testProbe.expectMsgType[ExecuteBackup]
          status should be(StatusCodes.InternalServerError)
        }
      }
    }
  }

  "MetadataHttpService.uploadBackup" when {
    "everything goes right" should {
      "upload a ZK backup" in {
        val fileResponse = Seq(KongCloudFile("backup",
          "/etc/sds/sparta/backup","/etc/sds/sparta/backup","251"))
        startAutopilot(KongFilesResponse(Success(fileResponse)))
        Put(s"/${HttpConstant.MetadataPath}/backup") ~> routes(dummyUser) ~> check {
          testProbe.expectMsgType[UploadBackups]
          status should be(StatusCodes.OK)
          responseAs[Seq[KongCloudFile]] should equal(fileResponse)
        }
      }
    }
    "there is an error" should {
      "return a 500 error" in {
        startAutopilot(KongFilesResponse(Failure(new MockException())))
        Put(s"/${HttpConstant.MetadataPath}/backup") ~> routes(dummyUser) ~> check {
          testProbe.expectMsgType[UploadBackups]
          status should be(StatusCodes.InternalServerError)
        }
      }
    }
  }

  "MetadataHttpService.getAllBackups" when {
    "everything goes right" should {
      "retrieve all the ZK backups" in {
        val fileResponse = Seq(KongCloudFile("a", "", "", ""), KongCloudFile("b","","",""))
        startAutopilot(KongFilesResponse(Success(fileResponse)))
        Get(s"/${HttpConstant.MetadataPath}/backup") ~> routes(dummyUser) ~> check {
          testProbe.expectMsgType[ListBackups.type]
          status should be(StatusCodes.OK)
          responseAs[Seq[KongCloudFile]] should equal(fileResponse)
        }
      }
    }
    "there is an error" should {
      "return a 500 error" in {
        startAutopilot(KongFilesResponse(Failure(new MockException())))
        Get(s"/${HttpConstant.MetadataPath}/backup") ~> routes(dummyUser) ~> check {
          testProbe.expectMsgType[ListBackups.type]
          status should be(StatusCodes.InternalServerError)
        }
      }
    }
  }

  "MetadataHttpService.deleteAllBackups" when {
    "everything goes right" should {
      "retrieve all the ZK backups" in {
        startAutopilot(BackupResponse(Success("Ok")))
        Delete(s"/${HttpConstant.MetadataPath}/backup") ~> routes(dummyUser) ~> check {
          testProbe.expectMsgType[DeleteBackups.type]
          status should be(StatusCodes.OK)
        }
      }
    }
    "there is an error" should {
      "return a 500 error" in {
        startAutopilot(BackupResponse(Failure(new MockException())))
        Delete(s"/${HttpConstant.MetadataPath}/backup") ~> routes(dummyUser) ~> check {
          testProbe.expectMsgType[DeleteBackups.type]
          status should be(StatusCodes.InternalServerError)
        }
      }
    }
  }

  "MetadataHttpService.deleteBackup" when {
    "everything goes right" should {
      "delete the desired backup" in {
        val fileToDelete = "backup1"
        startAutopilot(BackupResponse(Success("Ok")))
        Delete(s"/${HttpConstant.MetadataPath}/backup/$fileToDelete") ~> routes(dummyUser) ~> check {
          testProbe.expectMsgType[DeleteBackup]
          status should be(StatusCodes.OK)
        }
      }
    }
    "there is an error" should {
      "return a 500 error" in {
        val fileToDelete = "backup1"
        startAutopilot(BackupResponse(Failure(new MockException())))
        Delete(s"/${HttpConstant.MetadataPath}/backup/$fileToDelete") ~> routes(dummyUser) ~> check {
          testProbe.expectMsgType[DeleteBackup]
          status should be(StatusCodes.InternalServerError)
        }
      }
    }
  }


  "MetadataHttpService.cleanMetadata" when {
    "everything goes right" should {
      "clean all data in ZK" in {
        startAutopilot(BackupResponse(Success("Ok")))
        Delete(s"/${HttpConstant.MetadataPath}") ~> routes(dummyUser) ~> check {
          testProbe.expectMsgType[CleanMetadata.type]
          status should be(StatusCodes.OK)
        }
      }
    }
    "there is an error" should {
      "return a 500 error" in {
        startAutopilot(BackupResponse(Failure(new MockException())))
        Delete(s"/${HttpConstant.MetadataPath}") ~> routes(dummyUser) ~> check {
          testProbe.expectMsgType[CleanMetadata.type]
          status should be(StatusCodes.InternalServerError)
        }
      }
    }
  }
}