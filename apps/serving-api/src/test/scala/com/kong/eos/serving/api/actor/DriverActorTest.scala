
package com.kong.eos.serving.api.actor

import java.nio.file.{Files, Path}

import akka.actor.{ActorSystem, Props}
import akka.testkit.{DefaultTimeout, ImplicitSender, TestKit}
import akka.util.Timeout
import com.kong.eos.serving.api.actor.DriverActor.UploadDrivers
import com.kong.eos.serving.core.config.{KongCloudConfig, KongCloudConfigFactory}
import com.kong.eos.serving.core.models.KongCloudSerializer
import com.kong.eos.serving.core.models.files.{KongCloudFile, KongFilesResponse}
import com.kong.eos.serving.api.actor.DriverActor.UploadDrivers
import com.kong.eos.serving.core.config.{KongCloudConfig, KongCloudConfigFactory}
import com.kong.eos.serving.core.models.files.{KongCloudFile, KongFilesResponse}
import com.typesafe.config.{Config, ConfigFactory}
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import spray.http.BodyPart

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}

@RunWith(classOf[JUnitRunner])
class DriverActorTest extends TestKit(ActorSystem("PluginActorSpec"))
  with DefaultTimeout
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll
  with BeforeAndAfterEach
  with MockitoSugar with KongCloudSerializer {

  val tempDir: Path = Files.createTempDirectory("test")
  tempDir.toFile.deleteOnExit()

  val localConfig: Config = ConfigFactory.parseString(
    s"""
       |sparta{
       |   api {
       |     host = local
       |     port= 7777
       |   }
       |}
       |
       |sparta.config.driverPackageLocation = "$tempDir"
    """.stripMargin)

  val fileList = Seq(BodyPart("reference.conf", "file"))

  override def beforeEach(): Unit = {
    KongCloudConfig.initMainConfig(Option(localConfig), KongCloudConfigFactory(localConfig))
    KongCloudConfig.initApiConfig()
  }

  override def afterAll: Unit = {
    shutdown()
  }

  override implicit val timeout: Timeout = Timeout(15 seconds)

  "DriverActor " must {

    "Not save files with wrong extension" in {
      val driverActor = system.actorOf(Props(new DriverActor()))
      driverActor ! UploadDrivers(fileList)
      expectMsgPF() {
        case KongFilesResponse(Success(f: Seq[KongCloudFile])) => f.isEmpty shouldBe true
      }
    }
    "Not upload empty files" in {
      val driverActor = system.actorOf(Props(new DriverActor()))
      driverActor ! UploadDrivers(Seq.empty)
      expectMsgPF() {
        case KongFilesResponse(Failure(f)) => f.getMessage shouldBe "At least one file is expected"
      }
    }
    "Save a file" in {
      val driverActor = system.actorOf(Props(new DriverActor()))
      driverActor ! UploadDrivers(Seq(BodyPart("reference.conf", "file.jar")))
      expectMsgPF() {
        case KongFilesResponse(Success(f: Seq[KongCloudFile])) => f.head.fileName.endsWith("file.jar") shouldBe true
      }
    }
  }
}
