
package com.kong.eos.serving.api.actor

import java.nio.file.{Files, Path}

import akka.actor.{ActorSystem, Props}
import akka.event.slf4j.SLF4JLogging
import akka.testkit.{DefaultTimeout, ImplicitSender, TestKit}
import akka.util.Timeout
import com.kong.eos.serving.api.actor.MetadataActor.{ExecuteBackup, _}
import com.kong.eos.serving.core.config.{KongCloudConfig, KongCloudConfigFactory}
import com.kong.eos.serving.core.models.KongCloudSerializer
import com.kong.eos.serving.core.models.files.{BackupRequest, KongCloudFile, KongFilesResponse}
import com.kong.eos.serving.api.actor.MetadataActor.{BackupResponse, DeleteBackups, UploadBackups}
import com.kong.eos.serving.core.config.{KongCloudConfig, KongCloudConfigFactory}
import com.kong.eos.serving.core.models.files.{KongCloudFile, KongFilesResponse}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.curator.test.TestingCluster
import org.apache.curator.utils.CloseableUtils
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import spray.http.BodyPart

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}

@RunWith(classOf[JUnitRunner])
class MetadataActorTest extends TestKit(ActorSystem("PluginActorSpec"))
  with DefaultTimeout
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll
  with BeforeAndAfterEach
  with MockitoSugar
  with SLF4JLogging
  with KongCloudSerializer {

  val tempDir: Path = Files.createTempDirectory("test")
  tempDir.toFile.deleteOnExit()

  val localConfig: Config = ConfigFactory.parseString(
    s"""
       |sparta{
       |   api {
       |     host = local
       |     port= 7777
       |   }
       |   zookeeper: {
       |     connectionString = "localhost:2181",
       |     connectionTimeout = 15000,
       |     sessionTimeout = 60000
       |     retryAttempts = 5
       |     retryInterval = 2000
       |   }
       |}
       |
       |sparta.config.backupsLocation = "$tempDir"
    """.stripMargin)

  val fileList = Seq(BodyPart("reference.conf", "file"))

  var zkTestServer: TestingCluster = _
  var clusterConfig: Option[Config] = None

  override def beforeEach(): Unit = {
    zkTestServer = new TestingCluster(1)
    zkTestServer.start()
    clusterConfig = Some(localConfig.withValue("sparta.zookeeper.connectionString",
      ConfigValueFactory.fromAnyRef(zkTestServer.getConnectString)))
    KongCloudConfig.initMainConfig(clusterConfig, KongCloudConfigFactory(localConfig))
    KongCloudConfig.initApiConfig()

  }

  override def afterAll: Unit = {
    shutdown()

    CloseableUtils.closeQuietly(zkTestServer)
  }

  override implicit val timeout: Timeout = Timeout(15 seconds)

  "MetadataActor " must {

    "Not save files with wrong extension" in {
      val metadataActor = system.actorOf(Props(new MetadataActor()))
      metadataActor ! UploadBackups(fileList)
      expectMsgPF() {
        case KongFilesResponse(Success(f: Seq[KongCloudFile])) => f.isEmpty shouldBe true
      }
      metadataActor ! DeleteBackups
      expectMsgPF() {
        case BackupResponse(Success(_)) =>
      }
    }
    "Not upload empty files" in {
      val metadataActor = system.actorOf(Props(new MetadataActor()))
      metadataActor ! UploadBackups(Seq.empty)
      expectMsgPF() {
        case KongFilesResponse(Failure(f)) => f.getMessage shouldBe "At least one file is expected"
      }
      metadataActor ! DeleteBackups
      expectMsgPF() {
        case BackupResponse(Success(_)) =>
      }
    }
    "Save a file" in {
      val metadataActor = system.actorOf(Props(new MetadataActor()))
      metadataActor ! UploadBackups(Seq(BodyPart("reference.conf", "file.json")))
      expectMsgPF() {
        case KongFilesResponse(Success(f: Seq[KongCloudFile])) => f.head.fileName.endsWith("file.json") shouldBe true
      }
      metadataActor ! DeleteBackups
      expectMsgPF() {
        case BackupResponse(Success(_)) =>
      }
    }

    "Build backup and response the uploaded file" in {

    }

  }
}
