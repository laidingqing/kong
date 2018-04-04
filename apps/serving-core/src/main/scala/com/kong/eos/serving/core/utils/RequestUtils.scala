
package com.kong.eos.serving.core.utils

import akka.event.slf4j.SLF4JLogging
import com.kong.eos.serving.core.constants.AppConstant
import com.kong.eos.serving.core.exception.ServingCoreException
import com.kong.eos.serving.core.models.submit.SubmitRequest
import com.kong.eos.serving.core.models.{ErrorModel, KongCloudSerializer}
import com.kong.eos.serving.core.storage.MongoClientUtils
import com.mongodb.casbah.MongoClient
import org.apache.curator.framework.CuratorFramework
import org.json4s.jackson.Serialization._

import scala.collection.JavaConversions
import scala.util.Try

trait RequestUtils extends KongCloudSerializer with SLF4JLogging with MongoClientUtils{

//  val curatorFramework: CuratorFramework


  def createRequest(request: SubmitRequest): Try[SubmitRequest] = {
    val requestPath = s"${AppConstant.ExecutionsPath}/${request.id}"
//    if (CuratorFactoryHolder.existsPath(requestPath)) {
//      updateRequest(request)
//    } else {
//      Try {
//        log.info(s"Creating execution with id ${request.id}")
//        curatorFramework.create.creatingParentsIfNeeded.forPath(requestPath, write(request).getBytes)
//        request
//      }
//    }
    Try{
      request
    }
  }

  def updateRequest(request: SubmitRequest): Try[SubmitRequest] = {
    Try {
      val requestPath = s"${AppConstant.ExecutionsPath}/${request.id}"
//      if (CuratorFactoryHolder.existsPath(requestPath)) {
//        curatorFramework.setData().forPath(requestPath, write(request).getBytes)
//        request
//      } else createRequest(request).getOrElse(throw new ServingCoreException(
//        ErrorModel.toString(new ErrorModel(ErrorModel.CodeNotExistsPolicyWithId,
//          s"Is not possible to create execution with id ${request.id}."))))
      request
    }
  }

  def findAllRequests(): Try[Seq[SubmitRequest]] =
    Try {
      val requestPath = s"${AppConstant.ExecutionsPath}"
//      if (CuratorFactoryHolder.existsPath(requestPath)) {
//        val children = curatorFramework.getChildren.forPath(requestPath)
//        val policiesRequest = JavaConversions.asScalaBuffer(children).toList.map(element =>
//          read[SubmitRequest](new String(curatorFramework.getData.forPath(s"${AppConstant.ExecutionsPath}/$element")))
//        )
//        policiesRequest
//      } else
        Seq.empty[SubmitRequest]
    }

  def findRequestById(id: String): Try[SubmitRequest] =
    Try {
      val requestPath = s"${AppConstant.ExecutionsPath}/$id"
//      if (CuratorFactoryHolder.existsPath(requestPath))
//        read[SubmitRequest](new String(curatorFramework.getData.forPath(requestPath)))
//      else throw new ServingCoreException(
//        ErrorModel.toString(new ErrorModel(ErrorModel.CodeNotExistsPolicyWithId, s"No execution context with id $id")))
      throw new ServingCoreException(
              ErrorModel.toString(new ErrorModel(ErrorModel.CodeNotExistsPolicyWithId, s"No execution context with id $id")))
    }

  def deleteAllRequests(): Try[_] =
    Try {
      val requestPath = s"${AppConstant.ExecutionsPath}"
//      if (CuratorFactoryHolder.existsPath(requestPath)) {
//        val children = curatorFramework.getChildren.forPath(requestPath)
//        val policiesRequest = JavaConversions.asScalaBuffer(children).toList.map(element =>
//          read[SubmitRequest](new String(curatorFramework.getData.forPath(s"${AppConstant.ExecutionsPath}/$element")))
//        )
//
//        policiesRequest.foreach(request => deleteRequest(request.id))
//      }
    }

  def deleteRequest(id: String): Try[_] =
    Try {
      val requestPath = s"${AppConstant.ExecutionsPath}/$id"
//      if (CuratorFactoryHolder.existsPath(requestPath)) {
//        log.info(s"Deleting execution with id $id")
//        curatorFramework.delete().forPath(requestPath)
//      } else
        throw new ServingCoreException(ErrorModel.toString(
        new ErrorModel(ErrorModel.CodeNotExistsPolicyWithId, s"No execution with id $id")))
    }
}
