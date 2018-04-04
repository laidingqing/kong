
package com.kong.eos.sdk.properties

import akka.event.slf4j.SLF4JLogging
import com.kong.eos.sdk.properties.models.{HostsPortsModel, PropertiesFieldsModel, PropertiesQueriesModel}
import com.kong.eos.sdk.properties.models.{HostsPortsModel, PropertiesFieldsModel, PropertiesQueriesModel}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization._

import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}


class ValidatingPropertyMap[K, V](val m: Map[K, V]) extends SLF4JLogging {

  def getString(key: K): String =
    m.get(key) match {
      case Some(value: String) => value
      case Some(value) => value.toString
      case None =>
        throw new IllegalStateException(s"$key is mandatory")
    }

  def getHostsPorts(key: K): HostsPortsModel = {
    implicit val json4sJacksonFormats: Formats =
      DefaultFormats +
        new JsoneyStringSerializer()

    read[HostsPortsModel](
      s"""{"hostsPorts": ${m.get(key).fold("[]") { values => values.toString }}}""""
    )
  }

  def getPropertiesQueries(key: K): PropertiesQueriesModel = {
    implicit val json4sJacksonFormats: Formats =
      DefaultFormats + new JsoneyStringSerializer()

    read[PropertiesQueriesModel](
      s"""{"queries": ${m.get(key).fold("[]") { values => values.toString }}}""""
    )
  }

  def getPropertiesFields(key: K): PropertiesFieldsModel = {
    implicit val json4sJacksonFormats: Formats =
      DefaultFormats + new JsoneyStringSerializer()

    read[PropertiesFieldsModel](
      s"""{"fields": ${m.get(key).fold("[]") { values => values.toString }}}""""
    )
  }

  def getMapFromJsoneyString(key: K): Seq[Map[String, String]] = {
    m.get(key) match {
      case Some(value) =>

        val parsed = parse(s"""{"children": ${value.toString} }"""")

        val result: List[Map[String, String]] =
          for {
            JArray(element) <- parsed \ "children"
            JObject(list) <- element
          } yield {
            (for {
              JField(key, JString(value)) <- list
            } yield (key, value)).toMap
          }
        if (result.isEmpty)
          throw new IllegalStateException(s"$key is mandatory")
        else result
      case None => throw new IllegalStateException(s"$key is mandatory")
    }
  }

  def getString(key: K, default: String): String = {
    m.get(key) match {
      case Some(value: String) => if (value.isEmpty) default else value
      case Some(null) => default
      case Some(value) => if (value.toString.isEmpty) default else value.toString
      case None => default
    }
  }

  def getString(key: K, default: Option[String]): Option[String] = {
    m.get(key) match {
      case Some(value: String) => if (value.isEmpty) default else Some(value)
      case Some(null) => default
      case Some(value) => if (value.toString.isEmpty) default else Some(value.toString)
      case None => default
    }
  }

  //scalastyle:off
  def getBoolean(key: K): Boolean =
      m.get(key) match {
      case Some(value: String) => value.toBoolean
      case Some(value: Int) =>
        value.asInstanceOf[Int] match {
          case 1 => true
          case 0 => false
          case _ => throw new IllegalStateException(s"$value is not parsable as boolean")
        }
      case Some(value: Boolean) => value
      case Some(value) =>
        Try(value.toString.toBoolean) match {
          case Success(v) => v
          case Failure(ex) => throw new IllegalStateException(s"Bad value for $key", ex)
        }
      case None => throw new IllegalStateException(s"$key is mandatory")
    }

  //scalastyle:on

  def getInt(key: K): Int =
    m.get(key) match {
      case Some(value: String) =>
        Try(value.toInt) match {
          case Success(v) => v
          case Failure(ex) => throw new IllegalStateException(s"Bad value for $key", ex)
        }
      case Some(value: Int) => value
      case Some(value: Long) => value.toInt
      case Some(value) =>
        Try(value.toString.toInt) match {
          case Success(v) => v
          case Failure(ex) => throw new IllegalStateException(s"Bad value for $key", ex)
        }
      case None =>
        throw new IllegalStateException(s"$key is mandatory")
    }

  def getOptionsList(key: K,
                     propertyKey: String,
                     propertyValue: String): Map[String, String] =
    Try(getMapFromJsoneyString(key)).getOrElse(Seq.empty[Map[String, String]])
      .map(c =>
        (c.get(propertyKey) match {
          case Some(value) => value.toString
          case None => throw new IllegalStateException(s"The field $propertyKey is mandatory")
        },
          c.get(propertyValue) match {
            case Some(value) => value.toString
            case None => throw new IllegalStateException(s"The field $propertyValue is mandatory")
          })).toMap

  def hasKey(key: K): Boolean = m.get(key).isDefined
}

class NotBlankOption(s: Option[String]) {
  def notBlankWithDefault(default: String): String = notBlank.getOrElse(default)

  def notBlank: Option[String] = s.map(_.trim).filterNot(_.isEmpty)
}

object ValidatingPropertyMap {

  implicit def map2ValidatingPropertyMap[K, V](m: Map[K, V]): ValidatingPropertyMap[K, V] =
    new ValidatingPropertyMap[K, V](m)

  implicit def option2NotBlankOption(s: Option[String]): NotBlankOption = new NotBlankOption(s)

}