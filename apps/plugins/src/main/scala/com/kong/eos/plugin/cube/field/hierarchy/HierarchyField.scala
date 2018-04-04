
package com.kong.eos.plugin.cube.field.hierarchy

import java.io.{Serializable => JSerializable}

import akka.event.slf4j.SLF4JLogging
import com.kong.eos.plugin.cube.field.hierarchy.HierarchyField._
import com.kong.eos.sdk.pipeline.aggregation.cube.{DimensionType, Precision}
import com.kong.eos.sdk.pipeline.schema.TypeOp
import com.kong.eos.sdk.pipeline.schema.TypeOp.TypeOp
import com.kong.eos.sdk.properties.ValidatingPropertyMap._

case class HierarchyField(props: Map[String, JSerializable], override val defaultTypeOperation : TypeOp)
  extends DimensionType with JSerializable with SLF4JLogging {

  def this(defaultTypeOperation : TypeOp) {
    this(Map(), defaultTypeOperation)
  }

  def this(props: Map[String, JSerializable]) {
    this(props,  TypeOp.ArrayString)
  }

  def this() {
    this(Map(), TypeOp.ArrayString)
  }

  override val operationProps : Map[String, JSerializable] = props

  override val properties: Map[String, JSerializable] = props ++ {
    if (!props.contains(SplitterPropertyName)) Map(SplitterPropertyName -> DefaultSplitter) else Map()
  } ++ {
    if (!props.contains(WildCardPropertyName)) Map(WildCardPropertyName -> DefaultWildCard) else Map()
  }

  final val LeftToRight = getPrecision(LeftToRightName, getTypeOperation(LeftToRightName))
  final val RightToLeft = getPrecision(RightToLeftName, getTypeOperation(RightToLeftName))
  final val LeftToRightWithWildCard =
    getPrecision(LeftToRightWithWildCardName, getTypeOperation(LeftToRightWithWildCardName))
  final val RightToLeftWithWildCard =
    getPrecision(RightToLeftWithWildCardName, getTypeOperation(RightToLeftWithWildCardName))

  override def precision(keyName: String): Precision = keyName match {
    case LeftToRight.id => LeftToRight
    case RightToLeft.id => RightToLeft
    case LeftToRightWithWildCard.id => LeftToRightWithWildCard
    case RightToLeftWithWildCard.id => RightToLeftWithWildCard
  }

  val splitter = properties.getString(SplitterPropertyName)
  val wildcard = properties.getString(WildCardPropertyName)

  override def precisionValue(keyName: String, value: Any): (Precision, JSerializable) = {
    val precisionKey = precision(keyName)
      (precisionKey, TypeOp.transformValueByTypeOp(precisionKey.typeOp,
        getPrecision(value.asInstanceOf[String], precisionKey).asInstanceOf[JSerializable]))
  }

  def getPrecision(value: String, precision: Precision): Seq[JSerializable] = {
    precision match {
      case x if x == LeftToRight =>
        explodeWithWildcards(value, wildcard, splitter, false, false)
      case x if x == RightToLeft =>
        explodeWithWildcards(value, wildcard, splitter, true, false)
      case x if x == LeftToRightWithWildCard =>
        explodeWithWildcards(value, wildcard, splitter, false, true)
      case x if x == RightToLeftWithWildCard =>
        explodeWithWildcards(value, wildcard, splitter, true, true)
    }
  }
}

object HierarchyField {

  final val DefaultSplitter = "."
  final val SplitterPropertyName = "splitter"
  final val DefaultWildCard = "*"
  final val WildCardPropertyName = "wildcard"
  final val LeftToRightName = "leftToRight"
  final val RightToLeftName = "rightToLeft"
  final val LeftToRightWithWildCardName = "leftToRightWithWildCard"
  final val RightToLeftWithWildCardName = "rightToLeftWithWildCard"

  def explodeWithWildcards(
                            domain: String,
                            wildcard: String,
                            splitter: String,
                            reversed: Boolean,
                            withWildcards: Boolean
                            ): Seq[JSerializable] = {
    val split = domain.split("\\Q" + splitter + "\\E").toSeq
    val domainTails = if (reversed) split.reverse.tails.toSeq else split.tails.toSeq
    val fullDomain = domainTails.head
    domainTails.map({
      case Nil => wildcard
      case l: Seq[String] if l == fullDomain => domain
      case l: Seq[String] => if (reversed) {
        if (withWildcards) l.reverse.mkString(splitter) + splitter + wildcard else l.reverse.mkString(splitter)
      } else if (withWildcards) wildcard + splitter + l.mkString(splitter) else l.mkString(splitter)
    })
  }
}
