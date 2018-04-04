package com.kong.eos.sdk.properties

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.write

case class JsoneyString(string : String) {
  override def toString : String = string
  def toSeq : Seq[String] = {
    // transfors string of the form "[\"prop1\",\"prop2\"]" in a Seq
    string.drop(1).dropRight(1).replaceAll("\"","").split(",").toSeq
  }
}

class JsoneyStringSerializer extends CustomSerializer[JsoneyString](format => (
  {
    case obj : JObject => {
      new JsoneyString(write(obj)(implicitly(DefaultFormats + new JsoneyStringSerializer)))
    }
    case obj : org.json4s.JsonAST.JNull.type => {
      new JsoneyString(null)
    }
    case obj : JArray => {
      new JsoneyString(write(obj)(implicitly(DefaultFormats + new JsoneyStringSerializer)))
    }
    case s: JString =>
      new JsoneyString(s.s)
    case i : JInt =>
      new JsoneyString(i.num.toString())
    case b : JBool =>
      new JsoneyString(b.value.toString())
  },
  {
    case x: JsoneyString =>
      if(x.string == null) {
        new JString("")
      } else if(x.string.contains("[") && x.string.contains("{")) {
        parse(x.string)
      } else if(x.string.equals("true") || x.string.equals("false")) {
        new JBool(x.string.toBoolean)
      } else {
        new JString(x.string)
      }
  }
)) {
}
