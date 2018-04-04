
package com.kong.eos.plugin.transformation.json

import com.jayway.jsonpath.{Configuration, JsonPath, ReadContext}

class JsonPathExtractor(jsonDoc: String, isLeafToNull: Boolean) {

  val conf = {
    if (isLeafToNull)
      Configuration.defaultConfiguration().addOptions(com.jayway.jsonpath.Option.DEFAULT_PATH_LEAF_TO_NULL)
    else Configuration.defaultConfiguration()
  }

  private val ctx: ReadContext = JsonPath.using(conf).parse(jsonDoc)

  def query(query: String): Any = ctx.read(query)

}
