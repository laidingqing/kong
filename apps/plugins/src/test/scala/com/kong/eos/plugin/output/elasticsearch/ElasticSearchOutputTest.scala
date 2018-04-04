
package com.kong.eos.plugin.output.elasticsearch

import com.kong.eos.sdk.properties.JsoneyString
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ElasticSearchOutputTest extends FlatSpec with ShouldMatchers {

  trait BaseValues {

    final val localPort = 9200
    final val remotePort = 9300
    val output = getInstance()
    val outputMultipleNodes = new ElasticSearchOutput("ES-out",
      Map("nodes" ->
        new JsoneyString(
          s"""[{"node":"host-a","tcpPort":"$remotePort","httpPort":"$localPort"},{"node":"host-b",
              |"tcpPort":"9301","httpPort":"9201"}]""".stripMargin),
        "dateType" -> "long"))

    def getInstance(host: String = "localhost", httpPort: Int = localPort, tcpPort: Int = remotePort)
    : ElasticSearchOutput =
      new ElasticSearchOutput("ES-out",
        Map("nodes" -> new JsoneyString( s"""[{"node":"$host","httpPort":"$httpPort","tcpPort":"$tcpPort"}]"""),
          "clusterName" -> "elasticsearch"))
  }

  trait NodeValues extends BaseValues {

    val ipOutput = getInstance("127.0.0.1", localPort, remotePort)
    val ipv6Output = getInstance("0:0:0:0:0:0:0:1", localPort, remotePort)
    val remoteOutput = getInstance("dummy", localPort, remotePort)
  }

  trait TestingValues extends BaseValues {

    val indexNameType = "spartatable/sparta"
    val tableName = "spartaTable"
    val baseFields = Seq(StructField("string", StringType), StructField("int", IntegerType))
    val schema = StructType(baseFields)
    val extraFields = Seq(StructField("id", StringType, false), StructField("timestamp", LongType, false))
    val properties = Map("nodes" -> new JsoneyString(
      """[{"node":"localhost","httpPort":"9200","tcpPort":"9300"}]""".stripMargin),
      "dateType" -> "long",
      "clusterName" -> "elasticsearch")
    override val output = new ElasticSearchOutput("ES-out", properties)
    val dateField = StructField("timestamp", TimestampType, false)
    val expectedDateField = StructField("timestamp", LongType, false)
    val stringField = StructField("string", StringType)
    val expectedStringField = StructField("string", StringType)
  }

  trait SchemaValues extends BaseValues {

    val fields = Seq(
      StructField("long", LongType),
      StructField("double", DoubleType),
      StructField("decimal", DecimalType(10, 0)),
      StructField("int", IntegerType),
      StructField("boolean", BooleanType),
      StructField("date", DateType),
      StructField("timestamp", TimestampType),
      StructField("array", ArrayType(StringType)),
      StructField("map", MapType(StringType, IntegerType)),
      StructField("string", StringType),
      StructField("binary", BinaryType))
    val completeSchema = StructType(fields)
  }

  "ElasticSearchOutput" should "format properties" in new NodeValues with SchemaValues {
    output.httpNodes should be(Seq(("localhost", 9200)))
    outputMultipleNodes.httpNodes should be(Seq(("host-a", 9200), ("host-b", 9201)))
    output.clusterName should be("elasticsearch")
  }

  it should "parse correct index name type" in new TestingValues {
    output.indexNameType(tableName) should be(indexNameType)
  }

  it should "return a Seq of tuples (host,port) format" in new NodeValues {

    output.getHostPortConfs("nodes", "localhost", "9200", "node", "httpPort") should be(List(("localhost", 9200)))
    output.getHostPortConfs("nodes", "localhost", "9300", "node", "tcpPort") should be(List(("localhost", 9300)))
    outputMultipleNodes.getHostPortConfs("nodes", "localhost", "9200", "node", "httpPort") should be(List(
      ("host-a", 9200), ("host-b", 9201)))
    outputMultipleNodes.getHostPortConfs("nodes", "localhost", "9300", "node", "tcpPort") should be(List(
      ("host-a", 9300), ("host-b", 9301)))
  }
}
