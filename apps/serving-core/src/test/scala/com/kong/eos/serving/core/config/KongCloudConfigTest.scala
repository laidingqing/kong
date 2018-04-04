
package com.kong.eos.serving.core.config

import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}

@RunWith(classOf[JUnitRunner])
class KongCloudConfigTest extends WordSpec with Matchers {

  "SpartaConfig class" should{

    "initMainConfig should return X" in {

      val config = ConfigFactory.parseString(
        """
          |sparta {
          |
          | "testKey" : "test"
          |}
        """.stripMargin)

      val res = KongCloudConfig.initMainConfig(Some(config), KongCloudConfigFactory(config)).get.toString
      res should be ("""Config(SimpleConfigObject({"testKey":"test"}))""")

    }
    "initApiConfig should return X" in {
      KongCloudConfig.mainConfig = None

      val configApi = ConfigFactory.parseString(
        """
          | api {
          |       "host" : "localhost"
          |       "port" : 9090
          |      }
        """.stripMargin)

      val res = KongCloudConfig.initApiConfig(KongCloudConfigFactory(configApi)).get.toString
      res should be ("""Config(SimpleConfigObject({"host":"localhost","port":9090}))""")

    }


    "getClusterConfig(Case: Success) should return cluster config" in {
      KongCloudConfig.mainConfig = None
      KongCloudConfig.apiConfig = None

      val configCluster = ConfigFactory.parseString(
        """
          |sparta {
          |  config {
          |    executionMode = mesos
          |    rememberPartitioner = true
          |    topGracefully = false
          |  }
          |  mesos {
          |   deployMode = cluster
          |   numExecutors = 2
          |  }
          |  }
        """.stripMargin
      )

      KongCloudConfig.initMainConfig(Some(configCluster), KongCloudConfigFactory(configCluster))

      val clusterConf = KongCloudConfig.getClusterConfig().get.toString

      clusterConf should be ("""Config(SimpleConfigObject({"deployMode":"cluster","numExecutors":2}))""")

    }
    "getClusterConfig(Case: Success and executionMode = local) should return None" in {
      KongCloudConfig.mainConfig = None
      KongCloudConfig.apiConfig = None

      val configCluster = ConfigFactory.parseString(
        """
          |sparta {
          |  config {
          |    executionMode = local
          |    rememberPartitioner = true
          |    topGracefully = false
          |  }
          |  }
        """.stripMargin
      )

      KongCloudConfig.initMainConfig(Some(configCluster), KongCloudConfigFactory(configCluster))

      val clusterConf = KongCloudConfig.getClusterConfig()

      clusterConf should be (None)

    }

    "getClusterConfig(Case: _) should return None" in {
      KongCloudConfig.mainConfig = None
      KongCloudConfig.apiConfig = None

      val clusterConf = KongCloudConfig.getClusterConfig()

      clusterConf should be (None)

    }


    "getHdfsConfig(Case: Some(config) should return hdfs config" in {
      KongCloudConfig.mainConfig = None
      KongCloudConfig.apiConfig = None

      val configHdfs = ConfigFactory.parseString(
        """
          |sparta {
          |  hdfs {
          |    "hadoopUserName" : "stratio"
          |    "hadoopConfDir" : "/home/ubuntu"
          |  }
          |  }
        """.stripMargin
      )

      KongCloudConfig.initMainConfig(Some(configHdfs), KongCloudConfigFactory(configHdfs))

      val hdfsConf = KongCloudConfig.getHdfsConfig.get.toString

      hdfsConf should be ("""Config(SimpleConfigObject({"hadoopConfDir":"/home/ubuntu","hadoopUserName":"stratio"}))""")

    }
    "getHdfsConfig(Case: None) should return hdfs config" in {
      KongCloudConfig.mainConfig = None
      KongCloudConfig.apiConfig = None

      val hdfsConf = KongCloudConfig.getHdfsConfig

      hdfsConf should be (None)

    }

    "getDetailConfig (Case: Some(Config) should return the config" in {
      KongCloudConfig.mainConfig = None
      KongCloudConfig.apiConfig = None

      val configDetail = ConfigFactory.parseString(
        """
          |sparta {
          |  config {
          |    "executionMode": "local"
          |    "rememberPartitioner": true
          |    "topGracefully": false
          |  }
          |  }
        """.stripMargin
      )

      KongCloudConfig.initMainConfig(Some(configDetail), KongCloudConfigFactory(configDetail))

      val detailConf = KongCloudConfig.getDetailConfig.get.toString

      detailConf should be
      (
        """"Config(SimpleConfigObject({
          |"executionMode":"local",
          |"rememberPartitioner":true,
          |"topGracefully":false
          |}))"""".stripMargin)

    }
    "getDetailConfig (Case: None should return the config" in {
      KongCloudConfig.mainConfig = None
      KongCloudConfig.apiConfig = None

      val detailConf = KongCloudConfig.getDetailConfig

      detailConf should be (None)
    }
    "getZookeeperConfig (Case: Some(config) should return zookeeper conf" in {
      KongCloudConfig.mainConfig = None
      KongCloudConfig.apiConfig = None

      val configZk = ConfigFactory.parseString(
        """
          |sparta {
          |  zookeeper {
          |    "connectionString" : "localhost:6666"
          |    "connectionTimeout": 15000
          |    "sessionTimeout": 60000
          |    "retryAttempts": 5
          |    "retryInterval": 10000
          |  }
          |  }
        """.stripMargin
      )

      KongCloudConfig.initMainConfig(Some(configZk), KongCloudConfigFactory(configZk))

      val zkConf = KongCloudConfig.getZookeeperConfig.get.toString

      zkConf should be
      (
        """"Config(SimpleConfigObject(
          |{"connectionString":"localhost:6666",
          |"connectionTimeout":15000,
          |"retryAttempts":5,
          |"retryInterval":10000,
          |"sessionTimeout":60000
          |}))"""".stripMargin)

    }
    "getZookeeperConfig (Case: None) should return zookeeper conf" in {
      KongCloudConfig.mainConfig = None
      KongCloudConfig.apiConfig = None

      val zkConf = KongCloudConfig.getZookeeperConfig

      zkConf should be (None)
    }

    "initOptionalConfig should return a config" in {

      val config = ConfigFactory.parseString(
        """
          |sparta {
          | testKey : "testValue"
          |}
        """.stripMargin)

      val spartaConfig = KongCloudConfig.initOptionalConfig(
        node = "sparta",
        configFactory = KongCloudConfigFactory(config))
      spartaConfig.get.getString("testKey") should be ("testValue")
    }

    "getOptionStringConfig should return None" in {

      val config = ConfigFactory.parseString(
        """
          |sparta {
          | testKey : "testValue"
          |}
        """.stripMargin)
      val res = KongCloudConfig.getOptionStringConfig(
        node = "sparta",
        currentConfig = config)

      res should be (None)
    }
  }
}
