package org.apache.spark.sql.arangodb.commons


class ArangoClientTest {

  private val client = new ArangoClient(ArangoOptions(Map(
    "database" -> "sparkConnectorTest",
    "user" -> "root",
    "password" -> "test",
    "endpoints" -> "172.28.3.1:8529",
    "table" -> "aaa"
  )))


}
