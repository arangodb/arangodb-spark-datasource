import org.apache.spark.sql.SparkSession

object Demo {
  val importPath: String = System.getProperty("importPath", "/demo/docker/import")
  val password: String = System.getProperty("password", "test")
  val endpoints: String = System.getProperty("endpoints", "172.28.0.1:8529,172.28.0.1:8539,172.28.0.1:8549")
  val sslCertValue: String = System.getProperty("ssl.cert.value", null)

  val spark: SparkSession = SparkSession.builder
    .appName("arangodb-demo")
    .master("local[*, 3]")
    .getOrCreate

  val options: Map[String, String] = Map(
    "password" -> password,
    "endpoints" -> endpoints,
    "ssl.cert.value" -> sslCertValue,
    "ssl.enabled" -> "true",
    "protocol" -> "vst",
    "contentType" -> "vpack"
  )

  def main(args: Array[String]): Unit = {
    WriteDemo.writeDemo()
    ReadDemo.readDemo()
    ReadWriteDemo.readWriteDemo()
    spark.stop
  }

}
