import org.apache.spark.sql.SparkSession

object Demo {
  val importPath: String = System.getProperty("importPath", "/demo/docker/import")
  val password: String = System.getProperty("password", "test")
  val endpoints: String = System.getProperty("endpoints", "172.17.0.1:8529,172.17.0.1:8539,172.17.0.1:8549")
  val sslEnabled: String = System.getProperty("ssl.enabled", "false")
  val sslCertValue: String = System.getProperty("ssl.cert.value")

  val spark: SparkSession = SparkSession.builder
    .appName("arangodb-demo")
    .master("local[*, 3]")
    .getOrCreate

  val options = Map(
    "password" -> password,
    "endpoints" -> endpoints,
    "ssl.enabled" -> sslEnabled,
    "ssl.cert.value" -> sslCertValue
  )

  def main(args: Array[String]): Unit = {
    WriteDemo.writeDemo()
    ReadDemo.readDemo()
    ReadWriteDemo.readWriteDemo()
    spark.stop
  }

}
