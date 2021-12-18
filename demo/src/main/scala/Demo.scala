import org.apache.spark.sql.SparkSession

object Demo {
  val spark: SparkSession = SparkSession.builder
    .appName("arangodb-demo")
    .master("local[*, 3]")
    .getOrCreate

  val password = "test"
  val endpoints = "172.17.0.1:8529,172.17.0.1:8539,172.17.0.1:8549"

  val options = Map(
    "password" -> password,
    "endpoints" -> endpoints
  )

  def main(args: Array[String]): Unit = {
    WriteDemo.writeDemo()
    ReadDemo.readDemo()
    ReadWriteDemo.readWriteDemo()
    spark.stop
  }

}
