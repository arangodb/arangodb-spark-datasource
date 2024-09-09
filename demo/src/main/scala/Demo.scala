import org.apache.spark.sql.SparkSession

object Demo {
  val importPath: String = System.getProperty("importPath", "/demo/docker/import")
  val password: String = System.getProperty("password", "test")
  val endpoints: String = System.getProperty("endpoints", "172.28.0.1:8529,172.28.0.1:8539,172.28.0.1:8549")
  val sslEnabled: String = System.getProperty("ssl.enabled", "true")
  val sslCertValue: String = System.getProperty("ssl.cert.value", "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURlekNDQW1PZ0F3SUJBZ0lFZURDelh6QU5CZ2txaGtpRzl3MEJBUXNGQURCdU1SQXdEZ1lEVlFRR0V3ZFYKYm10dWIzZHVNUkF3RGdZRFZRUUlFd2RWYm10dWIzZHVNUkF3RGdZRFZRUUhFd2RWYm10dWIzZHVNUkF3RGdZRApWUVFLRXdkVmJtdHViM2R1TVJBd0RnWURWUVFMRXdkVmJtdHViM2R1TVJJd0VBWURWUVFERXdsc2IyTmhiR2h2CmMzUXdIaGNOTWpBeE1UQXhNVGcxTVRFNVdoY05NekF4TURNd01UZzFNVEU1V2pCdU1SQXdEZ1lEVlFRR0V3ZFYKYm10dWIzZHVNUkF3RGdZRFZRUUlFd2RWYm10dWIzZHVNUkF3RGdZRFZRUUhFd2RWYm10dWIzZHVNUkF3RGdZRApWUVFLRXdkVmJtdHViM2R1TVJBd0RnWURWUVFMRXdkVmJtdHViM2R1TVJJd0VBWURWUVFERXdsc2IyTmhiR2h2CmMzUXdnZ0VpTUEwR0NTcUdTSWIzRFFFQkFRVUFBNElCRHdBd2dnRUtBb0lCQVFDMVdpRG5kNCt1Q21NRzUzOVoKTlpCOE53STBSWkYzc1VTUUdQeDNsa3FhRlRaVkV6TVpMNzZIWXZkYzlRZzdkaWZ5S3lRMDlSTFNwTUFMWDlldQpTc2VEN2JaR25mUUg1MkJuS2NUMDllUTN3aDdhVlE1c04yb215Z2RITEM3WDl1c250eEFmdjdOem12ZG9nTlhvCkpReVkvaFNaZmY3UklxV0g4Tm5BVUtranFPZTZCZjVMRGJ4SEtFU21yRkJ4T0NPbmhjcHZaV2V0d3BpUmRKVlAKd1VuNVA4MkNBWnpmaUJmbUJabkI3RDBsKy82Q3Y0ak11SDI2dUFJY2l4blZla0JRemwxUmd3Y3p1aVpmMk1HTwo2NHZETU1KSldFOUNsWkYxdVF1UXJ3WEY2cXdodVAxSG5raWk2d05iVHRQV2xHU2txZXV0cjAwNCtIemJmOEtuClJZNFBBZ01CQUFHaklUQWZNQjBHQTFVZERnUVdCQlRCcnY5QXd5bnQzQzVJYmFDTnlPVzV2NEROa1RBTkJna3EKaGtpRzl3MEJBUXNGQUFPQ0FRRUFJbTlyUHZEa1lwbXpwU0loUjNWWEc5WTcxZ3hSRHJxa0VlTHNNb0V5cUdudwovengxYkRDTmVHZzJQbmNMbFc2elRJaXBFQm9vaXhJRTlVN0t4SGdaeEJ5MEV0NkVFV3ZJVW1ucjZGNEYrZGJUCkQwNTBHSGxjWjdlT2VxWVRQWWVRQzUwMkcxRm80dGROaTRsRFA5TDlYWnBmN1ExUWltUkgycWFMUzAzWkZaYTIKdFk3YWgvUlFxWkw4RGt4eDgvemMyNXNnVEhWcHhvSzg1M2dsQlZCcy9FTk1peUdKV21BWFFheWV3WTNFUHQvOQp3R3dWNEttVTNkUERsZVFlWFNVR1BVSVNlUXhGankrakN3MjFwWXZpV1ZKVE5CQTlsNW55M0doRW1jbk9UL2dRCkhDdlZSTHlHTE1iYU1aNEpyUHdiK2FBdEJncmdlaUs0eGVTTU12cmJodz09Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K")

  val spark: SparkSession = SparkSession.builder
    .appName("arangodb-demo")
    .master("local[*, 3]")
    .getOrCreate

  val options: Map[String, String] = Map(
    "password" -> password,
    "endpoints" -> endpoints,
    "ssl.enabled" -> sslEnabled,
    "ssl.cert.value" -> sslCertValue,
    "ssl.verifyHost" -> "false"
  )

  def main(args: Array[String]): Unit = {
    WriteDemo.writeDemo()
    ReadDemo.readDemo()
    ReadWriteDemo.readWriteDemo()
    spark.stop
  }

}
