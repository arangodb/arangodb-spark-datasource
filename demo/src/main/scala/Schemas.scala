import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType, TimestampType}

object Schemas {
  val movieSchema: StructType = StructType(Array(
    StructField("_id", StringType, nullable = false),
    StructField("description", StringType),
    StructField("genre", StringType),
    StructField("homepage", StringType),
    StructField("imageUrl", StringType),
    StructField("imdbId", StringType),
    StructField("language", StringType),
    StructField("lastModified", TimestampType),
    StructField("releaseDate", DateType),
    StructField("runtime", IntegerType),
    StructField("studio", StringType),
    StructField("tagline", StringType),
    StructField("title", StringType),
    StructField("trailer", StringType)
  ))

  val personSchema: StructType = StructType(Array(
    StructField("_id", StringType, nullable = false),
    StructField("biography", StringType),
    StructField("birthday", DateType),
    StructField("birthplace", StringType),
    StructField("lastModified", TimestampType),
    StructField("name", StringType),
    StructField("profileImageUrl", StringType)
  ))

  val actsInSchema: StructType = StructType(Array(
    StructField("_id", StringType, nullable = false),
    StructField("_from", StringType, nullable = false),
    StructField("_to", StringType, nullable = false),
    StructField("name", StringType)
  ))

  val directedSchema: StructType = StructType(Array(
    StructField("_id", StringType, nullable = false),
    StructField("_from", StringType, nullable = false),
    StructField("_to", StringType, nullable = false)
  ))

}
