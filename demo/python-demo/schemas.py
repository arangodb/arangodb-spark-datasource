from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType, IntegerType

movie_schema: StructType = StructType([
    StructField("_id", StringType(), nullable=False),
    StructField("_key", StringType(), nullable=False),
    StructField("description", StringType()),
    StructField("genre", StringType()),
    StructField("homepage", StringType()),
    StructField("imageUrl", StringType()),
    StructField("imdbId", StringType()),
    StructField("language", StringType()),
    StructField("lastModified", TimestampType()),
    StructField("releaseDate", DateType()),
    StructField("runtime", IntegerType()),
    StructField("studio", StringType()),
    StructField("tagline", StringType()),
    StructField("title", StringType()),
    StructField("trailer", StringType())
])
person_schema: StructType = StructType([
    StructField("_id", StringType(), nullable=False),
    StructField("_key", StringType(), nullable=False),
    StructField("biography", StringType()),
    StructField("birthday", DateType()),
    StructField("birthplace", StringType()),
    StructField("lastModified", TimestampType()),
    StructField("name", StringType()),
    StructField("profileImageUrl", StringType())
])
edges_schema: StructType = StructType([
    StructField("_key", StringType(), nullable=False),
    StructField("_from", StringType(), nullable=False),
    StructField("_to", StringType(), nullable=False),
    StructField("$label", StringType()),
    StructField("name", StringType()),
    StructField("type", StringType()),
])
acts_in_schema: StructType = StructType([
    StructField("_id", StringType(), nullable=False),
    StructField("_key", StringType(), nullable=False),
    StructField("_from", StringType(), nullable=False),
    StructField("_to", StringType(), nullable=False),
    StructField("name", StringType())
])
directed_schema: StructType = StructType([
    StructField("_id", StringType(), nullable=False),
    StructField("_key", StringType(), nullable=False),
    StructField("_from", StringType(), nullable=False),
    StructField("_to", StringType(), nullable=False)
])
