# [UNDER DEVELOPMENT] arangodb-spark-datasource

## Supported versions

There are 2 variants of this library, each one compatible with different Spark and Scala versions:

- `2.4`: compatible with Spark 2.4 and Scala 2.11
- `3.1`: compatible with Spark 3.1 and Scala 2.12

In the following sections the placeholder `${sparkVersion}` refers to one of the values above.

## Build instructions

```shell
mvn -Pspark-${sparkVersion} -DskipTests=true install
```

## Use in local maven project

```xml

<dependencies>
    <dependency>
        <groupId>com.arangodb</groupId>
        <artifactId>arangodb-spark-datasource-${sparkVersion}</artifactId>
        <version>0.0.3-SNAPSHOT</version>
    </dependency>
    <!-- ... -->
</dependencies>
```

## External Spark cluster

Submit your application with the following parameter:

```shell
--jars="./arangodb-spark-datasource-${sparkVersion}/target/arangodb-spark-datasource-${sparkVersion}-0.0.3-SNAPSHOT-jar-with-dependencies.jar"
```

## Batch Read

Batch read implements columns pruning and filters pushdowns.

```scala
val df: DataFrame = spark.read
  .format("org.apache.spark.sql.arangodb.datasource")
  .options(options) // Map[String, String]
  .schema(schema) // StructType
  .load()
```

Example:

```scala
val spark: SparkSession = SparkSession.builder()
  .appName("ArangoDBSparkDemo")
  .master("local[*]")
  .config("spark.driver.host", "127.0.0.1")
  .getOrCreate()

val df: DataFrame = spark.read
  .format("org.apache.spark.sql.arangodb.datasource")
  .options(Map(
    "password" -> "test",
    "endpoints" -> "c1:8529,c2:8529,c3:8529",
    "table" -> "users"
  ))
  .schema(new StructType(
    Array(
      StructField("likes", ArrayType(StringType, containsNull = false)),
      StructField("birthday", DateType, nullable = true),
      StructField("gender", StringType, nullable = false),
      StructField("name", StructType(
        Array(
          StructField("first", StringType, nullable = true),
          StructField("last", StringType, nullable = false)
        )
      ), nullable = true)
    )
  ))
  .load()

usersDF.filter(col("birthday") === "1982-12-15").show()
```

## Batch Write (under development)

```scala
import org.apache.spark.sql.DataFrame

val df: DataFrame = ???
df.write
  .format("org.apache.spark.sql.arangodb.datasource")
  .mode(SaveMode.Append)  //FIXME: other modes not implemented yet
  .options(Map(
    "password" -> "test",
    "endpoints" -> "c1:8529,c2:8529,c3:8529",
    "table" -> "users"
  ))
  .save()
```

## Configuration

- `user`: db user, default `root`
- `password`: db password
- `endpoints`: list of coordinators, eg. `c1:8529,c2:8529`
- `protocol`: communication protocol (`vst`|`http`), default `vst`
- `content-type`: content type for driver communication (`json`|`vpack`), default `vpack`
- `ssl.enabled`: ssl secured driver connection (`true`|`false`), default `false`
- `ssl.cert.value`: base64 encoded certificate
- `ssl.cert.type`: certificate type, default `X.509`
- `ssl.cert.alias`: certificate alias name, default `arangodb`
- `ssl.algorithm`: trustmanager algorithm, default `SunX509`
- `ssl.keystore.type`: keystore type, default `jks`
- `ssl.protocol`: SSLContext protocol, default `TLS`
- `database`: database name, default `_system`
- `table`: table name (ignored if `query` is specified)
- `batch.size`: batch size (for reading and writing), default `1000`
- `topology`: ArangoDB deployment topology (`single`|`cluster`), default `cluster`

### read parameters
- `query`: custom AQL read query. This should be used for data coming from different tables, eg. resulting from a AQL
  traversal query. In this case the data will not be partitioned, so this should not be used for fetching a lot of data.
- `sample.size`: sample size prefetched for schema inference, only used if read schema is not provided, default `1000`
- `cache`: whether the AQL query results cache shall be used, default `true`
- `fillBlockCache`: whether the query should store the data it reads in the RocksDB block cache , default `false`

### write parameters
- `waitForSync`: whether to wait until the documents have been synced to disk, default `true`
- `confirm.truncate`: confirm to truncate table when using `SaveMode.Overwrite` mode, default `false`

## Implemented filter pushdowns

- `and`
- `or`
- `not`
- `equalTo`
- `equalNullSafe`
- `isNull`
- `isNotNull`
- `greaterThan`
- `greaterThanOrEqualFilter`
- `lessThan`
- `lessThanOrEqualFilter`
- `stringStartsWithFilter`
- `stringEndsWithFilter`
- `stringContainsFilter`
- `inFilter`

## Supported Spark Datatypes

- `Date`
- `Timestamp`
- `String`
- `Boolean`
- `Float`
- `Double`
- `Integer`
- `Long`
- `Short`
- `Null`
- `Array`
- `Struct`

## Demo

[demo](./demo)
[tests](./integration-tests/src/test/scala)
