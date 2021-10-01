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
- `table`: collection name (ignored if `query` is specified)
- `batch.size`: batch size (for reading and writing), default `1000`
- `topology`: ArangoDB deployment topology (`single`|`cluster`), default `cluster`

### read parameters
- `query`: custom AQL read query. This should be used for data coming from different tables, eg. resulting from a AQL
  traversal query. In this case the data will not be partitioned, so this should not be used for fetching a lot of data.
- `sample.size`: sample size prefetched for schema inference, only used if read schema is not provided, default `1000`
- `cache`: whether the AQL query results cache shall be used, default `true`
- `fill.cache`: whether the query should store the data it reads in the RocksDB block cache , default `false`

### write parameters
- `wait.sync`: whether to wait until the documents have been synced to disk, default `true`
- `confirm.truncate`: confirm to truncate table when using `SaveMode.Overwrite` mode, default `false`
- `overwrite.mode`: configures the behavior in case a document with the specified `_key` value exists already
  - `ignore`: it will not be written
  - `replace`: it will be overwritten with the specified document value
  - `update`: it will be patched (partially updated) with the specified document value. The overwrite mode can be further controlled via the `keep.null` and `merge.objects` parameters.
  - `conflict`: return a unique constraint violation error so that the insert operation fails (default)
- `keep.null`: in case `overwrite.mode` is set to `update`
  - `true`: `null` values are saved within the document (default)
  - `false`: `null` values are used to delete corresponding existing attributes
- `merge.objects`: in case `overwrite.mode` is set to `update`, controls whether objects (not arrays) will be merged.
  - `true`: objects will be merged (default)
  - `false`: existing document fields will be overwritten

## SaveMode

On writing, `org.apache.spark.sql.SaveMode` is used to specify the expected behavior in case the target collection 
already exists.  

Spark 2.4 implementation supports all save modes with the following semantics:
- `Append`: the target collection is created if it does not exist
- `Overwrite`: the target collection is created if it does not exist, it is truncated otherwise. Use in combination with `confirm.truncate` write configuration parameter.
- `ErrorIfExists`: the target collection is created if it does not exist, an `AnalysisException` is thrown otherwise 
- `Ignore`: the target collection is created if it does not exist, no write is performed otherwise

Spark 3.1 implementation supports:
- `Append`: the target collection is created if it does not exist
- `Overwrite`: the target collection is created if it does not exist, it is truncated otherwise. Use in combination with `confirm.truncate` write configuration parameter.
The other `SaveMode` values (`ErrorIfExists` and `Ignore`) behave the same as `Append`.

Use `overwrite.mode` write configuration parameter to specify the documents overwrite behavior (in case a document with 
the same `_key` already exists).


## Limitations
- Batch writes are not performed atomically, so in some cases (i.e. in case of `overwrite.mode: conflict`) some documents 
  in the batch may be written and some others may return an exception (i.e. due to conflicting key).


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
