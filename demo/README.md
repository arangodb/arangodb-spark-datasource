# ArangoDB Spark Datasource Demo

Set ArangoDB Spark Datasource version environment variable:

```shell
export ARANGO_SPARK_VERSION=0.0.7-SNAPSHOT
```

Start ArangoDB cluster:

```shell
./docker/start_db_cluster.sh docker.io/arangodb/arangodb:3.7.12
```

## Spark 2.4

Start Spark cluster:

```shell
./docker/start_spark_2.4.sh 
```

Build the project (with Java 8):

```shell
mvn -Pspark-2.4 package
```

Alternatively, an already built jar with dependencies can be found inside the latest published 
[package](https://github.com/orgs/arangodb/packages?tab=packages&q=com.arangodb.arangodb-spark-datasource-2.4).

Run Spark Shell:

```shell
docker run -it --rm \
  -v $(pwd):/arangodb-spark-datasource \
  --network arangodb \
  bde2020/spark-base:2.4.5-hadoop2.7 \
  ./spark/bin/spark-shell --master spark://spark-master:7077 \
    --jars="/arangodb-spark-datasource/arangodb-spark-datasource-2.4/target/arangodb-spark-datasource-2.4-$ARANGO_SPARK_VERSION-jar-with-dependencies.jar"
```

Run sample code:

```scala
  val options = Map(
  "database" -> "sparkConnectorTest",
  "user" -> "root",
  "password" -> "test",
  "endpoints" -> "172.17.0.1:8529,172.17.0.1:8539,172.17.0.1:8549",
  "table" -> "users"
)
val usersDF = spark.read.format("org.apache.spark.sql.arangodb.datasource").options(options).load()
usersDF.show()
usersDF.filter(col("name.first") === "Prudence").filter(col("birthday") === "1944-06-19").show()
```

Submit demo program:

```shell
docker run -it --rm \
  -v $(pwd):/arangodb-spark-datasource \
  --network arangodb \
  bde2020/spark-base:2.4.5-hadoop2.7 \
  ./spark/bin/spark-submit --master spark://spark-master:7077 \
    --jars="/arangodb-spark-datasource/arangodb-spark-datasource-2.4/target/arangodb-spark-datasource-2.4-$ARANGO_SPARK_VERSION-jar-with-dependencies.jar" \
    --class Demo /arangodb-spark-datasource/demo/target/demo-$ARANGO_SPARK_VERSION.jar
```

## Spark 3.1

Start Spark cluster:

```shell
./docker/start_spark_3.1.sh 
```

Build the project:

```shell
mvn -Pspark-3.1 package
```

Alternatively, an already built jar with dependencies can be found inside the latest published
[package](https://github.com/orgs/arangodb/packages?tab=packages&q=com.arangodb.arangodb-spark-datasource-3.1).

Run Spark Shell:

```shell
docker run -it --rm \
  -v $(pwd):/arangodb-spark-datasource \
  --network arangodb \
  bde2020/spark-base:3.1.1-hadoop3.2 \
  ./spark/bin/spark-shell --master spark://spark-master:7077 \
    --jars="/arangodb-spark-datasource/arangodb-spark-datasource-3.1/target/arangodb-spark-datasource-3.1-$ARANGO_SPARK_VERSION-jar-with-dependencies.jar"
```

Run sample code:

```scala
  val options = Map(
  "database" -> "sparkConnectorTest",
  "user" -> "root",
  "password" -> "test",
  "endpoints" -> "172.17.0.1:8529,172.17.0.1:8539,172.17.0.1:8549",
  "table" -> "users"
)
val usersDF = spark.read.format("org.apache.spark.sql.arangodb.datasource").options(options).load()
usersDF.show()
usersDF.filter(col("name.first") === "Prudence").filter(col("birthday") === "1944-06-19").show()
```

Submit demo program:

```shell
docker run -it --rm \
  -v $(pwd):/arangodb-spark-datasource \
  --network arangodb \
  bde2020/spark-base:3.1.1-hadoop3.2 \
  ./spark/bin/spark-submit --master spark://spark-master:7077 \
    --jars="/arangodb-spark-datasource/arangodb-spark-datasource-3.1/target/arangodb-spark-datasource-3.1-$ARANGO_SPARK_VERSION-jar-with-dependencies.jar" \
    --class Demo /arangodb-spark-datasource/demo/target/demo-$ARANGO_SPARK_VERSION.jar
```

## CONNECT TO OASIS

To connect to SSL secured deployments using X.509 base64 encoded CA certificate (Oasis):

```scala
  val options = Map(
  "database" -> "<dbname>",
  "user" -> "root",
  "password" -> "<passwd>",
  "endpoints" -> "<endpoint>:18529",
  "ssl.cert.value" -> "<base64 encoded CA certificate>",
  "ssl.enabled" -> "true",
  "table" -> "<table>"
)
val myDF = spark.read.format("org.apache.spark.sql.arangodb.datasource").options(options).load()
```