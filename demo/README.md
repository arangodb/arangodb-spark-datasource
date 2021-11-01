# ArangoDB Spark Datasource Demo

Set ArangoDB Spark Datasource version environment variable:

```shell
export ARANGO_SPARK_VERSION=0.0.10-SNAPSHOT
```

Set Scala version:

```shell
# Scala 2.11 is only supported by Spark 2.4
export SCALA_VERSION=2.11

# Scala 2.12 is supported by both Spark 2.4 and 3.1
export SCALA_VERSION=2.12
```

Start ArangoDB cluster:

```shell
STARTER_MODE=cluster ./docker/start_db.sh
```

Import users sample data:

```shell
curl -u root:test http://172.17.0.1:8529/_api/collection -d '{"name": "users", "numberOfShards": 6}'
docker run --rm -v $(pwd)/docker/import:/import arangodb \
  arangoimport --server.endpoint=http+tcp://172.17.0.1:8529 --server.password=test \
  --file "/import/users/users.json" --type json --collection "users"
```

## Spark 2.4

Start Spark cluster:

```shell
./docker/start_spark_2.4.sh 
```

Build `arangodb-spark-datasource` project (with Java 8):
```shell
mvn -f ../pom.xml -Pspark-2.4 -Pscala-$SCALA_VERSION -DskipTests=true install
```

Alternatively, an already built jar with dependencies can be found inside the latest published 
[package](https://github.com/orgs/arangodb/packages?tab=packages&q=com.arangodb.arangodb-spark-datasource-2.4).


Build the `demo` project (with Java 8):
```shell
mvn -Pspark-2.4 -Pscala-$SCALA_VERSION package
```

Run the Spark application in embedded mode:
```shell
mvn -Pspark-2.4 -Pscala-$SCALA_VERSION exec:java -Dexec.classpathScope="test" -Dexec.mainClass="Demo"
```

Run Spark Shell:

```shell
docker run -it --rm \
  -v $(pwd)/..:/arangodb-spark-datasource \
  --network arangodb \
  bde2020/spark-base:2.4.5-hadoop2.7 \
  ./spark/bin/spark-shell --master spark://spark-master:7077 \
    --jars="/arangodb-spark-datasource/arangodb-spark-datasource-2.4/target/arangodb-spark-datasource-2.4_$SCALA_VERSION-$ARANGO_SPARK_VERSION-jar-with-dependencies.jar"
```

Run sample code:

```scala
val options = Map("user" -> "root", "password" -> "test", "endpoints" -> "172.17.0.1:8529,172.17.0.1:8539,172.17.0.1:8549")
val usersDF = spark.read.format("org.apache.spark.sql.arangodb.datasource").options(options + ("table" -> "users")).load()
usersDF.show()
usersDF.printSchema()
usersDF.filter(col("name.first") === "Prudence").filter(col("birthday") === "1944-06-19").show()

// Spark SQL
usersDF.createOrReplaceTempView("users")
val californians = spark.sql("SELECT * FROM users WHERE contact.address.state = 'CA'")
californians.show()
californians.write.format("org.apache.spark.sql.arangodb.datasource").mode(org.apache.spark.sql.SaveMode.Overwrite).options(options + ("table" -> "californians", "confirm.truncate" -> "true")).save()
```

Submit demo program:

```shell
docker run -it --rm \
  -v $(pwd)/..:/arangodb-spark-datasource \
  --network arangodb \
  bde2020/spark-base:2.4.5-hadoop2.7 \
  ./spark/bin/spark-submit --master spark://spark-master:7077 \
    --jars="/arangodb-spark-datasource/arangodb-spark-datasource-2.4/target/arangodb-spark-datasource-2.4_$SCALA_VERSION-$ARANGO_SPARK_VERSION-jar-with-dependencies.jar" \
    --class Demo /arangodb-spark-datasource/demo/target/demo-$ARANGO_SPARK_VERSION.jar
```

## Spark 3.1

Start Spark cluster:

```shell
./docker/start_spark_3.1.sh 
```

Build `arangodb-spark-datasource` project:

```shell
mvn -f ../pom.xml -Pspark-3.1 -Pscala-$SCALA_VERSION -DskipTests=true install
```

Alternatively, an already built jar with dependencies can be found inside the latest published
[package](https://github.com/orgs/arangodb/packages?tab=packages&q=com.arangodb.arangodb-spark-datasource-3.1).

Build the `demo` project:
```shell
mvn -Pspark-3.1 -Pscala-$SCALA_VERSION package
```

Run the Spark application in embedded mode:
```shell
mvn -Pspark-3.1 -Pscala-$SCALA_VERSION exec:java -Dexec.classpathScope="test" -Dexec.mainClass="Demo"
```

Run Spark Shell:

```shell
docker run -it --rm \
  -v $(pwd)/..:/arangodb-spark-datasource \
  --network arangodb \
  bde2020/spark-base:3.1.1-hadoop3.2 \
  ./spark/bin/spark-shell --master spark://spark-master:7077 \
    --jars="/arangodb-spark-datasource/arangodb-spark-datasource-3.1/target/arangodb-spark-datasource-3.1_$SCALA_VERSION-$ARANGO_SPARK_VERSION-jar-with-dependencies.jar"
```

Run sample code:

```scala
val options = Map("user" -> "root", "password" -> "test", "endpoints" -> "172.17.0.1:8529,172.17.0.1:8539,172.17.0.1:8549")
val usersDF = spark.read.format("org.apache.spark.sql.arangodb.datasource").options(options + ("table" -> "users")).load()
usersDF.show()
usersDF.printSchema()
usersDF.filter(col("name.first") === "Prudence").filter(col("birthday") === "1944-06-19").show()

// Spark SQL
usersDF.createOrReplaceTempView("users")
val californians = spark.sql("SELECT * FROM users WHERE contact.address.state = 'CA'")
californians.show()
californians.write.format("org.apache.spark.sql.arangodb.datasource").mode(org.apache.spark.sql.SaveMode.Overwrite).options(options + ("table" -> "californians", "confirm.truncate" -> "true")).save()
```

Submit demo program:

```shell
docker run -it --rm \
  -v $(pwd)/..:/arangodb-spark-datasource \
  --network arangodb \
  bde2020/spark-base:3.1.1-hadoop3.2 \
  ./spark/bin/spark-submit --master spark://spark-master:7077 \
    --jars="/arangodb-spark-datasource/arangodb-spark-datasource-3.1/target/arangodb-spark-datasource-3.1_$SCALA_VERSION-$ARANGO_SPARK_VERSION-jar-with-dependencies.jar" \
    --class Demo /arangodb-spark-datasource/demo/target/demo-$ARANGO_SPARK_VERSION.jar
```
