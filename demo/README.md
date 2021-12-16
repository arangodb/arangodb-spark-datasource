# ArangoDB Spark Datasource Demo

Set ArangoDB Spark Datasource version environment variable:

```shell
export ARANGO_SPARK_VERSION=1.0.0
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
./docker/import.sh
```

## Spark 2.4

Start Spark cluster:

```shell
./docker/start_spark_2.4.sh 
```

Test the Spark application in embedded mode:
```shell
mvn -Pspark-2.4 -Pscala-$SCALA_VERSION test
```

Package the application:
```shell
mvn -Pspark-2.4 -Pscala-$SCALA_VERSION -DskipTests=true package
```

Run Spark Shell:

```shell
docker run -it --rm \
  -v $(pwd)/docker/.ivy2:/opt/bitnami/spark/.ivy2 \
  --network arangodb \
  docker.io/bitnami/spark:2.4.6 \
  ./bin/spark-shell --master spark://spark-master:7077 \
    --packages="com.arangodb:arangodb-spark-datasource-2.4_$SCALA_VERSION:$ARANGO_SPARK_VERSION"
```

Run sample code:

```scala
val options = Map("user" -> "root", "password" -> "test", "endpoints" -> "172.17.0.1:8529,172.17.0.1:8539,172.17.0.1:8549")
val usersDF = spark.read.format("com.arangodb.spark").options(options + ("table" -> "users")).load()
usersDF.show()
usersDF.printSchema()
usersDF.filter(col("name.first") === "Prudence").filter(col("birthday") === "1944-06-19").show()

// Spark SQL
usersDF.createOrReplaceTempView("users")
val californians = spark.sql("SELECT * FROM users WHERE contact.address.state = 'CA'")
californians.show()
californians.write.format("com.arangodb.spark").mode(org.apache.spark.sql.SaveMode.Overwrite).options(options + ("table" -> "californians", "confirm.truncate" -> "true")).save()
```

Submit demo program:

```shell
docker run -it --rm \
  -v $(pwd):/demo \
  -v $(pwd)/docker/.ivy2:/opt/bitnami/spark/.ivy2 \
  --network arangodb \
  docker.io/bitnami/spark:2.4.6 \
  ./bin/spark-submit --master spark://spark-master:7077 \
    --packages="com.arangodb:arangodb-spark-datasource-2.4_$SCALA_VERSION:$ARANGO_SPARK_VERSION" \
    --class Demo /demo/target/demo-$ARANGO_SPARK_VERSION.jar
```

## Spark 3.1

Start Spark cluster:

```shell
./docker/start_spark_3.1.sh 
```

Test the Spark application in embedded mode: 
```shell
mvn -Pspark-3.1 -Pscala-$SCALA_VERSION test
```

Package the application:
```shell
mvn -Pspark-3.1 -Pscala-$SCALA_VERSION -DskipTests=true package
```

Run Spark Shell:

```shell
docker run -it --rm \
  -v $(pwd)/docker/.ivy2:/opt/bitnami/spark/.ivy2 \
  --network arangodb \
  docker.io/bitnami/spark:3.1.2 \
  ./bin/spark-shell --master spark://spark-master:7077 \
    --packages="com.arangodb:arangodb-spark-datasource-3.1_$SCALA_VERSION:$ARANGO_SPARK_VERSION"
```

Run sample code:

```scala
val options = Map("user" -> "root", "password" -> "test", "endpoints" -> "172.17.0.1:8529,172.17.0.1:8539,172.17.0.1:8549")
val usersDF = spark.read.format("com.arangodb.spark").options(options + ("table" -> "users")).load()
usersDF.show()
usersDF.printSchema()
usersDF.filter(col("name.first") === "Prudence").filter(col("birthday") === "1944-06-19").show()

// Spark SQL
usersDF.createOrReplaceTempView("users")
val californians = spark.sql("SELECT * FROM users WHERE contact.address.state = 'CA'")
californians.show()
californians.write.format("com.arangodb.spark").mode(org.apache.spark.sql.SaveMode.Overwrite).options(options + ("table" -> "californians", "confirmTruncate" -> "true")).save()
```

Submit demo program:

```shell
docker run -it --rm \
  -v $(pwd):/demo \
  -v $(pwd)/docker/.ivy2:/opt/bitnami/spark/.ivy2 \
  --network arangodb \
  docker.io/bitnami/spark:3.1.2 \
  ./bin/spark-submit --master spark://spark-master:7077 \
    --packages="com.arangodb:arangodb-spark-datasource-3.1_$SCALA_VERSION:$ARANGO_SPARK_VERSION" \
    --class Demo /demo/target/demo-$ARANGO_SPARK_VERSION.jar
```
