# ArangoDB Spark Datasource Demo

Set environment variables:

```shell
export ARANGO_SPARK_VERSION=1.0.0
```

Start ArangoDB cluster with docker:

```shell
STARTER_MODE=cluster ./docker/start_db.sh
```

The deployed cluster will be accessible at [http://172.17.0.1:8529](http://172.17.0.1:8529) with username `root` and
password `test`.

Start Spark cluster:

```shell
./docker/start_spark_3.1.sh 
```

Test the Spark application in embedded mode: 
```shell
mvn -Pspark-3.1 -Pscala-2.12 -DimportPath=docker/import test
```

Package the application:
```shell
mvn -Pspark-3.1 -Pscala-2.12 -DskipTests=true package
```

Submit demo program:

```shell
docker run -it --rm \
  -v $(pwd):/demo \
  -v $(pwd)/docker/.ivy2:/opt/bitnami/spark/.ivy2 \
  --network arangodb \
  docker.io/bitnami/spark:3.1.2 \
  ./bin/spark-submit --master spark://spark-master:7077 \
    --packages="com.arangodb:arangodb-spark-datasource-3.1_2.12:$ARANGO_SPARK_VERSION" \
    --class Demo /demo/target/demo-$ARANGO_SPARK_VERSION.jar
```
