# ArangoDB Spark Datasource Demo

This demo is composed of 3 parts:

- `WriteDemo`: reads the input json files as Spark Dataframes, applies conversions to map the data to Spark data types
  and writes the records into ArangoDB collections
- `ReadDemo`: reads the ArangoDB collections created above as Spark Dataframes, specifying columns selection and records
  filters predicates or custom AQL queries
- `ReadWriteDemo`: reads the ArangoDB collections created above as Spark Dataframes, applies projections and filtering,
  writes to a new ArangoDB collection

There are demos available written in Scala & Python (using PySpark) as outlined below.

## Requirements

This demo requires:

- JDK 8, 11 or 17
- `maven`
- `docker`

For the python demo, you will also need
- `python`

## Prepare the environment

Set environment variables:

```shell
export ARANGO_SPARK_VERSION=1.9.0-SNAPSHOT
```

Start ArangoDB cluster with docker:

```shell
SSL=true STARTER_MODE=cluster ./docker/start_db.sh
```

The deployed cluster will be accessible at [https://172.28.0.1:8529](http://172.28.0.1:8529) with username `root` and
password `test`.

Start Spark cluster:

```shell
./docker/start_spark.sh 
```

## Install locally

NB: this is only needed for SNAPSHOT versions.

```shell
mvn -f ../pom.xml install -Dmaven.test.skip=true -Dgpg.skip=true -Dmaven.javadoc.skip=true -Pscala-2.12 -Pspark-3.5
```

## Run embedded

Test the Spark application in embedded mode:

```shell
mvn \
  -Pscala-2.12 -Pspark-3.5 \
  test
```

Test the Spark application against ArangoDB Oasis deployment:

```shell
mvn \
  -Pscala-2.12 -Pspark-3.5 \
  -Dpassword=<root-password> \
  -Dendpoints=<endpoint> \
  -Dssl.cert.value=<base64-encoded-cert> \
  test
```

## Submit to Spark cluster

Package the application:

```shell
mvn package -Dmaven.test.skip=true -Pscala-2.12 -Pspark-3.5
```

Submit demo program:

```shell
docker run -it --rm \
  -v $(pwd):/demo \
  -v $(pwd)/docker/.ivy2:/opt/bitnami/spark/.ivy2 \
  -v $HOME/.m2/repository:/opt/bitnami/spark/.m2/repository \
  --network arangodb \
  docker.io/bitnami/spark:3.5.2 \
  ./bin/spark-submit --master spark://spark-master:7077 \
    --packages="com.arangodb:arangodb-spark-datasource-3.5_2.12:$ARANGO_SPARK_VERSION" \
    --class Demo /demo/target/demo-$ARANGO_SPARK_VERSION.jar    
```

## Python(PySpark) Demo

This demo requires the same environment setup as outlined above.
Additionally, the python requirements will need to be installed as follows:
```shell
pip install -r ./python-demo/requirements.txt
```

To run the PySpark demo, run 
```shell
python ./python-demo/demo.py \
  --ssl-enabled=true \
  --endpoints=172.28.0.1:8529,172.28.0.1:8539,172.28.0.1:8549
```

To run it against an Oasis deployment, run
```shell
python ./python-demo/demo.py \
  --password=<root-password> \
  --endpoints=<endpoint> \
  --ssl-enabled=true \
  --ssl-cert-value=<base64-encoded-cert>
```