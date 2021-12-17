#!/bin/bash

docker network create arangodb --subnet 172.28.0.0/16

docker run -d --network arangodb --ip 172.28.10.1 --name spark-master -h spark-master \
  -e SPARK_MODE=master \
  -e SPARK_RPC_AUTHENTICATION_ENABLED=no \
  -e SPARK_RPC_ENCRYPTION_ENABLED=no \
  -e SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no \
  -e SPARK_SSL_ENABLED=no \
  -v $(pwd)/docker/import:/import \
  docker.io/bitnami/spark:3.1.2

docker run -d --network arangodb --ip 172.28.10.11 --name spark-worker-1 -h spark-worker-1 \
  -e SPARK_MODE=worker \
  -e SPARK_MASTER_URL=spark://spark-master:7077 \
  -e SPARK_WORKER_MEMORY=1G \
  -e SPARK_WORKER_CORES=1 \
  -e SPARK_RPC_AUTHENTICATION_ENABLED=no \
  -e SPARK_RPC_ENCRYPTION_ENABLED=no \
  -e SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no \
  -e SPARK_SSL_ENABLED=no \
  -v $(pwd)/docker/import:/import \
  docker.io/bitnami/spark:3.1.2

docker run -d --network arangodb --ip 172.28.10.12 --name spark-worker-2 -h spark-worker-2 \
  -e SPARK_MODE=worker \
  -e SPARK_MASTER_URL=spark://spark-master:7077 \
  -e SPARK_WORKER_MEMORY=1G \
  -e SPARK_WORKER_CORES=1 \
  -e SPARK_RPC_AUTHENTICATION_ENABLED=no \
  -e SPARK_RPC_ENCRYPTION_ENABLED=no \
  -e SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no \
  -e SPARK_SSL_ENABLED=no \
  -v $(pwd)/docker/import:/import \
  docker.io/bitnami/spark:3.1.2

docker run -d --network arangodb --ip 172.28.10.13 --name spark-worker-3 -h spark-worker-3 \
  -e SPARK_MODE=worker \
  -e SPARK_MASTER_URL=spark://spark-master:7077 \
  -e SPARK_WORKER_MEMORY=1G \
  -e SPARK_WORKER_CORES=1 \
  -e SPARK_RPC_AUTHENTICATION_ENABLED=no \
  -e SPARK_RPC_ENCRYPTION_ENABLED=no \
  -e SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no \
  -e SPARK_SSL_ENABLED=no \
  -v $(pwd)/docker/import:/import \
  docker.io/bitnami/spark:3.1.2
