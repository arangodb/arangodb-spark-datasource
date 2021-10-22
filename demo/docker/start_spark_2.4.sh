#!/bin/bash

docker network create arangodb --subnet 172.28.0.0/16
docker run --network arangodb --ip 172.28.10.1 --name spark-master -h spark-master -e ENABLE_INIT_DAEMON=false -d bde2020/spark-master:2.4.5-hadoop2.7
docker run --network arangodb --ip 172.28.10.11 --name spark-worker-1 -h spark-worker-1 -e SPARK_WORKER_CORES=1 -e ENABLE_INIT_DAEMON=false -d bde2020/spark-worker:2.4.5-hadoop2.7
docker run --network arangodb --ip 172.28.10.12 --name spark-worker-2 -h spark-worker-2 -e SPARK_WORKER_CORES=1 -e ENABLE_INIT_DAEMON=false -d bde2020/spark-worker:2.4.5-hadoop2.7
docker run --network arangodb --ip 172.28.10.13 --name spark-worker-3 -h spark-worker-3 -e SPARK_WORKER_CORES=1 -e ENABLE_INIT_DAEMON=false -d bde2020/spark-worker:2.4.5-hadoop2.7
