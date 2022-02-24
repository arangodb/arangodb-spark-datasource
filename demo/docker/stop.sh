#!/bin/bash

docker exec adb /app/arangodb stop
sleep 1
docker rm -f \
  adb \
  spark-master \
  spark-worker-1 \
  spark-worker-2 \
  spark-worker-3
