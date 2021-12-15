#!/bin/bash

LOCATION=$(pwd)/$(dirname "$0")

curl -u root:test http://172.17.0.1:8529/_api/collection -d '{"name": "users", "numberOfShards": 6}'
docker run --rm -v "$LOCATION"/import:/import arangodb \
  arangoimport --server.endpoint=http+tcp://172.17.0.1:8529 --server.password=test \
  --file "/import/users/users.json" --type json --collection "users"
