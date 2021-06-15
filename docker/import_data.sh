#!/bin/bash

# exit when any command fails
set -e

CONTAINER_NAME=$1

## create test db
curl -u root:test http://172.28.3.1:8529/_api/database -d '{"name":"sparkConnectorTest"}'


## import users sample data
curl -u root:test http://172.28.3.1:8529/_db/sparkConnectorTest/_api/collection -d '{"name": "users", "numberOfShards": 6}'
docker exec "$CONTAINER_NAME" arangoimport --server.username=root --server.password=test --server.database sparkConnectorTest --file "/import/users/users.json" --type json --collection "users"

## import IMDB sample data
curl -u root:test http://172.28.3.1:8529/_db/sparkConnectorTest/_api/gharial -d '
{
  "name": "imdb",
  "edgeDefinitions": [
    {
      "collection": "actsIn",
      "from": ["persons"],
      "to": ["movies"]
    },
    {
      "collection": "directed",
      "from": ["persons"],
      "to": ["movies"]
    }
  ],
  "options": {
    "numberOfShards": 9
  }
}'

docker exec "$CONTAINER_NAME" arangoimport --server.username=root --server.password=test --server.database sparkConnectorTest --file "/import/imdb/persons.json" --type json --collection "persons"
docker exec "$CONTAINER_NAME" arangoimport --server.username=root --server.password=test --server.database sparkConnectorTest --file "/import/imdb/movies.json" --type json --collection "movies"
docker exec "$CONTAINER_NAME" arangoimport --server.username=root --server.password=test --server.database sparkConnectorTest --file "/import/imdb/actsIn.json" --type json --collection "actsIn"
docker exec "$CONTAINER_NAME" arangoimport --server.username=root --server.password=test --server.database sparkConnectorTest --file "/import/imdb/directed.json" --type json --collection "directed"

echo "Done, data imported."
