# dev-README

## GH Actions
Check results [here](https://github.com/arangodb/arangodb-spark-datasource/actions).

## SonarCloud
Check results [here](https://sonarcloud.io/project/overview?id=arangodb_arangodb-spark-datasource).

## check dependencies updates
```shell script
mvn -Pspark-${sparkVersion} -Pscala-${scalaVersion} versions:display-dependency-updates
```
