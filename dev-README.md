# dev-README

## GH Actions
Check results [here](https://github.com/arangodb/arangodb-spark-datasource/actions).

## SonarCloud
Check results [here](https://sonarcloud.io/project/overview?id=arangodb_arangodb-spark-datasource).

## check dependencies updates
```shell
mvn -Pspark-${sparkVersion} -Pscala-${scalaVersion} versions:display-dependency-updates
```

## analysis tools

### scalastyle
```shell
mvn -Pspark-${sparkVersion} -Pscala-${scalaVersion} process-sources
```
Reports:
- [arangodb-spark-commons](arangodb-spark-commons/target/scalastyle-output.xml)
- [arangodb-spark-datasource-2.4](arangodb-spark-datasource-2.4/target/scalastyle-output.xml)
- [arangodb-spark-datasource-3.1](arangodb-spark-datasource-3.1/target/scalastyle-output.xml)

### scapegoat
```shell
mvn -Pspark-${sparkVersion} -Pscala-${scalaVersion} test-compile
```
Reports:
- [arangodb-spark-commons](arangodb-spark-commons/target/scapegoat/scapegoat.html)
- [arangodb-spark-datasource-2.4](arangodb-spark-datasource-2.4/target/scapegoat/scapegoat.html)
- [arangodb-spark-datasource-3.1](arangodb-spark-datasource-3.1/target/scapegoat/scapegoat.html)

### JaCoCo
```shell
mvn -Pspark-${sparkVersion} -Pscala-${scalaVersion} verify
```
Report:
- [integration-tests](integration-tests/target/site/jacoco-aggregate/index.html)
