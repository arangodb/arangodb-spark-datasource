# dev-README

## Development and agent guidance

Start with [AGENTS.md](AGENTS.md) and the
[development skill](.agents/skills/spark-connector-development/SKILL.md).
Load the [architecture map](.agents/skills/spark-connector-development/references/architecture.md)
or [change guide](.agents/skills/spark-connector-development/references/changes.md)
only for the area being changed.

## CircleCI

[.circleci/config.yml](.circleci/config.yml) defines the jobs and supported test
combinations. The [testing guide](.agents/skills/spark-connector-development/references/testing.md)
provides profile selection, database setup and local equivalents for those jobs.
Use one compatible Spark/Scala profile pair in all commands below.

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
- [arangodb-spark-datasource-3.5](arangodb-spark-datasource-3.5/target/scalastyle-output.xml)
- [arangodb-spark-datasource-4.0](arangodb-spark-datasource-4.0/target/scalastyle-output.xml)
- [arangodb-spark-datasource-4.1](arangodb-spark-datasource-4.1/target/scalastyle-output.xml)

### scapegoat
```shell
mvn -Pspark-${sparkVersion} -Pscala-${scalaVersion} test-compile
```
Reports:
- [arangodb-spark-commons](arangodb-spark-commons/target/scapegoat/scapegoat.html)
- [arangodb-spark-datasource-3.5](arangodb-spark-datasource-3.5/target/scapegoat/scapegoat.html)
- [arangodb-spark-datasource-4.0](arangodb-spark-datasource-4.0/target/scapegoat/scapegoat.html)
- [arangodb-spark-datasource-4.1](arangodb-spark-datasource-4.1/target/scapegoat/scapegoat.html)

### JaCoCo
```shell
mvn -Pspark-${sparkVersion} -Pscala-${scalaVersion} -Dgpg.skip=true verify
```
Report:
- [integration-tests](integration-tests/target/site/jacoco-aggregate/index.html)
