# Build and test guide

Contents: [profiles and CI matrix](#profiles-and-ci-matrix),
[focused checks](#focused-checks), [database fixture](#database-fixture),
[JVM and SSL jobs](#jvm-and-ssl-jobs), [Spark patch compatibility](#spark-patch-compatibility),
[PySpark](#pyspark), [demo](#demo), [analysis and reports](#analysis-and-reports),
[test placement](#test-placement).

[.circleci/config.yml](../../../../.circleci/config.yml) is authoritative for job
parameters and commands; the recipes below reproduce those jobs locally, and
local-only narrowing is marked as such. Commands run from the repository root.

## Profiles and CI matrix

Use `mvn` (Maven 3.9+, no wrapper) with exactly one Spark and one Scala profile.
JDK 17 runs every pair. The `test-jdk` workflow covers:

| Spark profile | Scala profile | JDKs |
| --- | --- | --- |
| `spark-3.5` | `scala-2.12` | 8, 11, 17 |
| `spark-3.5` | `scala-2.13` | 8, 11, 17 |
| `spark-4.0` | `scala-2.13.18` | 17, 21 |
| `spark-4.1` | `scala-2.13.18` | 17, 21 |

`scala-2.13` (Scala 2.13.8, Java 8 target) and `scala-2.13.18` (Java 17 target)
are distinct profiles. The commands below use these variables:

```sh
spark=4.1
scala=2.13.18
```

All variants share `target/` directories. Run `mvn clean` with the new profiles
before switching Spark/Scala/JDK in one checkout (`bin/clean.sh` cleans all pairs).

## Focused checks

Local subsets, not CI jobs. Neither needs a database:

```sh
# Compile main and test sources of the selected reactor; runs no tests.
mvn test-compile -Pscala-"$scala" -Pspark-"$spark"
# Commons unit tests only.
mvn test -Pscala-"$scala" -Pspark-"$spark" -pl arangodb-spark-commons -am
```

With a non-TLS database fixture running, select integration tests:

```sh
mvn test -Pscala-"$scala" -Pspark-"$spark" -pl integration-tests -am \
  -Dtest='org.apache.spark.sql.arangodb.datasource.ReadTest' \
  -Dsurefire.failIfNoSpecifiedTests=false
```

Tests are JUnit Jupiter under Surefire (`-Dtest`, not Failsafe's `-Dit.test`).
The no-match flag is needed for upstream modules without matching tests, but it
also hides a mistyped selector: confirm the intended classes and parameterized
cases in fresh `integration-tests/target/surefire-reports/`.

## Database fixture

`docker/start_db.sh` needs a Docker daemon and a host that reaches the fixed
gateway `172.28.0.1`. It creates the `arangodb` network (`172.28.0.0/16`), the
`adb` and `arangodb-data` containers and starter-managed server containers, and
mounts the Docker socket. Run it only in a disposable environment. Before
restarting with another topology, TLS setting or image, remove only the
containers and network of that test deployment.

Environment variables, matching the CI `start-db` command:

- `STARTER_MODE`: `single` (script default) or `cluster`. The `test` job defaults
  to `cluster`; `integration-tests` uses `single`.
- `SSL`: `true` only for the `ssl` workflow and the `demo` job.
- `DOCKER_IMAGE`: `test-adb-version` uses `docker.io/arangodb/enterprise:3.12`
  and `docker.io/arangodb/core-preview:4-nightly`. The other jobs pass the
  pipeline `docker-img` parameter, which is usually empty, so the script's
  default `docker.io/arangodb/enterprise:latest` applies. Enterprise images may
  need `ARANGO_LICENSE_KEY`, which the script forwards.
- `COMPRESSION`: supported by the script, but no workflow enables it.

```sh
STARTER_MODE=cluster SSL=false ./docker/start_db.sh
```

Report the actual server version when you use a mutable tag. If you substitute
an image, say so; do not claim the original CI case was covered.

The fixture sets root's password to `test`. Tests create user `sparkUser`
(password `sparkTest`) and database `sparkConnectorTest`, then create, truncate
and drop collections. `BaseSparkTest` connects while the class initializes, so a
missing database fails every integration class. It reads
`-Darango.endpoints=host:port,...`, and on a single server uses only the first
endpoint. `SslTest` hardcodes its endpoint, pytest takes `--adb-hostname`, and
the demo reads `-Dendpoints` and its own TLS properties. Adapt the fixture to
nonstandard networking; do not change connector defaults for it.

## JVM and SSL jobs

The `test` job:

```sh
mvn dependency:tree -Pscala-"$scala" -Pspark-"$spark"
mvn test -Pscala-"$scala" -Pspark-"$spark"
```

Where the workflows run it:

- `test-adb-version`: each server image above, on single and cluster. Spark
  3.5 runs with Scala 2.13; Spark 4.0 and 4.1 with Scala 2.13.18.
- `test-adb-topology`: the same matrix, used instead of `test-adb-version` when
  a `docker-img` pipeline parameter is set.
- `test-jdk`: the JDK table above, on a non-TLS cluster.

Run the combinations your change affects, not the whole matrix.
`bin/test.sh` cleans and tests all four pairs with the current JDK and an
already running database. It covers no other CI dimension.

The `ssl` workflow runs the `test` job for all four pairs on JDK 17, against a
TLS cluster, with only `SslTest` selected:

```sh
mvn test -Pscala-"$scala" -Pspark-"$spark" -am -pl integration-tests \
  -Dtest=org.apache.spark.sql.arangodb.datasource.SslTest \
  -DSslTest=true -Dsurefire.failIfNoSpecifiedTests=false
```

`SslTest` is disabled unless `-DSslTest=true` is set, and it needs the TLS fixture.

## Spark patch compatibility

The `test-spark-versions` workflow runs the `integration-tests` job on JDK 17
against a non-TLS **single** server. It builds the connector with the profile's
default Spark version, then runs the test module alone against a different
Spark runtime:

| Spark profile | Scala profiles | `spark-full-version` |
| --- | --- | --- |
| `spark-3.5` | `scala-2.12`, `scala-2.13` | `3.5.0` to `3.5.7` |
| `spark-4.0` | `scala-2.13.18` | `4.0.0`, `4.0.1` |
| `spark-4.1` | `scala-2.13.18` | `4.1.0`, `4.1.1` |

```sh
spark_full=4.1.0
mvn install -Dmaven.test.skip=true -Dgpg.skip=true -Dmaven.javadoc.skip=true \
  -Pscala-"$scala" -Pspark-"$spark"
(
  cd integration-tests
  mvn dependency:tree -Pscala-"$scala" -Pspark-"$spark" -Dspark.version="$spark_full"
  mvn test -Pscala-"$scala" -Pspark-"$spark" -Dspark.version="$spark_full"
)
```

Keep the two stages separate. Do not add `-am` to the test stage, and do not
pass `-Dspark.version` to the install stage: either would compile the connector
against the test runtime, which defeats this check.

## PySpark

The `test-python` workflow's `python-integration-tests` job runs on Python 3.12
and 3.13 with JDK `17.0.17-tem` and Maven `3.9.12`, against a non-TLS cluster:

| Spark profile | Scala profile | PySpark |
| --- | --- | --- |
| `spark-3.5` | `scala-2.12` | `3.5.7` |
| `spark-4.0` | `scala-2.13.18` | `4.0.1` |
| `spark-4.1` | `scala-2.13.18` | `4.1.1` |

In an isolated Python environment (example: the 4.1 row):

```sh
python -m pip install "pyspark==4.1.1" -r python-integration-tests/test-requirements.txt
mvn package -Dmaven.test.skip=true -Dgpg.skip=true -Dmaven.javadoc.skip=true \
  -Pscala-"$scala" -Pspark-"$spark"
cp arangodb-spark-datasource-"$spark"/target/arangodb-spark-datasource-"$spark"_*-jar-with-dependencies.jar \
  ./arangodb-spark-datasource-under-test.jar
pytest python-integration-tests/integration \
  --adb-datasource-jar ./arangodb-spark-datasource-under-test.jar \
  --adb-hostname 172.28.0.1
```

Test the assembly (`jar-with-dependencies`), not the thin JAR. If `cp` finds
several matches, a stale Scala variant is still in `target/`: clean and rebuild.
No Maven job loads the assembly, so only this job covers it. Pass a pytest node
ID to run a subset.

## Demo

The `demo` job runs all four pairs on JDK 17 against a **TLS cluster**. The demo
is outside the root reactor and consumes the installed connector:

```sh
mvn install -Dmaven.test.skip=true -Dgpg.skip=true -Dmaven.javadoc.skip=true \
  -Pscala-"$scala" -Pspark-"$spark"
mvn -f ./demo/pom.xml -Pscala-"$scala" -Pspark-"$spark" -DimportPath=docker/import test
```

`demo/pom.xml` pins its own connector version and profiles; keep them in sync
with the root. The job runs only the Scala demo (`DemoTest`), not `python-demo/`.

## Analysis and reports

Scalastyle (`lib/scalastyle_config.xml`) checks main sources of commons and the
adapters at `process-sources` and fails on violations; it does not reformat.
Scapegoat runs during Scala compilation and ignores test sources. The adapted
`mapping/json/` sources are excluded from both (`// scalastyle:off` headers,
Scapegoat `ignoredFiles`) and from JaCoCo/Sonar; do not widen these exclusions.

The `sonar` job runs Spark 4.1 / Scala 2.13.18 on JDK 17 against a non-TLS
cluster, then publishes with the Sonar scanner. The local build and coverage
part, without publishing:

```sh
mvn -Pscala-2.13.18 -Pspark-4.1 -Dgpg.skip=true -B verify
```

This runs the tests and writes `integration-tests/target/site/jacoco-aggregate/`.
It gives no quality-gate result. After tests, CI's `report` step runs
`mvn surefire-report:report-only` with the same profiles and archives
`integration-tests/target/site`. `report-only` runs no tests.

The `deploy` job publishes releases and is not a validation step. Do not set
`-Ddeploy` locally: it drops `integration-tests` from the reactor.

For documentation-only changes, check links, paths and commands against the
POMs and CI config, and run `git diff --check`. No database run is needed.

## Test placement

| Change | Start from |
| --- | --- |
| Filter translation, column pruning, exception serialization | `arangodb-spark-commons/src/test/scala/` |
| Read results, mapping, configuration, filters, bad records, data types | `integration-tests/src/test/scala/org/apache/spark/sql/arangodb/datasource/` |
| Save modes, collection creation, edge schemas, nulls, retries, abort | the same package's `write/` directory |
| Packaged discovery, PySpark behavior | `python-integration-tests/integration/`, plus the demo for installed artifacts |

Follow the conventions of nearby tests (JUnit Jupiter with AssertJ, or pytest).
Reuse `BaseSparkTest.provideProtocolAndContentType` where it applies. It drops
VST on servers 3.12 and newer, so CI's current images never exercise VST. Python
tests cover HTTP and HTTP/2 with JSON and VelocyPack.

Some tests skip themselves or are disabled, so a green run does not cover them:

- Smart Edge tests need an Enterprise cluster.
- `DeserializationCastTest` skips some VelocyPack cases.
- `AcquireHostListTest` and `WriteResiliencyTest.retryOnTimeout` are disabled
  (manual only).

If you change code on those paths, add enabled, deterministic coverage or report
the gap.
