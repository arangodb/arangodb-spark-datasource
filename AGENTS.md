# ArangoDB Connector for Apache Spark

This repository implements a Scala/Spark DataSource V2 connector. Shared read,
write, configuration and AQL logic lives in `arangodb-spark-commons`; the
`arangodb-spark-datasource-*` modules adapt serialization to each Spark line.

For code, tests, dependencies or refactoring, use the
[spark-connector-development skill](.agents/skills/spark-connector-development/SKILL.md).
Read only the references relevant to the task. Agents without skill discovery
can open these Markdown files directly.

## Quick checks

Neither command needs a database (example profile pair):

```sh
mvn test-compile -Pscala-2.13.18 -Pspark-4.1                        # all modules, no tests
mvn test -Pscala-2.13.18 -Pspark-4.1 -pl arangodb-spark-commons -am  # commons unit tests
```

## Boundaries

- Keep shared behavior in commons and Spark-version-specific mapping in the
  adapters. Commons also uses Spark internals; it is not a Spark-independent
  library. Check affected adapters, without mechanically copying between them.
- Preserve connector option names/defaults, Spark SQL results and write semantics
  unless the task changes that contract. A pushdown must not discard matching
  rows; a retry must not introduce unsafe replays.
- Select one Spark profile and one compatible Scala profile for every Maven run.
  There are no default Spark/Scala profiles or Maven wrapper. Shared code must
  still build for the Scala 2.12 / Java 8 variant; Spark 4.x variants use Java 17
  targets. See the [profile matrix](.agents/skills/spark-connector-development/references/testing.md#profiles-and-ci-matrix).
- Keep Spark and Scala dependencies `provided`, and preserve service-provider
  resources in the packaged connector. Test packaging changes as a consumer,
  not only against reactor class directories.

## Validation and scope

Use Maven 3.9 or newer. Tests use **Surefire at `test`**, including the
`integration-tests` module in the normal root reactor. A root `mvn test` is not a
unit-only run. Use the [testing guide](.agents/skills/spark-connector-development/references/testing.md)
for database-free checks, fixtures and CircleCI job equivalents.

Database tests create users and mutate shared database/collection names. Use
only disposable test infrastructure, and do not run suites concurrently against
the same deployment. Deployment, release signing and remote Sonar publishing are
not prerequisites for local validation.

Follow nearby Scala/JUnit/pytest conventions and preserve upstream license
notices. Keep unrelated formatting, dependency upgrades, release versions and
generated files out of the change. For user-visible changes, add an entry under
`[Unreleased]` in `ChangeLog.md` and update affected examples (`demo/`,
`integration-tests/.../examples/`). The option reference is published outside
this repository (the README's Documentation link), so report any required
changes to it as follow-up.

Use source and POMs to verify implementation details, and `.circleci/config.yml`
for CI coverage; correct this guidance when it drifts. Finish with the behavior
changed, checks actually executed and their results, and relevant untested
variants or environmental blockers. Skipped or unselected tests are not passes.
