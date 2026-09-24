# Change guide

Read the sections relevant to the change. Shared class names refer to
`arangodb-spark-commons/src/main/scala/org/apache/spark/sql/arangodb/`;
adapter mapping paths are described in the architecture reference.

## Configuration and reads

Add options through the constant/`ConfigEntry`, `confEntries` registration,
appropriate `ArangoDB*Conf` view and actual consumer in `commons/ArangoDBConf.scala`
and related code. Preserve case-insensitive lookup, operation-over-table
precedence, defaults and validation. Read mode currently prefers `query` when
both `query` and `table` are present. Test the intended behavior instead of
silently introducing mutual exclusion or changing precedence. Keep descriptions
and examples consistent with the accepted key, not obsolete spellings in an
error message. Avoid logging credentials or whole option maps.

Treat filter pushdown as a correctness boundary, not merely an AQL optimization.
A `FULL` filter must match Spark's result semantics; a `PARTIAL` filter must admit
all matching rows and remain a Spark residual. Follow `FilterSupport` composition
for AND/OR/NOT; negating an approximation is not generally safe. Timestamp
comparisons deliberately account for AQL's loss of sub-millisecond precision.
Cover null/missing values, nested or quoted field names, literal escaping and
compound predicates when relevant. Existing literal interpolation is not proof
that arbitrary values are safely escaped.

Change `ArangoScanBuilder`, `PushDownCtx`, `PushdownUtils` and the reader/client
boundary together where needed. Keep columns needed for residual evaluation and
the corrupt-record field semantics. Do not apply collection-query rewriting to
user AQL without designing that as a separate behavior change. Check collection
and query reads, inferred and supplied schemas, and single/cluster partitioning
as applicable. Use both translation unit tests and result-level integration tests
for a changed pushdown; valid AQL alone does not prove equivalent Spark results.

## Mapping and Spark versions

Keep common interfaces in `commons/mapping/` and their implementations in the
adapter. Preserve `META-INF/services` entries when moving/renaming providers.
`ServiceLoader` currently takes the first provider; do not rely on it to choose
among several incompatible adapters.

`mapping/json/` is maintained Spark-derived source, not generated output. Compare
against the matching upstream Spark line, preserve license notices and retain
connector-specific JSON/VelocyPack modifications. Do not wholesale synchronize
adapters or replace the code with Spark public JSON APIs without checking the
mapping contracts. The `org.apache.spark.sql.arangodb` namespace gives access to
Spark's package-scoped internals; relocating it is not cosmetic cleanup.

Test the formats and types the change touches: JSON and VelocyPack differ, and
the writer currently rejects JSON `DecimalType` schemas. Check nested values,
nullability, `ignoreNullFields`, date/timestamp precision and UTC behavior,
parse modes and corrupt-record text where applicable. Do not infer serialized
bytes or supported types from one format's success. Distinguish schema inference
(`ArangoUtils` and Spark's JSON reader) from executor-side adapter parsing.

## Writes and failures

Distinguish Spark `SaveMode` from document `overwriteMode`. Preserve the
`confirmTruncate` guard and edge-schema validation in `ArangoWriterBuilder` unless
explicitly changing those contracts. Check the builder, task writer and batch
abort behavior together; retries, speculative tasks and batch commits do not
provide an all-or-nothing or exactly-once transaction.

Use `ArangoDataWriter.canRetry` and `isConnectionException` as the current replay
policy. A non-nullable `_key` and the overwrite/`keepNull` settings matter for
idempotency; connection establishment failures have a separate retry path.
Do not broaden retries as incidental error-handling cleanup. Test partial batch
success, exhaustion, final flush, endpoint failover and abort where affected,
and preserve per-document errors and serializable exception causes.

Keep clients/cursors/generators executor-local when owned by a task, and close
resources on normal and exceptional paths without closing another owner's
resources. Check both readers when changing their shared lifecycle, and preserve
AQL warning delivery for streaming versus non-streaming cursors.

## Dependencies and packaging

Inspect the root, adapter and `integration-tests` POMs; the test serializer modules
have explicit versions of their own. `demo/pom.xml` independently declares
profiles and the connector dependency. Coordinate these only as required by the
upgrade; do not assume changing the root updates the standalone demo.

Retain explicit dependency scopes, Spark/Scala `provided` scope, Jackson BOMs and
VelocyPack exclusions. Commons must compile against every supported profile pair,
including Scala 2.12 and Java 8; do not introduce newer APIs there solely because
the default CI executor is JDK 17. Validate adapter internals against the patch
versions CI exercises, not only the root POM's default Spark patch.

For assembly or discovery changes, exercise the assembled JAR via PySpark and/or
the installed artifact via the demo as appropriate. Edit source POMs, not
`.flattened-pom.xml` or `target/` output. When adding a Spark line, coordinate its
adapter and services, root and demo profiles/dependencies, `.circleci/config.yml`
and `bin/test.sh` / `bin/clean.sh`; do not advertise compatibility from compilation
alone.
