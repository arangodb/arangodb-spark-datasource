# Architecture and ownership

Paths below are repository-relative. Use this map to locate the implementation;
read the affected source rather than treating the map as an exhaustive contract.

## Modules and build boundary

| Path | Responsibility |
| --- | --- |
| `arangodb-spark-commons/` | DataSource V2 entry point, table, scans, writers, configuration, AQL translation, driver calls and mapping interfaces; pure tests in `src/test/scala`. |
| `arangodb-spark-datasource-3.5/`, `-4.0/`, `-4.1/` | Spark-specific JSON/VelocyPack parsers and generators, adapted Spark JSON code, service descriptors and `jar-with-dependencies` assembly. |
| `integration-tests/` | Shared JVM integration suite, selected against one adapter through Maven profiles. Included by the root's `no-deploy` profile unless the `deploy` property is set. |
| `python-integration-tests/` | PySpark/pytest consumer tests using the assembled connector JAR. |
| `demo/` | Standalone Maven consumer with its own profiles, Scala demos and `DemoTest`; also Python examples and Docker demo setup. Not a root reactor module. |
| `.circleci/config.yml`, `docker/start_db.sh`, `bin/` | CI job definitions, test database provisioning, and local profile-loop helpers. |

The root POM builds commons plus **one** adapter selected by `spark.compat.version`.
Even commons artifacts are Spark/Scala-versioned. Adapters depend on commons;
commons discovers their mapping implementations through interfaces and Java
`ServiceLoader`, not a compile-time dependency on an adapter.

## Entry and read path

The entry is `arangodb-spark-commons/src/main/scala/com/arangodb/spark/DefaultSource.scala`
(`TableProvider` and `DataSourceRegister`, short name `arangodb`). It parses
options and optionally acquires cluster endpoints, then creates `ArangoTable`.
The remaining shared code is under
`arangodb-spark-commons/src/main/scala/org/apache/spark/sql/arangodb/`:

```text
DefaultSource -> datasource/ArangoTable
  -> reader/ArangoScanBuilder -> ArangoScan (Scan + Batch)
  -> InputPartition + ArangoPartitionReaderFactory
  -> ArangoCollectionPartitionReader | ArangoQueryReader
  -> commons/ArangoClient -> ArangoDB cursor of RawBytes
  -> ArangoParserProvider -> FailureSafeParser -> InternalRow
```

Table/schema resolution and partition planning happen on the Spark driver.
`ArangoUtils.inferSchema` samples documents through `ArangoClient` and asks Spark's
JSON reader to infer a schema; a supplied schema bypasses that inference.
Factories/partition descriptions cross to executors, where readers create their
clients and cursors. Preserve this boundary when adding state or resources.

Collection mode partitions by shard and cycles configured endpoints. The client
handles single-server shard discovery fallback and Smart Edge collection shard
lookup. Query mode uses `SingletonPartition`: the user AQL is not partitioned or
rewritten for filter pushdown. `ArangoScanBuilder` returns partial/unsupported
filters to Spark, and `PushDownCtx` carries the required schema and applied
filters. `commons/filter/` classifies/translates filters; `PushdownUtils` builds
filter and projection expressions.

Both readers combine an adapter parser with Spark's `FailureSafeParser`, excluding
the corrupt-record field from document parsing. Their `close()` methods close the
cursor and client. Inspect acquisition, exhaustion, early termination and failure
paths together when changing resource handling.

## Write path

Under the same shared source root:

```text
ArangoTable -> writer/ArangoWriterBuilder -> ArangoBatchWriter
  -> ArangoDataWriterFactory -> ArangoDataWriter
  -> ArangoGeneratorProvider -> JSON/VelocyPack batch bytes
  -> ArangoClient.saveDocuments -> document API -> per-document error handling
```

The builder validates the schema/options and creates or truncates the collection
on the driver. Executors serialize and flush batches by row/byte limits.
`ArangoDataWriter` owns retry eligibility, endpoint rotation and accumulated
failures; exceptions in `commons/exceptions/` cross Spark task boundaries.
`ArangoBatchWriter.commit` is a no-op and abort is not a rollback: with `Append`
it throws `DataWriteAbortException` and leaves written documents in place, with
`Overwrite` it truncates the collection. `ArangoTable` advertises `BATCH_READ`,
`BATCH_WRITE`, `TRUNCATE` and `ACCEPT_ANY_SCHEMA`, and no streaming capability.
The `stream` option controls an AQL cursor, not Spark Structured Streaming.

## Mapping and configuration

`commons/ArangoDBConf.scala` registers options and exposes driver/read/write/mapping
views over case-insensitive settings. `ArangoTable` merges operation options over
table options. `ArangoDBDriverConf.builder()` configures the Java driver;
`ArangoClient` supplies the internal serde and raw request/response handling.

Each adapter's `src/main/scala/org/apache/spark/sql/arangodb/datasource/mapping/`
contains provider implementations and `mapping/json/` contains adapted Spark
sources. `mapping/package.scala` creates JSON options with UTC and the connector's
null-field setting. Provider implementations are registered under that adapter's
`src/main/resources/META-INF/services/`, alongside `DataSourceRegister`.

The root POM supplies the shaded Java driver, VelocyPack dependencies and a
Spark-line-specific Jackson BOM. Spark and Scala are supplied by the runtime.
The connector's fat JAR is a Maven assembly, not a separate connector-wide shading
scheme. Multiple adapter JARs share class/provider names and do not belong on one
consumer classpath.
