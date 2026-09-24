---
name: spark-connector-development
description: >-
  Develop and maintain arangodb-spark-datasource: implement features and bug fixes,
  refactor connector internals, update Spark/Scala or driver dependencies, change
  configuration, AQL pushdown, partitioning, serialization or writes, and add or
  run tests matching CircleCI. Use for work on this repository, not for writing
  an application that merely consumes the connector.
---

# Spark connector development

Follow [AGENTS.md](../../../AGENTS.md). Start from the affected behavior and its
nearest existing test; use the references below selectively.

| Task | Read |
| --- | --- |
| Find ownership or trace driver/executor data flow | [Architecture](references/architecture.md) |
| Change options, reads, pushdown or schema handling | [Read and configuration contracts](references/changes.md#configuration-and-reads) |
| Change serialization or Spark compatibility | [Mapping and version boundaries](references/changes.md#mapping-and-spark-versions) |
| Change writes, retries or cleanup | [Write contracts](references/changes.md#writes-and-failures) |
| Update dependencies or assembly | [Build and packaging](references/changes.md#dependencies-and-packaging) |
| Add tests, choose profiles or reproduce CI | [Testing](references/testing.md) |

## Work the change

Trace the relevant path through `DefaultSource`, the shared scan/write code and,
when applicable, the selected adapter. Identify the contract to preserve or
intentionally change. Reproduce a bug at that boundary; for a refactor, retain
observable behavior rather than relying on compilation alone.

Choose the implementation layer before editing. For shared changes, assess all
supported profile pairs; for an adapter change, compare the corresponding Spark
implementation and neighboring adapters before deciding which need the fix.
Do not turn version-specific differences into accidental shared assumptions.

Add regression coverage at the narrowest useful level. Start with a focused run,
then expand along the affected CI dimensions (Spark/Scala/JDK, server/topology,
protocol/content type, TLS or packaged consumers). Use the testing reference for
actual commands and fixture requirements, not conventions from the Java driver
or another Maven project.

Review the final diff for unintended behavior changes and stale guidance. Keep
architecture, examples and test instructions synchronized when their contracts
change; report validation and remaining gaps as required by AGENTS.md.
