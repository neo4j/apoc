# Architecture

## Top-level layout

```
.
├── build.gradle          ← shared config: Java 21, Spotless, license checks, test setup
├── settings.gradle       ← the subproject list
├── gradle.properties     ← apocVersion / neo4jVersion
├── processor/            ← annotation processor (META-INF service generation)
├── common/               ← shared APOC code, shaded into the core jar. Note this is shared with APOC extended, do not delete or change method signatures without checking if it breaks for them.
├── core/                 ← the APOC procedures; shadowJar → the plugin jar
├── test-utils/           ← shared test infrastructure
├── it/                   ← integration tests against a real Neo4j
├── hadoop/bin/           ← Windows winutils binaries for HDFS tests
├── licenses/             ← license header template, third-party license config
├── Taskfile.yaml         ← task entry points used by CI (set-version, publish)
├── agents/               ← Claude Code skill/subagent definitions
└── .github/workflows/    ← ChatOps only (spotlessApply, generateLicenses)
```

## Modules

APOC ("Awesome Procedures on Cypher") is a Neo4j plugin: a library of stored procedures and functions that runs inside the database. The Gradle subprojects in build order:

- `processor` — annotation processor (`apoc-processor`, built with JavaPoet). The other modules run it via their `apt` configuration to generate `META-INF/services` entries.
- `common` — shared procedure/utility code (`apoc.util`, `apoc.load`, `apoc.export`, …). Its `api` dependencies end up shaded into the core jar; Neo4j itself is `compileOnly` because the database provides it at runtime. This is called common because it is shared with APOC extended. Be careful about deletion and method changes.
- `core` — the actual procedures and functions. `:core:shadowJar` produces `core/build/libs/apoc-<version>-core.jar`, the jar that goes into `$NEO4J_HOME/plugins/`.
- `test-utils` — shared test infrastructure (Neo4j test support, the Neo4j Java driver, Testcontainers).
- `it` — integration tests. They run against the *built* plugin: `copyApocJar` copies the core shadow jar and passes its path as the `apoc-core.test.jar.path` system property. Runs single-forked and needs Docker.

`find` / `grep` is faster than enumerating classes here; this doc deliberately stops at the layout level.

## Branches

Upstream is `neo4j/apoc`, main branch is `dev`. There is one branch per Neo4j major version; fixes land on `dev` first and maintainers cherry-pick them back (see [`CONTRIBUTING.adoc`](../../CONTRIBUTING.adoc)).
There is also a master branch which does not contain runnable APOC code, but instead contains `versions.json`, a file used by Docker and Desktop to relate versions of APOC with Neo4j.

## Skills

`agents/skills/` holds versioned Claude Code skills, `agents/subagents/` holds subagent definitions. To activate one in this checkout: `/manage-skills load <skill-name>`. See `agents/README.md`.
