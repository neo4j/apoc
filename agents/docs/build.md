# Build System

## Wrapper

Always use the Gradle Wrapper: `./gradlew` (Linux/macOS) or `.\gradlew.bat` (Windows). Don't rely on a system Gradle — the wrapper pins the version (8.13). Java 21 is required.

## Taskfile for release tasks

[Taskfile](https://taskfile.dev) (`Taskfile.yaml`) defines the entry points CI uses for versioning and publishing:

- `task set-version apoc_version=<v> neo4j_version=<v>` — update `gradle.properties` and `readme.adoc` (wraps `./gradlew setVersion`).
- `task publish neo4j_version=<v>` — build and publish artifacts.

Use these rather than the underlying Gradle invocations so agents, humans, and CI stay in sync. For everyday builds and tests there is no task — call `./gradlew` directly.

## Topology

- `settings.gradle` includes `processor`, `common`, `core`, `test-utils`, `it`. `benchmark` is commented out.
- Versions live in `gradle.properties` (`apocVersion`, `neo4jVersion`); `-Pneo4jVersionOverride` overrides the Neo4j version for a build.
- Shared configuration is in the root `build.gradle`: Java 21, Spotless, dependency constraints (mostly CVE-driven version floors), and the JUnit 5 test setup.

## Gradle recipes

```shell
# build the plugin jar → core/build/libs/apoc-<version>-core.jar
./gradlew :core:shadowJar

# full build including tests
./gradlew build

# all unit tests, or one module's
./gradlew test
./gradlew :core:test

# single test class / method
./gradlew :core:test --tests=ClassName
./gradlew :core:test --tests=ClassName#methodName

# integration tests (needs Docker — Testcontainers runs Neo4j images)
./gradlew :it:test

# format / format check
./gradlew spotlessApply
./gradlew spotlessCheck
```

## Capturing output

Builds and test runs are long and noisy. Pass `-q` to trim output to `[ERROR]` lines plus the build/test summary — prefer that over redirecting to a log file and grepping it (a redirect makes the command a compound that needs separate approvals).

## Properties worth knowing

- `-Pneo4jVersionOverride=<version>` — build/test against a different Neo4j version than `gradle.properties`.
- `-Pneo4jDockerEeOverride` / `-Pneo4jDockerCeOverride` — override the Neo4j Docker images used by Testcontainers in tests (default: `neo4j:<neo4jVersion>-enterprise-debian` / `neo4j:<neo4jVersion>-debian`).
- `-PtestDockerBundle` — enable the test Docker bundle.
- `-x test` — skip tests for a task invocation (e.g. `./gradlew build -x test`).
- Tests are forked JVMs: heap up to 5G, one fork per two CPU cores, re-forked every 50 classes. Expect real resource usage.

## Spotless

Spotless wraps Palantir Java Format (Java, also removes unused imports) and greclipse (`*.gradle` files, config in `greclipse.properties`). **Don't hand-format** — run `spotlessApply`. Commenting `/spotlessApply` on a PR triggers a bot that applies and pushes the formatting.

## License headers

Every `*.java` file must carry the header in `licenses/headers/source-header.txt`. The `license` task inserts/fixes headers and is wired into `check`, so a plain `./gradlew build` repairs them as a side effect; `licenseTest` checks without modifying. Third-party licenses are checked via `downloadLicenses` and `licenses-3rdparties.gradle`; `/generateLicenses` on a PR regenerates the report.

## CI

The real build/test CI runs on TeamCity, not GitHub Actions — `teamcity-repository.gradle` is applied when the `CI` env var is set, and on TeamCity test failures may be ignored (muted tests). GitHub Actions hosts only the ChatOps commands above.
