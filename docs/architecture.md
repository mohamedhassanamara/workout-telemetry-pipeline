# Workout telemetry pipeline

This repository is a scaffold. Pipeline behavior and infrastructure are not yet implemented.

## Intended components

- `src/telemetry/producer/`: simulate and publish workout events, including chaos scenarios.
- `src/telemetry/schemas/`: event contracts and schema registry helpers.
- `schemas/`: exported JSON Schema contracts.
- `src/telemetry/consumers/bronze_sink/`: batch raw events into S3 objects.
- `src/telemetry/consumers/silver_loader/`: transform events into ClickHouse rows.
- `src/telemetry/processing/fatigue_windows/`: windowed fatigue processing.
- `src/telemetry/dlq/`: publish, inspect, and replay dead-letter events.
- `src/telemetry/tools/`: operational checks and bronze audits.
- `infra/`: LocalStack bucket bootstrap, Redpanda topic definitions, and ClickHouse silver tables / gold views.
- `tests/unit/`: package-import smoke test and future unit tests.
- `tests/integration/`: reserved for future integration tests.

## Development

Requires Python 3.11 or newer.

```sh
python3 -m venv .venv
. .venv/bin/activate
make install
make check
make test
```

`make check` validates Python syntax. `make test` discovers standard-library
`unittest` tests in `tests/unit/`, including a package-import smoke test. The
integration suite is not wired into CI until integration tests are added.
Docker Compose currently has no services, and the Dockerfile only builds the
package scaffold.
