.PHONY: up down reset ps health logs schemas schemas-check \
	apps-up apps-logs demo topics install check lint \
	produce-backfill produce-live bronze bronze-reset audit migrate sql \
	silver fatigue dlq-peek dlq-inspect test test-integration

up:
	docker compose up -d --wait
	$(MAKE) topics
	$(MAKE) migrate

down:
	docker compose --profile apps down

reset:
	docker compose --profile apps down -v
apps-up:
	docker compose --profile apps up -d --build

apps-logs:
	docker compose --profile apps logs -f bronze-sink silver-loader fatigue-windows

demo:
	$(MAKE) reset
	$(MAKE) up
	$(MAKE) schemas
	$(MAKE) apps-up
	$(MAKE) produce-backfill

ps:
	docker compose ps

health:
	docker compose exec redpanda rpk cluster health
	docker compose exec localstack awslocal s3 ls

logs:
	docker compose logs -f
topics:
	docker compose exec -T redpanda bash -s < infra/redpanda/create-topics.sh

install:
	python3.12 -m venv .venv && .venv/bin/pip install -e ".[dev]"

check:
	python -m telemetry.tools.check

lint:
	ruff check src tests && ruff format --check src tests

schemas:
	python -m telemetry.schemas.registry

schemas-check:
	python -m telemetry.schemas.registry --check

produce-backfill:
	python -m telemetry.producer.main backfill --athletes 20 --days 42 \
		--malformed-rate 0.02 --duplicate-rate 0.01 --late-rate 0.01

produce-live:
	python -m telemetry.producer.main live --athletes 5 --speed 60 \
		--malformed-rate 0.02 --duplicate-rate 0.01 --late-rate 0.02

bronze:
	python -m telemetry.consumers.bronze_sink.main

bronze-reset:   # stop the sink first
	docker compose exec localstack awslocal s3 rm s3://telemetry-bronze/ --recursive
	docker compose exec redpanda rpk group seek bronze-sink --to start
audit:
	python -m telemetry.tools.bronze_audit

migrate:
	for f in infra/clickhouse/init/*.sql; do \
		echo "applying $$f"; \
		docker compose exec -T clickhouse clickhouse-client --user telemetry --password telemetry --multiquery < $$f || exit 1; \
	done

sql:   # usage: make sql Q="SELECT 1"
	docker compose exec -T clickhouse clickhouse-client --user telemetry --password telemetry -d telemetry --format PrettyCompactMonoBlock -q "$(Q)"

silver:
	python -m telemetry.consumers.silver_loader.main
fatigue:
	python -m telemetry.processing.fatigue_windows.main
dlq-peek:
	docker compose exec redpanda rpk topic consume workout.dlq.v1 -n 3 -f '%h{%k: %v\\n}\\n%v\\n\\n'
dlq-inspect:
	python -m telemetry.dlq.inspect

test:
	pytest -q
test-integration:
	pytest -m integration -v
