.PHONY: up down reset ps health logs

up:
	docker compose up -d --wait
	$(MAKE) topics
	$(MAKE) migrate

down:
	docker compose down

reset:
	docker compose down -v

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
