.PHONY: up down reset ps health logs

up:
	docker compose up -d --wait
	$(MAKE) topics

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
