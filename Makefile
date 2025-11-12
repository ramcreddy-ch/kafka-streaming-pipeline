.PHONY: help up down ps logs topic

help:
	@echo "Kafka Streaming Pipeline CLI"
	@echo "  up    - Start full Confluent stack"
	@echo "  down  - Stop and remove containers"
	@echo "  ps    - Check status"
	@echo "  logs  - Follow broker logs"
	@echo "  topic - Create default topics"

up:
	docker compose up -d

down:
	docker compose down -v

ps:
	docker compose ps

logs:
	docker compose logs -f broker

topic:
	docker exec broker kafka-topics --create --topic telemetry-events --bootstrap-server localhost:9092 --partitions 3
