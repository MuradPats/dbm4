.PHONY: help up airflow-up down clean pull-models reset logs

COMPOSE := docker compose

OLLAMA_MODEL     ?= qwen2.5:3b
POSTGRES_USER    ?= rico
POSTGRES_DB      ?= rico
MINIO_ACCESS_KEY ?= minioadmin
MINIO_SECRET_KEY ?= minioadmin
MINIO_BUCKET     ?= rico-raw

help:
	@echo "Targets:"
	@echo "  up           start Postgres+pgvector, MinIO, Ollama (waits until healthy)"
	@echo "  airflow-up   initialise and start Airflow webserver + scheduler"
	@echo "  pull-models  pull qwen2.5:3b into the Ollama container (run once)"
	@echo "  down         stop all services (volumes preserved)"
	@echo "  clean        stop all services and wipe volumes (full reset)"
	@echo "  reset        truncate pipeline tables + clear MinIO bucket"
	@echo "  logs         tail compose logs"

up:
	$(COMPOSE) up -d --wait postgres minio ollama
	$(COMPOSE) up -d minio-init ollama-init

airflow-up:
	$(COMPOSE) run --rm airflow-init
	$(COMPOSE) up -d --wait airflow-webserver airflow-scheduler

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v

pull-models:
	$(COMPOSE) exec ollama ollama pull $(OLLAMA_MODEL)

# Wipe lab data without re-pulling Ollama or rebuilding volumes.
# Use this between notebook re-runs (after `Kernel → Restart`).
reset:
	$(COMPOSE) exec postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c \
	  "TRUNCATE TABLE pipeline_metrics, audit_results, screens_eval, screens_review_queue, screens_embeddings, screens_metadata, pipeline_runs RESTART IDENTITY CASCADE;"
	$(COMPOSE) exec minio mc alias set local http://minio:9000 $(MINIO_ACCESS_KEY) $(MINIO_SECRET_KEY) >/dev/null
	$(COMPOSE) exec minio mc rm --recursive --force local/$(MINIO_BUCKET)/ >/dev/null 2>&1 || true
	@echo "pipeline state reset"

logs:
	$(COMPOSE) logs -f --tail=100
