COMPOSE = docker compose
COMPOSE_PROD = docker compose -f docker-compose.prod.yaml

.PHONY: up up-build down restart logs ps build clean \
	up-prod up-build-prod down-prod restart-prod logs-prod ps-prod build-prod clean-prod prune-prod

## Local/dev environment (docker-compose.yaml)
up:
	$(COMPOSE) up -d

up-build:
	$(COMPOSE) up --build -d

down:
	$(COMPOSE) down

restart:
	$(COMPOSE) restart $(svc)

logs:
	$(COMPOSE) logs -f $(svc)

ps:
	$(COMPOSE) ps

build:
	$(COMPOSE) build $(svc)

clean:
	$(COMPOSE) down -v --remove-orphans

## Prod environment (docker-compose.prod.yaml)
up-prod:
	$(COMPOSE_PROD) up -d

up-build-prod:
	$(COMPOSE_PROD) up --build -d

down-prod:
	$(COMPOSE_PROD) down

restart-prod:
	$(COMPOSE_PROD) restart $(svc)

logs-prod:
	$(COMPOSE_PROD) logs -f $(svc)

ps-prod:
	$(COMPOSE_PROD) ps

build-prod:
	$(COMPOSE_PROD) build $(svc)

clean-prod:
	$(COMPOSE_PROD) down -v --remove-orphans

prune-prod:
	docker builder prune -af
	docker image prune -af
