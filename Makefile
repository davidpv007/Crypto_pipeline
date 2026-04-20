.PHONY: up down logs test lint

# ── Docker ───────────────────────────────────────────────
up:
	docker compose -f infrastructure/docker-compose.yml up --build -d

down:
	docker compose -f infrastructure/docker-compose.yml down

logs:
	docker compose -f infrastructure/docker-compose.yml logs -f

# ── Quality ───────────────────────────────────────────────
test:
	pytest tests/

lint:
	ruff check src/ tests/
	black --check src/ tests/
	mypy src/