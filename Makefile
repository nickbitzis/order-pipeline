.PHONY: install test lint fmt run demo chaos api relay payments inventory docker up down clean

install:
	uv sync --python 3.12

test:
	uv run pytest -q

lint:
	uv run ruff check .

fmt:
	uv run ruff check --fix .

demo run:
	uv run orderpipeline demo --fresh

chaos:
	uv run python scripts/chaos_demo.py --orders 500 --seed 42

api:
	uv run orderpipeline api --port 8000

relay payments inventory:
	uv run orderpipeline $@

docker:
	docker build -t orderpipeline:local .

up:
	docker compose up --build -d

down:
	docker compose down

clean:
	rm -rf data .pytest_cache .ruff_cache
