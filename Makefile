.PHONY: up down schema reset-db dev app indexer run-backend dashboard-install dashboard-dev dashboard-build demo smoke check test fmt clippy

up:
	docker compose -f docker-compose.clickhouse.yml up -d

down:
	docker compose -f docker-compose.clickhouse.yml down

schema:
	./scripts/schema_apply.sh

reset-db:
	./scripts/db_reset.sh

dev:
	./scripts/dev.sh

indexer:
	cargo run -p indexer

run-backend:
	cargo run -p dune-project-backend

app:
	./scripts/app.sh

dashboard-install:
	cd dashboard && npm install

dashboard-dev:
	cd dashboard && npm run dev

dashboard-build:
	cd dashboard && npm run build

demo:
	./scripts/demo.sh

smoke:
	./scripts/smoke.sh

fmt:
	cargo fmt --all

clippy:
	cargo clippy --workspace --all-targets

check:
	cargo check --workspace

test:
	cargo test --workspace
