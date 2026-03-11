#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: required command not found: $1" >&2
    exit 1
  fi
}

need_cmd docker
need_cmd cargo
need_cmd npm
need_cmd curl
need_cmd lsof

cleanup() {
  local exit_code=$?
  trap - EXIT INT TERM
  kill_tree "${INDEXER_PID:-}"
  kill_tree "${BACKEND_PID:-}"
  kill_tree "${DASHBOARD_PID:-}"
  wait >/dev/null 2>&1 || true
  exit "$exit_code"
}

kill_tree() {
  local pid="${1:-}"
  if [[ -z "$pid" ]]; then
    return
  fi
  if ! kill -0 "$pid" >/dev/null 2>&1; then
    return
  fi

  local child
  for child in $(pgrep -P "$pid" 2>/dev/null || true); do
    kill_tree "$child"
  done

  kill "$pid" >/dev/null 2>&1 || true
}

find_free_port() {
  local port="${1}"
  while lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1; do
    port=$((port + 1))
  done
  echo "${port}"
}

trap cleanup EXIT INT TERM

cd "$PROJECT_DIR"

echo "[dev] starting local infra"
docker compose -f docker-compose.clickhouse.yml up -d

echo "[dev] waiting for ClickHouse"
for _ in $(seq 1 30); do
  if curl -fsS "http://127.0.0.1:8123/?user=dune_project&password=dune_project_pass&query=SELECT%201" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

echo "[dev] applying schema"
./scripts/schema_apply.sh

if [[ ! -d dashboard/node_modules ]]; then
  echo "[dev] installing dashboard dependencies"
  (cd dashboard && npm install)
fi

echo "[dev] starting backend on http://127.0.0.1:8080"
cargo run -p dune-project-backend &
BACKEND_PID=$!

echo "[dev] waiting for backend"
for _ in $(seq 1 30); do
  if curl -fsS "http://127.0.0.1:8080/health" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "${BACKEND_PID}" >/dev/null 2>&1; then
    echo "[dev] backend exited during startup" >&2
    wait "${BACKEND_PID}"
  fi
  sleep 1
done

echo "[dev] starting indexer"
INDEXER_TUI=0 INDEXER_PLAIN_LOGS=0 cargo run -q -p indexer &
INDEXER_PID=$!

DASHBOARD_PORT="$(find_free_port 5174)"
echo "[dev] starting dashboard on http://127.0.0.1:${DASHBOARD_PORT}"
(cd dashboard && npm run dev -- --host 127.0.0.1 --port "${DASHBOARD_PORT}" --strictPort) &
DASHBOARD_PID=$!

echo "[dev] stack is booting"
echo "[dev] dashboard: http://127.0.0.1:${DASHBOARD_PORT}"
echo "[dev] backend:   http://127.0.0.1:8080"
echo "[dev] press Ctrl-C to stop local processes"

while true; do
  if ! kill -0 "${BACKEND_PID}" >/dev/null 2>&1; then
    wait "${BACKEND_PID}"
    break
  fi
  if ! kill -0 "${INDEXER_PID}" >/dev/null 2>&1; then
    wait "${INDEXER_PID}"
    break
  fi
  if ! kill -0 "${DASHBOARD_PID}" >/dev/null 2>&1; then
    wait "${DASHBOARD_PID}"
    break
  fi
  sleep 1
done

echo "[dev] one of the processes exited; shutting down"
