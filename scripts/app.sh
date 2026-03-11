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

need_cmd cargo
need_cmd npm
need_cmd curl

cleanup() {
  local exit_code=$?
  trap - EXIT INT TERM
  if [[ -n "${BACKEND_PID:-}" ]]; then
    kill "${BACKEND_PID}" >/dev/null 2>&1 || true
  fi
  wait >/dev/null 2>&1 || true
  exit "${exit_code}"
}

trap cleanup EXIT INT TERM

cd "${PROJECT_DIR}"

echo "[app] starting backend on http://127.0.0.1:8080"
cargo run -p dune-project-backend &
BACKEND_PID=$!

echo "[app] waiting for backend"
for _ in $(seq 1 30); do
  if curl -fsS "http://127.0.0.1:8080/health" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "${BACKEND_PID}" >/dev/null 2>&1; then
    echo "[app] backend exited during startup" >&2
    wait "${BACKEND_PID}"
  fi
  sleep 1
done

echo "[app] starting dashboard on http://127.0.0.1:5174"
cd "${PROJECT_DIR}/dashboard"
npm run dev -- --host 127.0.0.1 --port 5174 --strictPort
