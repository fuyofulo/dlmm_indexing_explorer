#!/usr/bin/env bash
set -euo pipefail

API_BASE="${API_BASE:-http://127.0.0.1:8080}"
MINUTES="${MINUTES:-60}"
CSV_LIMIT="${CSV_LIMIT:-500}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DEFAULT_CSV_OUT="${PROJECT_DIR}/exports/dlmm_events_$(date +%Y%m%d_%H%M%S).csv"
CSV_OUT="${CSV_OUT:-$DEFAULT_CSV_OUT}"
POOL="${POOL:-}"

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: required command not found: $1" >&2
    exit 1
  fi
}

need_cmd curl
mkdir -p "$(dirname "$CSV_OUT")"

CURL_ARGS=(-sS)

echo "== API health =="
curl -sS "${API_BASE}/health"
echo

echo "== Ingestion lag =="
curl "${CURL_ARGS[@]}" "${API_BASE}/v1/ingestion/lag"
echo

echo "== Top pools (${MINUTES}m, 5) =="
TOP_JSON="$(curl "${CURL_ARGS[@]}" "${API_BASE}/v1/pools/top?minutes=${MINUTES}&limit=5")"
echo "$TOP_JSON"
echo

if [[ -z "$POOL" ]]; then
  if command -v jq >/dev/null 2>&1; then
    POOL="$(echo "$TOP_JSON" | jq -r '.items[0].pool // empty')"
  else
    POOL="$(echo "$TOP_JSON" | sed -n 's/.*"pool":"\([^"]*\)".*/\1/p' | head -n1)"
  fi
fi

if [[ -z "$POOL" ]]; then
  echo "warning: could not infer pool from top pools; exporting CSV without pool filter" >&2

  echo "== Export CSV (all pools, limit=${CSV_LIMIT}) =="
  curl "${CURL_ARGS[@]}" -o "$CSV_OUT" "${API_BASE}/v1/export/events.csv?limit=${CSV_LIMIT}"
  echo "wrote: $CSV_OUT"
  echo "preview:"
  head -n 5 "$CSV_OUT" || true
  echo
else
  echo "== Pool summary (${POOL}) =="
  curl "${CURL_ARGS[@]}" "${API_BASE}/v1/pools/${POOL}/summary?minutes=${MINUTES}"
  echo

  echo "== Pool events sample (${POOL}) =="
  curl "${CURL_ARGS[@]}" "${API_BASE}/v1/pools/${POOL}/events?limit=5"
  echo

  echo "== Export CSV (${POOL}, limit=${CSV_LIMIT}) =="
  curl "${CURL_ARGS[@]}" -o "$CSV_OUT" "${API_BASE}/v1/export/events.csv?pool=${POOL}&limit=${CSV_LIMIT}"
  echo "wrote: $CSV_OUT"
  echo "preview:"
  head -n 5 "$CSV_OUT" || true
  echo
fi

echo "== Quality latest =="
curl "${CURL_ARGS[@]}" "${API_BASE}/v1/quality/latest"
echo

echo "== Quality window (${MINUTES}m) =="
curl "${CURL_ARGS[@]}" "${API_BASE}/v1/quality/window?minutes=${MINUTES}"
echo

echo "demo complete"
