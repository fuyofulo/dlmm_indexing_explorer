#!/usr/bin/env bash
set -euo pipefail

API_BASE="${API_BASE:-http://127.0.0.1:8080}"
POOL="${POOL:-}"

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: required command not found: $1" >&2
    exit 1
  fi
}

need_cmd curl

CURL_ARGS=(-sS)

echo "[smoke] health"
HEALTH="$(curl -sS "${API_BASE}/health")"
echo "$HEALTH"
echo "$HEALTH" | grep -q '"status":"ok"'

echo "[smoke] lag"
LAG="$(curl "${CURL_ARGS[@]}" "${API_BASE}/v1/ingestion/lag")"
echo "$LAG"
echo "$LAG" | grep -q '"now_unix_ms"'

echo "[smoke] top pools"
TOP="$(curl "${CURL_ARGS[@]}" "${API_BASE}/v1/pools/top?minutes=180&limit=5")"
echo "$TOP"
echo "$TOP" | grep -q '"items"'

if [[ -z "$POOL" ]]; then
  if command -v jq >/dev/null 2>&1; then
    POOL="$(echo "$TOP" | jq -r '.items[0].pool // empty')"
  else
    POOL="$(echo "$TOP" | sed -n 's/.*"pool":"\([^"]*\)".*/\1/p' | head -n1)"
  fi
fi

if [[ -n "$POOL" ]]; then
  echo "[smoke] pool summary"
  SUMMARY="$(curl "${CURL_ARGS[@]}" "${API_BASE}/v1/pools/${POOL}/summary?minutes=180")"
  echo "$SUMMARY"
  echo "$SUMMARY" | grep -q '"pool_activity"'
fi

echo "[smoke] quality latest"
QL="$(curl "${CURL_ARGS[@]}" "${API_BASE}/v1/quality/latest")"
echo "$QL"
echo "$QL" | grep -q '"item"'

echo "[smoke] quality window"
QW="$(curl "${CURL_ARGS[@]}" "${API_BASE}/v1/quality/window?minutes=180")"
echo "$QW"
echo "$QW" | grep -q '"totals"'

echo "[smoke] csv export"
TMP_CSV="/tmp/dune_project_smoke_$(date +%s).csv"
if [[ -n "$POOL" ]]; then
  curl "${CURL_ARGS[@]}" -o "$TMP_CSV" "${API_BASE}/v1/export/events.csv?pool=${POOL}&limit=50"
else
  curl "${CURL_ARGS[@]}" -o "$TMP_CSV" "${API_BASE}/v1/export/events.csv?limit=50"
fi
head -n 1 "$TMP_CSV" | grep -q 'slot,signature,instruction_index'

echo "[smoke] success"
