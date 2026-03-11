#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${PROJECT_DIR}"

docker exec -i dune-project-clickhouse clickhouse-client \
  --user dune_project --password dune_project_pass --multiquery \
  < schema/clickhouse_v2.sql
