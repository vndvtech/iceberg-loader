#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXAMPLES_DIR="$ROOT_DIR/examples"

cd "$EXAMPLES_DIR"

echo "Starting local example stack..."
docker compose up -d

echo "Waiting for Trino to become healthy..."
for _ in {1..60}; do
  status="$(docker inspect --format '{{.State.Health.Status}}' examples-trino-1 2>/dev/null || true)"
  if [[ "$status" == "healthy" ]]; then
    break
  fi
  sleep 2
done

status="$(docker inspect --format '{{.State.Health.Status}}' examples-trino-1 2>/dev/null || true)"
if [[ "$status" != "healthy" ]]; then
  echo "Trino did not become healthy in time"
  docker compose ps
  exit 1
fi

examples=(
  "load_upsert.py"
  "load_complex_json.py"
  "compare_complex_json_fail.py"
  "load_with_commits.py"
  "advanced_scenarios.py"
  "load_timestamp_partitioning.py"
  "maintenance_example.py"
)

for example in "${examples[@]}"; do
  echo "Running ${example}..."
  uv run python "$example"
done

echo "Example smoke suite completed successfully."
