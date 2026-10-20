#!/usr/bin/env bash
# runs loadgen against the compose stack at each concurrency level
set -euo pipefail
cd "$(dirname "$0")/.."
levels=${LEVELS:-"8 32 128"}
duration=${DURATION:-20s}

docker compose up -d --build --wait postgres payd >/dev/null
trap 'docker compose down -v >/dev/null 2>&1' EXIT
until curl -so /dev/null http://localhost:8080/payments; do sleep 0.2; done
docker compose exec -T postgres psql -U payd -d payd -qc "INSERT INTO api_keys VALUES (sha256('sk_bench'), 'm_bench')"
echo "ACQUIRER_LATENCY=${ACQUIRER_LATENCY:-0s} PAYD_POOL_MAX_CONNS=${PAYD_POOL_MAX_CONNS:-32} $(date -u +%F) $(nproc) cpus"
for c in $levels; do
	go run ./cmd/loadgen -url http://localhost:8080 -key sk_bench -c "$c" -d "$duration" -warmup 3s
done
