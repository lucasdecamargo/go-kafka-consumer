#!/usr/bin/env bash
# Run Layer 2 end-to-end benchmarks against the Docker Compose Kafka cluster.
#
# Prerequisites:
#   docker compose -f benchmarks/docker-compose.bench.yml up -d --wait
#
# Usage:
#   ./benchmarks/scripts/run_e2e.sh           # 3 runs, start cluster
#   ./benchmarks/scripts/run_e2e.sh 1         # 1 run
#   ./benchmarks/scripts/run_e2e.sh 3 skip    # 3 runs, skip cluster start
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

COUNT="${1:-3}"
SKIP_CLUSTER="${2:-}"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
OUTFILE="benchmarks/results/e2e_${TIMESTAMP}.txt"
COMPOSE_FILE="benchmarks/docker-compose.bench.yml"

# Start the cluster if not skipped.
if [ "${SKIP_CLUSTER}" != "skip" ]; then
    echo "=== Starting 3-broker Kafka cluster ==="
    docker compose -f "${COMPOSE_FILE}" up -d
    echo "Waiting 15s for cluster to stabilize..."
    sleep 15
fi

echo ""
echo "=== Layer 2: E2E Benchmarks ==="
echo "  Runs:    ${COUNT}"
echo "  Output:  ${OUTFILE}"
echo ""

go test \
    -tags e2ebench \
    -bench=. \
    -benchmem \
    -count="${COUNT}" \
    -timeout=30m \
    -run='^$' \
    ./benchmarks/e2e/ \
    | tee "${OUTFILE}"

echo ""
echo "Results saved to ${OUTFILE}"
echo "Compare with: benchstat ${OUTFILE} <other_file>"
