#!/usr/bin/env bash
# Run all Layer 1 microbenchmarks and produce benchstat-compatible output.
#
# Usage:
#   ./benchmarks/scripts/run_micro.sh           # 5 runs, save to results/
#   ./benchmarks/scripts/run_micro.sh 3         # 3 runs
#   ./benchmarks/scripts/run_micro.sh 1 short   # 1 run, short timeout
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

COUNT="${1:-5}"
TIMEOUT="${2:-10m}"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
OUTFILE="benchmarks/results/micro_${TIMESTAMP}.txt"

echo "=== Layer 1: Microbenchmarks ==="
echo "  Runs:    ${COUNT}"
echo "  Timeout: ${TIMEOUT}"
echo "  Output:  ${OUTFILE}"
echo ""

go test \
    -bench=. \
    -benchmem \
    -count="${COUNT}" \
    -timeout="${TIMEOUT}" \
    -run='^$' \
    ./internal/dispatcher/ \
    ./internal/offset/ \
    | tee "${OUTFILE}"

echo ""
echo "Results saved to ${OUTFILE}"
echo "Compare with: benchstat ${OUTFILE} <other_file>"
