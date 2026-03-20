#!/usr/bin/env bash
# Compare two benchmark result files using benchstat.
#
# Install benchstat:
#   go install golang.org/x/perf/cmd/benchstat@latest
#
# Usage:
#   ./benchmarks/scripts/compare.sh results/micro_old.txt results/micro_new.txt
set -euo pipefail

if [ $# -lt 2 ]; then
    echo "Usage: $0 <old_results> <new_results>"
    echo ""
    echo "Example:"
    echo "  $0 benchmarks/results/micro_20260319_100000.txt benchmarks/results/micro_20260319_110000.txt"
    exit 1
fi

if ! command -v benchstat &> /dev/null; then
    echo "benchstat not found. Install with:"
    echo "  go install golang.org/x/perf/cmd/benchstat@latest"
    exit 1
fi

benchstat "$1" "$2"
