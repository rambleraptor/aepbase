#!/usr/bin/env bash
# Run the Aepbase vs OpenClaw comparison benchmark.
#
# Prerequisites:
#   1. Go 1.23+
#   2. ANTHROPIC_API_KEY set in environment (or use --dry-run)
#
# Usage:
#   ./benchmarks/comparison/run.sh                    # full run
#   ./benchmarks/comparison/run.sh --dry-run           # estimate tokens without API calls
#   ./benchmarks/comparison/run.sh --json              # JSON output
#   ./benchmarks/comparison/run.sh --records 10,50     # custom record counts

set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

echo "Building benchmark..."
go build -o /tmp/aepbase-benchmark ./benchmarks/comparison

echo "Running benchmark..."
/tmp/aepbase-benchmark "$@"
