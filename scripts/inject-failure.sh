#!/usr/bin/env bash
# Triggers the reproducible failure scenario for a given example (see
# 02-distributed-systems-playground-PLAN.md §7). Filled in per-example as each
# example's failure scenario is implemented — not wired up yet for any of them.
set -euo pipefail

example="${1:-}"
scenario="${2:-}"

if [ -z "$example" ] || [ -z "$scenario" ]; then
  echo "Usage: $0 <example-name> <scenario>" >&2
  echo "Not implemented yet — each example wires this up when its failure scenario lands." >&2
  exit 1
fi

echo "✗ No failure scenario implemented yet for '$example/$scenario'." >&2
exit 1
