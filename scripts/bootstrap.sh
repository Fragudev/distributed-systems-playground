#!/usr/bin/env bash
# Checks the prerequisites a reviewer needs before running any example.
# Fails fast with a clear message instead of letting them hit a cryptic error two steps in.
set -euo pipefail

fail() {
  echo "✗ $1" >&2
  exit 1
}

command -v docker >/dev/null 2>&1 || fail "Docker is required. Install it from https://docs.docker.com/get-docker/"
docker compose version >/dev/null 2>&1 || fail "Docker Compose v2 is required (docker compose, not docker-compose)."
docker info >/dev/null 2>&1 || fail "Docker daemon is not running."

command -v java >/dev/null 2>&1 || fail "Java 21+ is required."
java_major=$(java -version 2>&1 | head -1 | sed -E 's/.*"([0-9]+).*/\1/')
if [ "$java_major" -lt 21 ]; then
  fail "Java 21+ is required, found major version $java_major."
fi

echo "✓ Docker, Docker Compose and Java $java_major look good."
echo "  Next: ./scripts/run-example.sh <example-name>"
