#!/usr/bin/env bash
# Starts the Docker Compose profile an example needs, then runs it with Maven.
# One command, no need to remember which example needs which infrastructure.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
example="${1:-}"

if [ -z "$example" ]; then
  echo "Usage: $0 <example-name>" >&2
  echo "Available examples:" >&2
  ls "$ROOT_DIR/examples" >&2
  exit 1
fi

if [ ! -d "$ROOT_DIR/examples/$example" ]; then
  echo "✗ Unknown example: $example" >&2
  exit 1
fi

case "$example" in
  synchronous-processing) profile="sync" ;;
  outbox) profile="kafka" ;;
  kafka-order-processing) profile="kafka" ;;
  rabbitmq-order-processing) profile="rabbitmq" ;;
  resilience) profile="resilience" ;;
  saga-order-fulfillment) profile="saga" ;;
  *)
    echo "✗ No Compose profile mapped for '$example' — add one in scripts/run-example.sh" >&2
    exit 1
    ;;
esac

echo "→ Starting infrastructure (profile: $profile)..."
docker compose -f "$ROOT_DIR/docker-compose.yml" --profile "$profile" up -d --wait

echo "→ Running $example..."
# No -am here: spring-boot:run would otherwise also try (and fail) to run it against the
# build-only, packaging=pom parent aggregator, which has no main class.
"$ROOT_DIR/mvnw" -f "$ROOT_DIR/pom.xml" -pl "examples/$example" spring-boot:run
