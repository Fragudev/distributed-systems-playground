#!/usr/bin/env bash
# Replays every message currently sitting on a topic's dead-letter topic back onto the original
# topic, preserving the key (so partitioning/ordering still applies once reprocessed). Assumes
# the project's own naming convention, "<topic>.DLT" (see docs/adr/0004-at-least-once-delivery.md),
# and the Kafka container started by docker-compose.yml's `kafka`/`saga` profile.
#
# This is exactly what PoisonMessageAndReplayTest exercises in-process against Testcontainers —
# this script does the same thing against a real running cluster.
set -euo pipefail

topic="${1:-}"
if [ -z "$topic" ]; then
  echo "Usage: $0 <original-topic>" >&2
  echo "Example: $0 order.created.v1   (reads order.created.v1.DLT, republishes onto order.created.v1)" >&2
  exit 1
fi

container="dsplayground-kafka-1"
dlt_topic="${topic}.DLT"
bootstrap="localhost:29092" # the INTERNAL listener — reachable from inside the container itself

if ! docker exec "$container" true >/dev/null 2>&1; then
  echo "✗ Kafka container '$container' isn't running. Start it first:" >&2
  echo "    docker compose --profile kafka up -d" >&2
  exit 1
fi

echo "→ Replaying pending messages from ${dlt_topic} onto ${topic}..."

# The consumer below is bounded by --timeout-ms and always exits non-zero once that timeout
# fires — including the normal, expected case of "the DLT is empty, nothing to replay". Turning
# off errexit just for this pipe avoids treating that as a script failure.
set +e
docker exec "$container" /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server "$bootstrap" \
  --topic "$dlt_topic" \
  --from-beginning \
  --timeout-ms 5000 \
  --property print.key=true \
  --property key.separator=: \
  2>/dev/null \
  | docker exec -i "$container" /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server "$bootstrap" \
    --topic "$topic" \
    --property parse.key=true \
    --property key.separator=:
set -e

echo "✓ Replayed pending messages from ${dlt_topic} onto ${topic} (if there were any)."
echo "  (Re-run this after fixing the underlying cause — replaying an unfixed poison message will"
echo "   just send it back through retry → DLT again.)"
