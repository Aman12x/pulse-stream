#!/usr/bin/env bash
# Stage 1 crash test. A reference ingester runs untouched while a second ingester
# is kill -9'd KILLS times at random intervals. Both read the same Jetstream
# instance into their own topics; bin/verify then checks that no event in the
# shared window is missing from the chaos topic.
set -euo pipefail
cd "$(dirname "$0")/.."

KILLS=${KILLS:-10}
MIN_UP=${MIN_UP:-30}
MAX_UP=${MAX_UP:-90}
RUN=$(date -u +%Y%m%dT%H%M%SZ)
export HASH_SALT=${HASH_SALT:-crash-test}
export JETSTREAM_URL=${JETSTREAM_URL:-wss://jetstream2.us-east.bsky.network/subscribe}
mkdir -p results logs

INGEST_ID=ref-$RUN KAFKA_TOPIC=bsky.events.ref.$RUN METRICS_ADDR=:9102 \
  ./bin/ingest 2>"logs/ref-$RUN.log" &
REF=$!
trap 'kill -INT $REF 2>/dev/null || true' EXIT
sleep 15

for i in $(seq 1 "$KILLS"); do
  INGEST_ID=chaos-$RUN KAFKA_TOPIC=bsky.events.chaos.$RUN METRICS_ADDR=:9101 \
    ./bin/ingest 2>>"logs/chaos-$RUN.log" &
  PID=$!
  UP=$(( MIN_UP + RANDOM % (MAX_UP - MIN_UP + 1) ))
  sleep "$UP"
  kill -9 "$PID"
  wait "$PID" 2>/dev/null || true
  echo "kill $i/$KILLS after ${UP}s"
  sleep 3
done

# A final clean run so the chaos topic catches up past the last kill.
INGEST_ID=chaos-$RUN KAFKA_TOPIC=bsky.events.chaos.$RUN METRICS_ADDR=:9101 \
  ./bin/ingest 2>>"logs/chaos-$RUN.log" &
PID=$!
sleep 60
kill -INT "$PID"; wait "$PID" || true
sleep 20
kill -INT "$REF"; wait "$REF" || true
trap - EXIT

./bin/verify -ref "bsky.events.ref.$RUN" -chaos "bsky.events.chaos.$RUN" \
  -kills "$KILLS" -out "results/stage1_crash_test_$RUN.json"
