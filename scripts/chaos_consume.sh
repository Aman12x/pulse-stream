#!/usr/bin/env bash
# Stage 2 exactly-once test. Replays a stage-1 topic (which carries real duplicates
# and out-of-order repair events) into three schemas:
#   clean  one consumer, never disturbed
#   ref    one consumer on the duplicate-free reference topic
#   chaos  three consumers, randomly kill -9'd or frozen past the session timeout
# then diffs every output table between clean and chaos, and the minute
# aggregates inside the stage-1 verified window between ref and chaos.
set -euo pipefail
cd "$(dirname "$0")/.."

STAGE1=${STAGE1:-20260927T171523Z}
INPUT=bsky.events.chaos.$STAGE1
REF=bsky.events.ref.$STAGE1
WIN_START=$(python3 -c "import json;print(json.load(open('results/stage1_crash_test_$STAGE1.json'))['window_start_utc'])")
WIN_END=$(python3 -c "import json;print(json.load(open('results/stage1_crash_test_$STAGE1.json'))['window_end_utc'])")
RUN=$(date -u +%Y%m%dt%H%M%Sz)
CLEAN=clean_$RUN CHAOS=chaos_$RUN REFS=ref_$RUN
mkdir -p logs results

one() { # schema topic group metrics_port
  PG_SCHEMA=$1 KAFKA_TOPIC=$2 CONSUMER_GROUP=$3 CONSUMER_ID=$3 METRICS_ADDR=:$4 \
    ./bin/consume 2>"logs/$3.log" &
  local pid=$!
  ./bin/compare wait -schema "$1" -topic "$2"
  kill -INT "$pid"; wait "$pid" || true
}
one "$CLEAN" "$INPUT" "g-clean-$RUN" 9114
one "$REFS" "$REF" "g-ref-$RUN" 9115

declare -a PIDS
start() { # slot
  CONSUMER_ID=c$1-$RUN PG_SCHEMA=$CHAOS KAFKA_TOPIC=$INPUT CONSUMER_GROUP=g-chaos-$RUN \
    METRICS_ADDR=:$((9110 + $1)) PROCESS_DELAY_MS=${PROCESS_DELAY_MS:-300} MAX_POLL_RECORDS=${MAX_POLL_RECORDS:-200} \
    ./bin/consume 2>>"logs/chaos-consume-$RUN.log" &
  PIDS[$1]=$!
}
start 1; sleep 3; start 2; start 3   # the first one creates the schema

./bin/compare wait -schema "$CHAOS" -topic "$INPUT" > "logs/wait-$RUN.log" &
WAITER=$!
KILLS=0 STOPS=0
while kill -0 "$WAITER" 2>/dev/null; do
  sleep $(( 10 + RANDOM % 11 ))
  kill -0 "$WAITER" 2>/dev/null || break
  slot=$(( 1 + RANDOM % 3 ))
  if (( RANDOM % 2 )); then
    kill -9 "${PIDS[$slot]}"; wait "${PIDS[$slot]}" 2>/dev/null || true
    KILLS=$((KILLS + 1)); echo "kill -9 consumer $slot"
    sleep 2; start "$slot"
  else
    kill -STOP "${PIDS[$slot]}"; STOPS=$((STOPS + 1)); echo "froze consumer $slot for 15s"
    sleep 15
    kill -CONT "${PIDS[$slot]}"
  fi
done
for p in "${PIDS[@]}"; do kill -INT "$p" 2>/dev/null || true; done
wait 2>/dev/null || true
FENCED=$(grep -c '"msg":"fenced"' "logs/chaos-consume-$RUN.log" || true)

./bin/compare diff -a "$CLEAN" -b "$CHAOS" -ref "$REFS" -window-start "$WIN_START" -window-end "$WIN_END" \
  -note input_topic="$INPUT" -note consumers=3 -note kills="$KILLS" -note freezes="$STOPS" -note fenced_batches="$FENCED" \
  -out "results/stage2_exactly_once_$RUN.json"
