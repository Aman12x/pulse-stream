#!/usr/bin/env bash
# Stage 3 live schema rollout. Consumers built with schema v2 start first (BACKWARD:
# readers upgrade before writers). An ingester built from tag stage-3a (schema v1)
# runs, then is replaced mid-stream by the current ingester (schema v2) with the
# same checkpoint. A reference ingester runs untouched throughout. Checks: no event
# missing across the switch, no record the consumers cannot decode, both schema ids
# seen, and the registry rejecting a breaking schema.
set -euo pipefail
cd "$(dirname "$0")/.."

PHASE=${PHASE:-90}
RUN=$(date -u +%Y%m%dT%H%M%SZ)
TOPIC=bsky.events.rollout.$RUN REFTOPIC=bsky.events.rollout-ref.$RUN SCHEMA=rollout_$(echo "$RUN" | tr 'A-Z' 'a-z')
export HASH_SALT=${HASH_SALT:-rollout} JETSTREAM_URL=${JETSTREAM_URL:-wss://jetstream2.us-east.bsky.network/subscribe}
mkdir -p logs results bin

WT=$(mktemp -d)
git worktree add -q "$WT" stage-3a
(cd "$WT" && go build -o "$OLDPWD/bin/ingest-v1" ./cmd/ingest)
git worktree remove --force "$WT"
# One command per line: under set -e a failure early in an && chain does not stop
# the script, and a stale binary would silently invalidate the test.
go build -o bin/ingest ./cmd/ingest
go build -o bin/consume ./cmd/consume
go build -o bin/verify ./cmd/verify
go build -o bin/compare ./cmd/compare

INGEST_ID=ref-$RUN KAFKA_TOPIC=$REFTOPIC METRICS_ADDR=:9102 ./bin/ingest 2>"logs/rollout-ref-$RUN.log" &
REF=$!
INGEST_ID=rollout-$RUN KAFKA_TOPIC=$TOPIC METRICS_ADDR=:9101 ./bin/ingest-v1 2>"logs/rollout-ingest-$RUN.log" &
ING=$!
sleep 5
for c in 1 2; do
  CONSUMER_ID=c$c PG_SCHEMA=$SCHEMA KAFKA_TOPIC=$TOPIC CONSUMER_GROUP=g-rollout-$RUN METRICS_ADDR=:913$c \
    ./bin/consume 2>>"logs/rollout-consume-$RUN.log" &
  eval "C$c=\$!"
  sleep 2
done

sleep "$PHASE"
kill -INT "$ING"; wait "$ING" || true
echo "switching ingester v1 -> v2"
INGEST_ID=rollout-$RUN KAFKA_TOPIC=$TOPIC METRICS_ADDR=:9101 ./bin/ingest 2>>"logs/rollout-ingest-$RUN.log" &
ING=$!
sleep "$PHASE"
kill -INT "$ING"; wait "$ING" || true
sleep 20
kill -INT "$REF"; wait "$REF" || true

./bin/compare wait -schema "$SCHEMA" -topic "$TOPIC"
M1=$(curl -s localhost:9131/metrics) M2=$(curl -s localhost:9132/metrics)
kill -INT "$C1" "$C2"; wait "$C1" "$C2" || true

./bin/verify -ref "$REFTOPIC" -chaos "$TOPIC" -kills 0 -out "logs/rollout-verify-$RUN.json" || true

SUBJECT=$TOPIC-value RUN=$RUN M1=$M1 M2=$M2 python3 - <<'PY'
import json, os, re, urllib.request, urllib.error
subject, run = os.environ["SUBJECT"], os.environ["RUN"]
def get(path):
    return json.load(urllib.request.urlopen("http://localhost:8081" + path))
versions = get(f"/subjects/{subject}/versions")
ids = {v: get(f"/subjects/{subject}/versions/{v}")["id"] for v in versions}
# A breaking change: field 3 retyped from int64 to string.
proto = open("proto/pulse/v1/event.proto").read().replace("int64 time_us = 3;", "string time_us = 3;")
req = urllib.request.Request(f"http://localhost:8081/subjects/{subject}/versions",
    data=json.dumps({"schemaType": "PROTOBUF", "schema": proto}).encode(),
    headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}, method="POST")
try:
    urllib.request.urlopen(req); breaking_status = 200
except urllib.error.HTTPError as e:
    breaking_status = e.code
decoded, malformed, subject_events = {}, 0, 0
for text in (os.environ["M1"], os.environ["M2"]):
    for fmt, sid, n in re.findall(r'pulse_consume_decoded_total\{consumer="[^"]+",format="([^"]+)",schema_id="([^"]*)"\} (\S+)', text):
        if fmt == "malformed":
            malformed += int(float(n))
        else:
            decoded[f"{fmt} schema {sid}"] = decoded.get(f"{fmt} schema {sid}", 0) + int(float(n))
    subject_events += sum(int(float(n)) for n in re.findall(r'pulse_consume_with_subject_total\{[^}]*\} (\S+)', text))
errors = sum(1 for l in open(f"logs/rollout-consume-{run}.log") if '"level":"ERROR"' in l)
verify = json.load(open(f"logs/rollout-verify-{run}.json"))
res = {
    "measured_at": verify["measured_at"], "topic": subject.rsplit("-value", 1)[0],
    "registry_versions": ids, "decoded_by_schema": decoded, "undecodable_records": malformed,
    "events_with_subject_account_id": subject_events, "consumer_error_logs": errors,
    "breaking_schema_registration_http_status": breaking_status,
    "reference_events_in_window": verify["reference_events_in_window"],
    "missing_across_rollout": verify["missing_from_chaos"], "window_seconds": verify["window_seconds"],
}
res["pass"] = (len(ids) == 2 and malformed == 0 and errors == 0 and breaking_status == 409
               and verify["missing_from_chaos"] == 0 and subject_events > 0 and len(decoded) == 2)
out = json.dumps(res, indent=2)
print(out)
open(f"results/stage3_rollout_{run}.json", "w").write(out + "\n")
raise SystemExit(0 if res["pass"] else 2)
PY
