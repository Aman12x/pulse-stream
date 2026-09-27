#!/usr/bin/env bash
# One health sample of the capture stack, appended as a JSON line to
# ~/pulse-health.jsonl. Run every 15 minutes by pulse-health.timer; the log is the
# source for the stage 5 uptime report.
set -uo pipefail
cd "$(dirname "$0")"
q() { curl -s --max-time 10 "http://127.0.0.1:9090/api/v1/query" --data-urlencode "query=$1" \
      | python3 -c 'import json,sys
try:
    r=json.load(sys.stdin)["data"]["result"]; print(round(sum(float(x["value"][1]) for x in r),3) if r else "null")
except Exception: print("null")'; }
containers=$(docker compose ps -a --format '{{.Service}} {{.State}}' 2>/dev/null | python3 -c 'import sys,json; print(json.dumps(dict(l.split(None,1) for l in sys.stdin.read().splitlines() if l.strip())))')
restarts=$(docker inspect -f '{{.RestartCount}}' $(docker compose ps -aq) 2>/dev/null | python3 -c 'import sys; print(sum(int(x) for x in sys.stdin.read().split()))')
python3 - "$containers" "${restarts:-0}" <<PY
import json, sys, shutil, datetime
disk = shutil.disk_usage("/")
print(json.dumps({
  "at": datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
  "containers": json.loads(sys.argv[1] or "{}"),
  "restarts_total": int(sys.argv[2]),
  "ingest_events_per_s_15m": $(q 'sum(rate(pulse_ingest_events_total[15m]))'),
  "consume_decoded_per_s_15m": $(q 'sum(rate(pulse_consume_decoded_total[15m]))'),
  "undecodable_15m": $(q 'sum(increase(pulse_consume_decoded_total{format="malformed"}[15m]))'),
  "ingest_lag_s": $(q 'max(pulse_ingest_lag_seconds)'),
  "ingest_reconnects_15m": $(q 'sum(increase(pulse_ingest_reconnects_total[15m]))'),
  "repair_failures_total": $(q 'sum(pulse_ingest_repair_failures_total)'),
  "archive_rows_total": $(q 'sum(pulse_archive_rows_total)'),
  "targets_up": $(q 'sum(up)'),
  "disk_used_pct": round(100 * disk.used / disk.total, 1),
}))
PY
