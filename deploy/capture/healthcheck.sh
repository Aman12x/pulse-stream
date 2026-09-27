#!/usr/bin/env bash
# One health sample of the capture stack, appended as a JSON line to
# ~/pulse-health.jsonl. Run every 15 minutes by pulse-health.timer; the log is the
# source for the stage 5 uptime report.
set -uo pipefail
cd "$(dirname "$0")"
q() { curl -s --max-time 10 "http://127.0.0.1:9090/api/v1/query" --data-urlencode "query=$1" \
      | python3 -c 'import json,sys
try:
    r=json.load(sys.stdin)["data"]["result"]; print(round(sum(float(x["value"][1]) for x in r),3) if r else "None")
except Exception: print("None")'; }
containers=$(docker compose ps -a --format '{{.Service}} {{.State}}' 2>/dev/null | python3 -c 'import sys,json; print(json.dumps(dict(l.split(None,1) for l in sys.stdin.read().splitlines() if l.strip())))')
restarts=$(docker inspect -f '{{.RestartCount}}' $(docker compose ps -aq) 2>/dev/null | python3 -c 'import sys; print(sum(int(x) for x in sys.stdin.read().split()))')
python3 - "$containers" "${restarts:-0}" <<PY
import json, os, sys, shutil, datetime, urllib.request
disk = shutil.disk_usage("/")
sample = {
  "at": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
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
}

# Compare with the previous sample for counters that should not move.
prev = {}
try:
    with open(os.path.expanduser("~/pulse-health.jsonl")) as f:
        lines = f.read().splitlines()
    prev = json.loads(lines[-1]) if lines else {}
except (OSError, ValueError):
    pass

s, problems = sample, []
down = [k for k, v in s["containers"].items() if v != "running"]
if down: problems.append("not running: " + ", ".join(down))
if prev.get("restarts_total") is not None and s["restarts_total"] > prev["restarts_total"]:
    problems.append(f"restarts {prev['restarts_total']} -> {s['restarts_total']}")
ing, con = s["ingest_events_per_s_15m"], s["consume_decoded_per_s_15m"]
if ing is None or ing < 100: problems.append(f"ingest rate {ing}/s (expect ~350)")
if ing and (con is None or con < 0.5 * ing): problems.append(f"consume rate {con}/s vs ingest {ing}/s")
if s["ingest_lag_s"] is not None and s["ingest_lag_s"] > 60: problems.append(f"ingest lag {s['ingest_lag_s']}s")
if (s["undecodable_15m"] or 0) > 0: problems.append(f"{s['undecodable_15m']} undecodable records in 15m")
if (s["repair_failures_total"] or 0) > 0: problems.append(f"{s['repair_failures_total']} handoff repair failures")
if s["targets_up"] is None or s["targets_up"] < 4: problems.append(f"only {s['targets_up']} of 4 scrape targets up")
if s["disk_used_pct"] > 80: problems.append(f"disk {s['disk_used_pct']}%")
s["problems"] = problems
print(json.dumps(s))

# Dead-man's switch: success ping when healthy, /fail with the reasons otherwise.
# healthchecks.io also alerts when pings stop (VM down, network gone).
url = os.environ.get("HC_PING_URL", "")
if url:
    body = ("\n".join(problems) if problems else "ok") + "\n\n" + json.dumps(s)
    try:
        urllib.request.urlopen(urllib.request.Request(url + ("/fail" if problems else ""), data=body.encode()), timeout=10)
    except Exception as e:
        print(f"ping failed: {e}", file=sys.stderr)
PY
