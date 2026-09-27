#!/usr/bin/env bash
# Stage 4 test. On one captured topic:
#   v1  baseline (30 min session gap), one worker, diffed and cut over: v1 is live
#   v2  the same config rebuilt by two workers while one is frozen past its lease:
#       must equal v1 row for row, and the frozen worker's writes must be rejected
#   v3  a real logic change (session gap 30 min -> 5 min): diffed against live v1,
#       then cut over; the live views must now read v3
set -euo pipefail
cd "$(dirname "$0")/.."

TOPIC=${TOPIC:?set TOPIC to a captured topic}
LEASE=${LEASE:-8s}
FREEZE=${FREEZE:-20}
RUN=$(date -u +%Y%m%dt%H%M%Sz)
export BACKFILL_SCHEMA=bf_$RUN
mkdir -p logs results
go build -o bin/backfill ./cmd/backfill
B=./bin/backfill
L=logs/backfill-$RUN.log

v() { sed -n 's/^version \([0-9]*\) planned.*/\1/p'; }

V1=$($B plan -topic "$TOPIC" -gap 1800 -late 300 -note "baseline" | tee -a "$L" | v)
$B work -version "$V1" -worker solo -lease "$LEASE" 2>>"$L"
$B diff -version "$V1" -out "logs/diff-v$V1-$RUN.json" > /dev/null
$B cutover -version "$V1"

V2=$($B plan -topic "$TOPIC" -gap 1800 -late 300 -note "replica of v$V1, built while a worker is frozen" | tee -a "$L" | v)
$B work -version "$V2" -worker A -lease "$LEASE" -batch 500 -delay 200ms 2>>"logs/backfill-A-$RUN.log" &
A=$!
$B work -version "$V2" -worker B -lease "$LEASE" -batch 500 -delay 200ms 2>>"logs/backfill-B-$RUN.log" &
BW=$!
# Freeze A only once it is inside a job, so the freeze always forces a takeover.
until [ "$(grep -c '"msg":"claimed job"' "logs/backfill-A-$RUN.log" 2>/dev/null)" -gt 0 ]; do sleep 0.2; done
sleep 3
kill -STOP "$A"; echo "froze worker A mid-job (lease $LEASE)"
# Keep A frozen until another worker has taken its job over, however long B's own
# jobs take. A timer alone is not enough: on a large topic B stayed busy for the
# whole freeze and A woke up with its lease renewed, so nothing was tested.
claims() { $B status -version "$V2" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["Claims"] - d["Jobs"])'; }
T0=$(date +%s)
until [ "$(claims)" -gt 0 ]; do
  [ $(( $(date +%s) - T0 )) -lt 600 ] || { echo "no takeover within 600s"; break; }
  sleep 1
done
FROZE_FOR=$(( $(date +%s) - T0 ))
sleep 2
kill -CONT "$A"; echo "resumed worker A after ${FROZE_FOR}s; another worker holds its job"
wait "$A" "$BW"
$B diff -version "$V2" -against "$V1" -out "logs/diff-v$V2-$RUN.json" > /dev/null

V3=$($B plan -topic "$TOPIC" -gap 300 -late 300 -note "session gap 30 min -> 5 min" | tee -a "$L" | v)
$B work -version "$V3" -worker C -lease "$LEASE" 2>>"$L" &
C1=$!
$B work -version "$V3" -worker D -lease "$LEASE" 2>>"$L" &
C2=$!
wait "$C1" "$C2"
$B diff -version "$V3" -out "results/stage4_logic_change_diff_$RUN.json" > /dev/null   # against the live version
$B cutover -version "$V3"

RUN=$RUN V1=$V1 V2=$V2 V3=$V3 TOPIC=$TOPIC LEASE=$LEASE FREEZE=$FROZE_FOR python3 - <<'PY'
import json, os, subprocess
e = os.environ; run = e["RUN"]; ctl = "bf_" + run
def psql(q):
    return subprocess.check_output(["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "pulse", "-Atc", q], text=True).strip()
def status(v):
    return json.loads(subprocess.check_output(["./bin/backfill", "status", "-version", v], env={**os.environ, "BACKFILL_SCHEMA": ctl}, text=True))
logA = [json.loads(l) for l in open(f"logs/backfill-A-{run}.log") if l.startswith("{")]
logB = [json.loads(l) for l in open(f"logs/backfill-B-{run}.log") if l.startswith("{")]
outcomes = {}
for rec in logA + logB:
    if rec.get("msg") == "job ended":
        k = f'{rec["worker"]} {rec["outcome"]}'
        outcomes[k] = outcomes.get(k, 0) + 1
rejected = sum(1 for r in logA + logB if r.get("msg") in ("fenced", "lease lost", "lease lost before completion"))
replica = json.load(open(f"logs/diff-v{e['V2']}-{run}.json"))
rows_diff = sum(v["only_in_baseline"] + v["only_in_candidate"] for v in replica["rows_differing"].values())
s2 = status(e["V2"])
live_v3 = psql(f"select count(*) from {ctl}_live.sessions") == psql(f"select count(*) from {ctl}_v{e['V3']}.sessions")
change = json.load(open(f"results/stage4_logic_change_diff_{run}.json"))
res = {
    "measured_at": replica["measured_at"], "topic": e["TOPIC"],
    "records_in_range": int(psql(f"select sum(end_offset) from {ctl}.jobs where version = {e['V1']}")),
    "replica": {
        "version": int(e["V2"]), "rebuilds_version": int(e["V1"]), "lease": e["LEASE"], "freeze_seconds": int(e["FREEZE"]),
        "jobs": s2["Jobs"], "claims": s2["Claims"], "job_outcomes": outcomes,
        "stale_worker_rejections": rejected, "rows_differing_from_baseline": rows_diff,
    },
    "logic_change": {"from": change["baseline"], "to": change["candidate"],
                     "diff_report": f"results/stage4_logic_change_diff_{run}.json"},
    "cutover": {"live_version": int(e["V3"]), "live_views_read_new_version": live_v3},
}
res["pass"] = rows_diff == 0 and s2["Claims"] > s2["Jobs"] and rejected > 0 and live_v3 and s2["State"] == "ready"
out = json.dumps(res, indent=2); print(out)
open(f"results/stage4_backfill_{run}.json", "w").write(out + "\n")
raise SystemExit(0 if res["pass"] else 2)
PY
