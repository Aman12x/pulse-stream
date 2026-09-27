# pulse-stream

A streaming pipeline over Bluesky's public Jetstream feed: Go producers and
consumers on Kafka, Postgres for state, Prometheus and Grafana for operations.

The point is correctness under failure on real traffic. Every figure in this README
is read from a file in `results/`, produced by a run you can repeat.

## Status

| Stage | What | State |
|---|---|---|
| 0 | Skeleton: Compose stack, CI, Makefile | done |
| 1 | Ingest: Jetstream → Kafka, crash-safe checkpoints | done, see `results/stage1_*` |
| 2 | Exactly-once consumers, sessions, watermarks | next |
| 3 | Protobuf schemas, registry, CI compatibility gate | planned |
| 4 | Leased, fenced backfills into versioned tables | planned |
| 5 | Continuous capture, dbt retention marts, dashboards | planned |

## Run it

```bash
make up          # Kafka (KRaft), Postgres, Prometheus, Grafana
make build
HASH_SALT=dev ./bin/ingest      # metrics on :9101, Grafana on :3000
make test                       # unit tests
make test-integration           # needs the stack up
make crash-test                 # stage 1 kill -9 test, writes results/stage1_crash_test_*.json
```

## Stage 1 — ingest

`cmd/ingest` reads Jetstream over a websocket, drops the record body, hashes the
account DID, and produces JSON events to Kafka keyed by account. Delivery is
at-least-once: a cursor is checkpointed to Postgres only after Kafka acknowledges
every event up to it, and a restart resumes from the checkpoint minus a 5-second
rewind. Reasons for each choice are in [docs/decisions.md](docs/decisions.md).

**Crash test.** `scripts/chaos_ingest.sh` runs a reference ingester untouched while
a second ingester is `kill -9`'d at random 30 to 90 second intervals. Both read the
same Jetstream instance. `cmd/verify` then compares the two topics over the window
both covered and reports events missing from the killed ingester (must be zero) and
duplicate records (expected, removed in stage 2).

Results, all in `results/`:

| Run | Kills | Reference events in window | Missing | Note |
|---|---|---|---|---|
| `stage1_crash_test_20260927T170044Z.json` | 10 | 268,468 | 35 | before the handoff repair; found the bug |
| `stage1_crash_test_20260927T171523Z.json` | 10 | 250,879 | 0 | repair pass, 30 to 90 s uptimes |
| `stage1_crash_test_20260927T172644Z.json` | 10 | 92,783 | 0 | 10 to 25 s uptimes, every kill before its repair |

The first run is kept on purpose. Its 35 missing events all sat within 70 ms of a
reconnect, which is how the Jetstream replay-to-live handoff gap was found; the
trace and the fix are in [docs/decisions.md](docs/decisions.md).
