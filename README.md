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
| 2 | Exactly-once consumers, sessions, watermarks | done, see `results/stage2_*` |
| 3 | Protobuf schemas, registry, CI compatibility gate | next |
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
./scripts/chaos_consume.sh      # stage 2 exactly-once test, writes results/stage2_exactly_once_*.json
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

## Stage 2 — exactly-once consumers

`cmd/consume` is a Kafka consumer group whose offsets live in Postgres. Each
partition's batch is applied in one transaction that deduplicates by event id,
updates per-minute engagement counts, late-event counts and per-account sessions
(30-minute gap), and advances the partition's stored offset. A consumer that claims
a partition bumps its generation number; a batch from any earlier owner, such as a
consumer frozen past its session timeout and then resumed, is rejected. Details and
the bugs found on the way are in [docs/decisions.md](docs/decisions.md).

**Test.** `scripts/chaos_consume.sh` replays a stage 1 topic, which carries real
duplicates and out-of-order repair events, three times: one undisturbed consumer,
three consumers under random `kill -9` and 15-second freezes, and one consumer on
the duplicate-free reference topic. Every output table is diffed row by row between
the first two, and each minute inside the stage 1 verified window must reconcile
with the reference: reference = under test + events excluded as late.

| Run | Kills / freezes / fenced batches | Records | Duplicates removed | Closed sessions | Late events | Rows differing, clean vs chaos (7 tables) | Minutes not reconciled with reference |
|---|---|---|---|---|---|---|---|
| `stage2_exactly_once_20260927t175548z.json` | 6 / 5 / 5 | 325,591 | 66,131 | 0 | 0 | 0 | 0 |
| `stage2_exactly_once_20260927t180124z.json` | 5 / 6 / 6 | 325,591 | 66,131 | 26,790 | 845 | 0 | 0 |

The second run shortens the session gap to 60 s and the lateness bound to 20 s so
that session closing and the late path both run under failure; with the defaults the
10-minute input closes no sessions and has no late events. Its 843 late events
inside the window are exactly the difference from the reference.
