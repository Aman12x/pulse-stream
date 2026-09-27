# Decision log

One entry per non-obvious choice: what was chosen, over what, and why.

## 2026-09-27 — Data source: Bluesky Jetstream, not a simulator

Chose the public Jetstream feed over a synthetic event generator. Every number this
project reports has to come from real traffic; a simulator only measures itself.
Jetstream also supports time-based cursors (unix microseconds) that replay history
from an instance, which is what makes the crash tests possible: a restarted process
can ask for exactly the range it missed.

## 2026-09-27 — Privacy at the edge

The record body (post text, profile fields) is never decoded: `RawCommit` has no
`record` field, so `encoding/json` skips it. DIDs are replaced by a salted SHA-256
before an event leaves `internal/jetstream`. Nothing downstream can leak what never
arrived.

## 2026-09-27 — Ingest delivery is at-least-once; exactly-once is the consumer's job

The ingester checkpoints a cursor only after Kafka has acknowledged every event up
to it (`ProduceSync` per batch, then advance), and resumes from that checkpoint
minus a 5-second rewind. Duplicates are therefore possible and gaps are not.
Deduplication belongs in the consumer (stage 2), where it can be done in the same
Postgres transaction as the write. Trying to make the producer exactly-once would
still leave the websocket-to-producer hop at-least-once.

Rejected: Kafka transactions on the producer side. They make Kafka-to-Kafka
pipelines exactly-once, but the source here is a websocket, and a crash between
reading a message and producing it is still recovered only by replay.

## 2026-09-27 — Exit on a produce error instead of retrying in place

franz-go already retries transient broker errors internally. An error that reaches
`ProduceSync` means those retries failed; the ingester exits, and the next start
resumes from the last checkpoint. One recovery path, and it is the one the crash
test exercises.

## 2026-09-27 — Checkpoints never move backwards

`Save` uses `GREATEST(existing, new)`. A process that is being replaced can still
write a stale cursor on its way out; that write must not undo its successor's
progress. The cost of a stale-but-forward checkpoint is a few seconds of replay.

## 2026-09-27 — Partition key is the hashed account id

All of one account's events land on one partition, in order. Stage 2's
per-account sessionization depends on that ordering.

## 2026-09-27 — Repair the reconnect handoff; hold the checkpoint until it is repaired

**Found by the first crash test** (`results/stage1_crash_test_20260927T170044Z.json`):
35 of 268,468 reference events in the shared window never reached the killed
ingester. All 35 sit within 70 ms of one of the 10 cursor reconnects (every
cluster matched a reconnect, none unmatched; the first connection had no cursor and
lost nothing). The reference ingester received the same events live, so they were
on the feed; they were not delivered on the connection opened with a cursor. The
loss is at Jetstream's replay-to-live handoff, which is why the 5-second rewind
could not fix it: the rewind moves the start of the replay, not the handoff.

**Fix.** Each cursor connection sends a marker before its first event. The
ingester then waits 30 seconds, until the handoff is history, opens a second
connection that replays ±5 s around it, and produces those events (duplicates are
removed downstream). Until that replay is produced, `ingest.Holds` caps the
checkpoint below the handoff, so a crash before the repair replays the window
again instead of skipping it. A repair that fails three times leaves the hold in
place: the checkpoint stalls, which is safe, and a metric and error log say so.

**Also fixed:** the crash test script's verdict was hidden by piping it through
`tail`, which reported exit 0 on a failing run. Run it without a pipe, or with
`set -o pipefail` in the calling shell.

## 2026-09-27 — Stage 2: offsets live in Postgres, in the same transaction as the output

Each partition's batch is applied in one Postgres transaction that deduplicates
(`seen_events`, `ON CONFLICT DO NOTHING RETURNING`), updates minute aggregates,
late counts and sessions, and advances that partition's `next_offset`. A crash
keeps the whole batch or none of it. Kafka's committed offsets are never used;
on assignment the consumer reads `next_offset` from Postgres
(`AdjustFetchOffsetsFn`).

Rejected: Kafka transactions (read-process-write exactly-once). They only cover
output written back to Kafka; the output here is Postgres, so the offset has to
commit with the Postgres write.

## 2026-09-27 — Fencing: a generation number checked under the row lock

Claiming a partition bumps `partition_state.generation`. Every batch starts with
`SELECT … FOR UPDATE` on that row and aborts with `ErrFenced` if the generation is
not its own. Because the claim is an UPDATE of the same row, it waits for any
in-flight batch of the previous owner: that batch either committed before the
claim (and the new owner resumes after it) or is fenced after. A consumer frozen
past its session timeout and then resumed cannot write.

`BlockRebalanceOnPoll` keeps a live member from losing a partition mid-batch; the
generation check covers the members that are not live (frozen, partitioned).

## 2026-09-27 — CloseAllowingRebalance, found by a hung test run

The first stage 2 run hung for 10 minutes after the clean consumer finished. A
goroutine dump (SIGQUIT) showed `main.run` blocked in `kgo.(*Client).Close` →
`LeaveGroupContext`: the last poll before shutdown left rebalancing blocked, and
leaving the group needs a rebalance. `CloseAllowingRebalance` fixes it; SIGINT now
exits in under a second.

## 2026-09-27 — Shared rows are written in sorted order

`engagement_minute` rows are shared by all partitions, so concurrent consumers
could lock them in opposite orders and deadlock. Keys are sorted before the
`unnest` upsert; a deadlock or serialization failure (40P01, 40001) that still
happens is retried up to 5 times rather than crashing the consumer.

## 2026-09-27 — Stage 2 test design

Input is a stage 1 chaos topic: real traffic with real duplicates and repair
events arriving out of order. It is replayed into three schemas: `clean` (one
undisturbed consumer), `chaos` (three consumers, randomly `kill -9`'d or frozen
for 15 s against a 10 s session timeout), and `ref` (one consumer on the
duplicate-free reference topic). Every output table is diffed row by row between
clean and chaos (`EXCEPT ALL` both ways), and minute aggregates inside the window
stage 1 verified are diffed between ref and chaos. `PROCESS_DELAY_MS` throttles the
chaos consumers so the kills land mid-stream; it is a test knob, off by default.

## 2026-09-27 — Reference check reconciles late events per minute

The second stage 2 run (20 s lateness) reported 46/44 minute rows differing from
the reference, and the first version of `compare` failed it. The reference topic
has no late events; the chaos topic's repair events arrive about 35 s late and were
correctly excluded. Checked directly: reference minus under-test totals was 843,
and the under-test run's late events in those minutes were 843. The check now
requires, per minute, reference = under test + late, which is the property that
matters; identical rows are still required when no events were late. Both result
files were regenerated from the same schemas with the new check (no reprocessing).

## 2026-09-27 — Stage 3: Protobuf in the Confluent wire format, two compatibility gates

Events move from JSON to Protobuf (`proto/pulse/v1/event.proto`), framed in the
Confluent wire format (magic byte, registry schema id, message index). Consumers
decode both, so topics written before the migration stay readable.

Two gates, because they catch different mistakes. `buf breaking` in CI compares
the .proto against the base branch and fails a pull request that renumbers,
retypes or removes a field. The schema registry, set to BACKWARD, checks the schema
a binary actually registers at startup; an ingester built against an incompatible
schema gets a 409 and does not start, so it can never write events the running
consumers cannot read.

Rollout order under BACKWARD is consumers first: the new schema must be able to
read data written with the old one, so readers upgrade before writers. (The stage
plan had said producer first; that was wrong for this compatibility mode.)

Kafka now has two listeners (kafka:19092 inside the Compose network, localhost:9092
for the host) so the registry container can reach it. Recreating the container
dropped the stage 1 and 2 topics; their results are in `results/`, and rerunning
those tests needs a fresh stage 1 run first.

## 2026-09-27 — Rollout test, and a run thrown away

`scripts/rollout_schema.sh` builds the v1 ingester from tag `stage-3a` in a
temporary git worktree, so "v1" is the real old binary, not a flag. The first run
was discarded: the working tree was switched to the breaking-change demo branch
while the script was building, a `go build` failed, and because it sat early in an
`&&` chain `set -e` did not stop the script, leaving possibly stale binaries in
the test. Each build is now its own line, so a failed build aborts the run. The
rerun on a clean `main` is the recorded result.
