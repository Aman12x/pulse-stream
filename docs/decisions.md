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
