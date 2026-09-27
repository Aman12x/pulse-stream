package consume

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Aman12x/pulse-stream/internal/jetstream"
)

const schemaDDL = `
CREATE TABLE IF NOT EXISTS partition_state (
    topic        text   NOT NULL,
    partition    int    NOT NULL,
    generation   bigint NOT NULL DEFAULT 0,
    next_offset  bigint NOT NULL DEFAULT 0,
    watermark_us bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (topic, partition)
);
CREATE TABLE IF NOT EXISTS seen_events (
    event_id text   PRIMARY KEY,
    time_us  bigint NOT NULL
);
CREATE INDEX IF NOT EXISTS seen_events_time_us ON seen_events (time_us);
CREATE TABLE IF NOT EXISTS engagement_minute (
    minute_us  bigint NOT NULL,
    kind       text   NOT NULL,
    collection text   NOT NULL,
    operation  text   NOT NULL,
    events     bigint NOT NULL,
    PRIMARY KEY (minute_us, kind, collection, operation)
);
CREATE TABLE IF NOT EXISTS late_minute (
    minute_us bigint PRIMARY KEY,
    events    bigint NOT NULL
);
CREATE TABLE IF NOT EXISTS account_state (
    account_id text   PRIMARY KEY,
    start_us   bigint NOT NULL,
    last_us    bigint NOT NULL,
    events     bigint NOT NULL
);
CREATE TABLE IF NOT EXISTS sessions (
    account_id text   NOT NULL,
    start_us   bigint NOT NULL,
    end_us     bigint NOT NULL,
    events     bigint NOT NULL,
    PRIMARY KEY (account_id, start_us)
);
CREATE TABLE IF NOT EXISTS session_late (
    topic     text   NOT NULL,
    partition int    NOT NULL,
    events    bigint NOT NULL,
    PRIMARY KEY (topic, partition)
);`

// ErrFenced means another consumer has claimed the partition since this one did.
// The batch was not applied; the caller must stop processing that partition.
var ErrFenced = errors.New("partition claimed by a newer consumer")

type Config struct {
	LateUS int64 // how far behind the watermark an event may arrive and still count
	GapUS  int64 // inactivity that ends a session
}

type Store struct {
	pool *pgxpool.Pool
	cfg  Config
}

// Open connects with search_path set to schema, creating it and its tables.
func Open(ctx context.Context, dsn, schema string, cfg Config) (*Store, error) {
	pcfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	pcfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, err
	}
	if _, err := pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+pgx.Identifier{schema}.Sanitize()); err != nil {
		pool.Close()
		return nil, err
	}
	if _, err := pool.Exec(ctx, schemaDDL); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool, cfg: cfg}, nil
}

func (s *Store) Close() { s.pool.Close() }

// Claim takes ownership of a partition. It bumps the partition's generation, which
// fences every earlier owner, and returns where to resume. It waits for any
// in-flight transaction of the previous owner to finish, because Apply holds the
// row lock for its whole transaction.
func (s *Store) Claim(ctx context.Context, topic string, partition int32) (generation, nextOffset int64, err error) {
	err = s.pool.QueryRow(ctx, `
		INSERT INTO partition_state (topic, partition, generation) VALUES ($1, $2, 1)
		ON CONFLICT (topic, partition) DO UPDATE SET generation = partition_state.generation + 1
		RETURNING generation, next_offset`, topic, partition).Scan(&generation, &nextOffset)
	return
}

// Record is one Kafka record handed to Apply, in partition offset order.
type Record struct {
	Offset    int64
	Event     jetstream.Event
	Malformed bool // undecodable value: its offset is consumed, the record is ignored
}

type ApplyStats struct {
	Applied    int // records whose offsets advanced the partition
	Skipped    int // records at offsets already applied (redelivery after a restart)
	New        int // events not seen before
	Duplicates int
	Late       int
}

// Apply processes one partition's records in a single transaction: fencing check,
// dedupe, aggregates, sessions, and the next offset commit together or not at all.
func (s *Store) Apply(ctx context.Context, topic string, partition int32, generation int64, recs []Record) (ApplyStats, error) {
	var st ApplyStats
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return st, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after Commit

	var gen, next, watermark int64
	err = tx.QueryRow(ctx, `
		SELECT generation, next_offset, watermark_us FROM partition_state
		WHERE topic = $1 AND partition = $2 FOR UPDATE`, topic, partition).Scan(&gen, &next, &watermark)
	if err != nil {
		return st, err
	}
	if gen != generation {
		return st, ErrFenced
	}

	for len(recs) > 0 && recs[0].Offset < next {
		recs = recs[1:]
		st.Skipped++
	}
	if len(recs) == 0 {
		return st, nil
	}
	if recs[0].Offset != next {
		return st, fmt.Errorf("offset gap on %s/%d: stored next %d, batch starts at %d", topic, partition, next, recs[0].Offset)
	}
	st.Applied = len(recs)
	lastOffset := recs[len(recs)-1].Offset
	valid := recs[:0:0]
	for _, r := range recs {
		if !r.Malformed {
			valid = append(valid, r)
		}
	}
	recs = valid

	ids := make([]string, len(recs))
	times := make([]int64, len(recs))
	for i, r := range recs {
		ids[i], times[i] = r.Event.EventID, r.Event.TimeUS
	}
	rows, err := tx.Query(ctx, `
		INSERT INTO seen_events (event_id, time_us)
		SELECT * FROM unnest($1::text[], $2::bigint[])
		ON CONFLICT DO NOTHING RETURNING event_id`, ids, times)
	if err != nil {
		return st, err
	}
	fresh := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return st, err
		}
		fresh[id] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return st, err
	}

	events := make([]jetstream.Event, 0, len(fresh))
	accounts := make([]string, 0, len(fresh))
	for _, r := range recs {
		// fresh[id] is consumed on first use so a duplicate inside this batch is dropped too.
		if fresh[r.Event.EventID] {
			delete(fresh, r.Event.EventID)
			events = append(events, r.Event)
			accounts = append(accounts, r.Event.AccountID)
		}
	}
	st.New, st.Duplicates = len(events), len(recs)-len(events)

	states := map[string]AccountState{}
	rows, err = tx.Query(ctx, `SELECT account_id, start_us, last_us, events FROM account_state WHERE account_id = ANY($1)`, accounts)
	if err != nil {
		return st, err
	}
	for rows.Next() {
		var id string
		var a AccountState
		if err := rows.Scan(&id, &a.StartUS, &a.LastUS, &a.Events); err != nil {
			rows.Close()
			return st, err
		}
		states[id] = a
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return st, err
	}

	res := Process(watermark, s.cfg.LateUS, s.cfg.GapUS, states, events)
	for _, n := range res.Late {
		st.Late += int(n)
	}
	if err := s.write(ctx, tx, topic, partition, res); err != nil {
		return st, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE partition_state SET next_offset = $3, watermark_us = $4
		WHERE topic = $1 AND partition = $2`, topic, partition, lastOffset+1, res.Watermark); err != nil {
		return st, err
	}
	return st, tx.Commit(ctx)
}

func (s *Store) write(ctx context.Context, tx pgx.Tx, topic string, partition int32, r Result) error {
	if len(r.Minutes) > 0 {
		// Shared rows across partitions: lock them in one global order so concurrent
		// consumers cannot deadlock on each other.
		keys := make([]MinuteKey, 0, len(r.Minutes))
		for key := range r.Minutes {
			keys = append(keys, key)
		}
		slices.SortFunc(keys, func(a, b MinuteKey) int {
			return cmp.Or(cmp.Compare(a.MinuteUS, b.MinuteUS), cmp.Compare(a.Kind, b.Kind),
				cmp.Compare(a.Collection, b.Collection), cmp.Compare(a.Operation, b.Operation))
		})
		var m, n []int64
		var k, c, o []string
		for _, key := range keys {
			m, k, c, o, n = append(m, key.MinuteUS), append(k, key.Kind), append(c, key.Collection), append(o, key.Operation), append(n, r.Minutes[key])
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO engagement_minute (minute_us, kind, collection, operation, events)
			SELECT * FROM unnest($1::bigint[], $2::text[], $3::text[], $4::text[], $5::bigint[])
			ON CONFLICT (minute_us, kind, collection, operation)
			DO UPDATE SET events = engagement_minute.events + EXCLUDED.events`, m, k, c, o, n); err != nil {
			return err
		}
	}
	if len(r.Late) > 0 {
		var m, n []int64
		for minute := range r.Late {
			m = append(m, minute)
		}
		slices.Sort(m)
		for _, minute := range m {
			n = append(n, r.Late[minute])
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO late_minute (minute_us, events) SELECT * FROM unnest($1::bigint[], $2::bigint[])
			ON CONFLICT (minute_us) DO UPDATE SET events = late_minute.events + EXCLUDED.events`, m, n); err != nil {
			return err
		}
	}
	if len(r.Accounts) > 0 {
		var ids []string
		var start, last, n []int64
		for id, a := range r.Accounts {
			ids, start, last, n = append(ids, id), append(start, a.StartUS), append(last, a.LastUS), append(n, a.Events)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO account_state (account_id, start_us, last_us, events)
			SELECT * FROM unnest($1::text[], $2::bigint[], $3::bigint[], $4::bigint[])
			ON CONFLICT (account_id) DO UPDATE
			SET start_us = EXCLUDED.start_us, last_us = EXCLUDED.last_us, events = EXCLUDED.events`, ids, start, last, n); err != nil {
			return err
		}
	}
	if len(r.Closed) > 0 {
		var ids []string
		var start, end, n []int64
		for _, c := range r.Closed {
			ids, start, end, n = append(ids, c.AccountID), append(start, c.StartUS), append(end, c.EndUS), append(n, c.Events)
		}
		// Plain INSERT: a primary-key conflict here would mean a session was closed
		// twice, which exactly-once processing must never do. Let it fail loudly.
		if _, err := tx.Exec(ctx, `
			INSERT INTO sessions (account_id, start_us, end_us, events)
			SELECT * FROM unnest($1::text[], $2::bigint[], $3::bigint[], $4::bigint[])`, ids, start, end, n); err != nil {
			return err
		}
	}
	if r.SessionLate > 0 {
		if _, err := tx.Exec(ctx, `
			INSERT INTO session_late (topic, partition, events) VALUES ($1, $2, $3)
			ON CONFLICT (topic, partition) DO UPDATE SET events = session_late.events + EXCLUDED.events`,
			topic, partition, r.SessionLate); err != nil {
			return err
		}
	}
	return nil
}

// PruneSeen deletes dedupe entries for events older than cutoffUS, in batches so
// no single statement holds locks for long. The dedupe window is therefore
// bounded: a duplicate arriving after its original has been pruned is counted
// again. Duplicates in this pipeline come from ingest restarts and handoff
// repairs, which replay seconds to minutes, so a window of days is ample.
func (s *Store) PruneSeen(ctx context.Context, cutoffUS int64, batch int) (int64, error) {
	var total int64
	for {
		tag, err := s.pool.Exec(ctx, `
			DELETE FROM seen_events WHERE event_id IN (
			    SELECT event_id FROM seen_events WHERE time_us < $1 LIMIT $2)`, cutoffUS, batch)
		if err != nil {
			return total, err
		}
		total += tag.RowsAffected()
		if tag.RowsAffected() < int64(batch) {
			return total, nil
		}
	}
}
