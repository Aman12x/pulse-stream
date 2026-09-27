// Package backfill rebuilds output tables for a new logic version from the Kafka
// log, safely.
//
// A version is a logic configuration (session gap, lateness) whose output goes
// into its own Postgres schema, built with the same exactly-once store as the live
// consumers. A backfill is planned as one job per partition over an offset range
// fixed at planning time. Workers take jobs under a time-limited lease
// (SELECT … FOR UPDATE SKIP LOCKED) and renew it after every batch.
//
// Two fencing layers. Every claim bumps the job's token, and completing a job
// requires the current token. Data writes are fenced by the version schema's
// partition generation, which the worker bumps when it starts the job
// (consume.Store.Claim). A worker frozen past its lease can therefore neither write
// output nor mark its job done once another worker has taken over.
//
// Nothing becomes visible until an explicit cutover, which refuses to run before
// the version is complete and a diff against the live version has been recorded,
// and then repoints the live views in one transaction.
package backfill

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Aman12x/pulse-stream/internal/consume"
)

var (
	ErrNoJob     = errors.New("no claimable job")
	ErrLeaseLost = errors.New("lease lost to another worker")
)

// OutputTables are the tables a version produces and the live views expose.
var OutputTables = []string{"engagement_minute", "late_minute", "sessions", "account_state", "session_late"}

type Control struct {
	pool   *pgxpool.Pool
	dsn    string
	schema string // control tables
	live   string // schema holding the live views
}

type Spec struct {
	Topic       string
	SessionGapS int
	LateS       int
	Note        string
}

type Job struct {
	ID        int64
	Version   int
	Partition int32
	EndOffset int64 // exclusive
	Token     int64
	Topic     string
	Schema    string // the version's output schema
	Config    consume.Config
}

type Status struct {
	Version  int
	Schema   string
	State    string // building, ready, live, retired
	Jobs     int
	Done     int
	Claims   int // total claims across jobs; above Jobs means leases were taken over
	DiffPath string
}

func Open(ctx context.Context, dsn, schema string) (*Control, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	live := "live"
	if schema != "backfill" {
		live = schema + "_live" // isolated control schemas (tests) get isolated live views
	}
	c := &Control{pool: pool, dsn: dsn, schema: schema, live: live}
	s := c.q
	_, err = pool.Exec(ctx, fmt.Sprintf(`
		CREATE SCHEMA IF NOT EXISTS %[1]s;
		CREATE TABLE IF NOT EXISTS %[1]s.versions (
		    version       int PRIMARY KEY,
		    schema_name   text NOT NULL,
		    topic         text NOT NULL,
		    session_gap_s int  NOT NULL,
		    late_s        int  NOT NULL,
		    note          text NOT NULL DEFAULT '',
		    state         text NOT NULL DEFAULT 'building',
		    diff_path     text,
		    created_at    timestamptz NOT NULL DEFAULT now(),
		    live_at       timestamptz
		);
		CREATE TABLE IF NOT EXISTS %[1]s.jobs (
		    id          bigserial PRIMARY KEY,
		    version     int    NOT NULL REFERENCES %[1]s.versions,
		    partition   int    NOT NULL,
		    end_offset  bigint NOT NULL,
		    status      text   NOT NULL DEFAULT 'pending',
		    lease_owner text,
		    lease_until timestamptz,
		    token       bigint NOT NULL DEFAULT 0,
		    claims      int    NOT NULL DEFAULT 0,
		    UNIQUE (version, partition)
		);`, s("")))
	if err != nil {
		pool.Close()
		return nil, err
	}
	return c, nil
}

func (c *Control) Close() { c.pool.Close() }

// q returns a schema-qualified identifier; q("") is the schema itself.
func (c *Control) q(table string) string {
	if table == "" {
		return pgx.Identifier{c.schema}.Sanitize()
	}
	return pgx.Identifier{c.schema, table}.Sanitize()
}

// Plan registers a new version and one job per non-empty partition. endOffsets
// fixes the range: the backfill rebuilds exactly the log as it stood at planning.
func (c *Control) Plan(ctx context.Context, spec Spec, endOffsets map[int32]int64) (int, error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	var v int
	if _, err := tx.Exec(ctx, "LOCK TABLE "+c.q("versions")+" IN EXCLUSIVE MODE"); err != nil {
		return 0, err
	}
	if err := tx.QueryRow(ctx, "SELECT coalesce(max(version),0)+1 FROM "+c.q("versions")).Scan(&v); err != nil {
		return 0, err
	}
	schema := fmt.Sprintf("%s_v%d", c.schema, v)
	if _, err := tx.Exec(ctx, "INSERT INTO "+c.q("versions")+" (version, schema_name, topic, session_gap_s, late_s, note) VALUES ($1,$2,$3,$4,$5,$6)",
		v, schema, spec.Topic, spec.SessionGapS, spec.LateS, spec.Note); err != nil {
		return 0, err
	}
	for p, end := range endOffsets {
		if end <= 0 {
			continue
		}
		if _, err := tx.Exec(ctx, "INSERT INTO "+c.q("jobs")+" (version, partition, end_offset) VALUES ($1,$2,$3)", v, p, end); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	// Create the version's output tables now, so cutover and diff never meet a
	// half-created schema.
	st, err := consume.Open(ctx, c.dsn, schema, consume.Config{})
	if err != nil {
		return 0, err
	}
	st.Close()
	return v, nil
}

// Claim leases the next pending job, or a running job whose lease has expired.
func (c *Control) Claim(ctx context.Context, version int, worker string, lease time.Duration) (Job, error) {
	var j Job
	var gap, late int
	err := c.pool.QueryRow(ctx, fmt.Sprintf(`
		UPDATE %[1]s j SET lease_owner = $2, lease_until = now() + $3 * interval '1 millisecond',
		    token = j.token + 1, claims = j.claims + 1, status = 'running'
		FROM %[2]s v
		WHERE v.version = j.version AND j.id = (
		    SELECT id FROM %[1]s
		    WHERE version = $1 AND (status = 'pending' OR (status = 'running' AND lease_until < now()))
		    ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1)
		RETURNING j.id, j.version, j.partition, j.end_offset, j.token, v.topic, v.schema_name, v.session_gap_s, v.late_s`,
		c.q("jobs"), c.q("versions")), version, worker, lease.Milliseconds()).
		Scan(&j.ID, &j.Version, &j.Partition, &j.EndOffset, &j.Token, &j.Topic, &j.Schema, &gap, &late)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNoJob
	}
	j.Config = consume.Config{GapUS: int64(gap) * 1_000_000, LateUS: int64(late) * 1_000_000}
	return j, err
}

// Heartbeat extends the lease; ErrLeaseLost means another worker holds the job now.
func (c *Control) Heartbeat(ctx context.Context, j Job, lease time.Duration) error {
	tag, err := c.pool.Exec(ctx, "UPDATE "+c.q("jobs")+
		" SET lease_until = now() + $3 * interval '1 millisecond' WHERE id = $1 AND token = $2 AND status = 'running'",
		j.ID, j.Token, lease.Milliseconds())
	if err == nil && tag.RowsAffected() == 0 {
		err = ErrLeaseLost
	}
	return err
}

// Complete marks the job done if the caller still holds it, and the version ready
// once every job is done.
func (c *Control) Complete(ctx context.Context, j Job) error {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	tag, err := tx.Exec(ctx, "UPDATE "+c.q("jobs")+" SET status = 'done', lease_owner = NULL, lease_until = NULL WHERE id = $1 AND token = $2 AND status = 'running'", j.ID, j.Token)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(`
		UPDATE %[1]s SET state = 'ready'
		WHERE version = $1 AND state = 'building'
		  AND NOT EXISTS (SELECT 1 FROM %[2]s WHERE version = $1 AND status <> 'done')`, c.q("versions"), c.q("jobs")), j.Version); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Remaining reports whether the version still has jobs that are not done.
func (c *Control) Remaining(ctx context.Context, version int) (int, error) {
	var n int
	err := c.pool.QueryRow(ctx, "SELECT count(*) FROM "+c.q("jobs")+" WHERE version = $1 AND status <> 'done'", version).Scan(&n)
	return n, err
}

func (c *Control) Status(ctx context.Context, version int) (Status, error) {
	s := Status{Version: version}
	var diff *string
	err := c.pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT v.schema_name, v.state, v.diff_path, count(j.id), count(j.id) FILTER (WHERE j.status = 'done'), coalesce(sum(j.claims),0)
		FROM %[1]s v LEFT JOIN %[2]s j USING (version) WHERE v.version = $1 GROUP BY 1, 2, 3`, c.q("versions"), c.q("jobs")), version).
		Scan(&s.Schema, &s.State, &diff, &s.Jobs, &s.Done, &s.Claims)
	if diff != nil {
		s.DiffPath = *diff
	}
	return s, err
}

// Live returns the version the live views point at, or 0.
func (c *Control) Live(ctx context.Context) (int, error) {
	var v int
	err := c.pool.QueryRow(ctx, "SELECT coalesce(max(version),0) FROM "+c.q("versions")+" WHERE state = 'live'").Scan(&v)
	return v, err
}

// MarkDiffed records where the diff report for a ready version was written.
func (c *Control) MarkDiffed(ctx context.Context, version int, path string) error {
	tag, err := c.pool.Exec(ctx, "UPDATE "+c.q("versions")+" SET diff_path = $2 WHERE version = $1 AND state = 'ready'", version, path)
	if err == nil && tag.RowsAffected() == 0 {
		err = fmt.Errorf("version %d is not ready", version)
	}
	return err
}

// Cutover points the live views at version in one transaction and retires the
// previous live version. It refuses a version that is not complete or not diffed.
func (c *Control) Cutover(ctx context.Context, version int) error {
	st, err := c.Status(ctx, version)
	if err != nil {
		return err
	}
	if st.State != "ready" {
		return fmt.Errorf("version %d is %s, not ready", version, st.State)
	}
	if st.DiffPath == "" {
		return fmt.Errorf("version %d has no recorded diff against the live version; run diff first", version)
	}
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	live := pgx.Identifier{c.live}.Sanitize()
	if _, err := tx.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+live); err != nil {
		return err
	}
	for _, t := range OutputTables {
		if _, err := tx.Exec(ctx, fmt.Sprintf("CREATE OR REPLACE VIEW %s AS SELECT * FROM %s",
			pgx.Identifier{c.live, t}.Sanitize(), pgx.Identifier{st.Schema, t}.Sanitize())); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, "UPDATE "+c.q("versions")+" SET state = 'retired' WHERE state = 'live'"); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "UPDATE "+c.q("versions")+" SET state = 'live', live_at = now() WHERE version = $1", version); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
