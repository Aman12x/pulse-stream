// Command backfill rebuilds output for a new logic version from the Kafka log.
//
//	backfill plan    -topic T -gap S -late S [-note N]   register a version and its jobs
//	backfill work    -version V [-worker W]              take jobs under a lease until none remain
//	backfill diff    -version V [-against U] -out F      compare with the live (or another) version
//	backfill cutover -version V                          point the live views at V
//	backfill status  -version V
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Aman12x/pulse-stream/internal/backfill"
	"github.com/Aman12x/pulse-stream/internal/codec"
	"github.com/Aman12x/pulse-stream/internal/consume"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

var (
	dsn     = env("PG_DSN", "postgres://pulse:pulse@localhost:5433/pulse?sslmode=disable")
	brokers = strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ",")
	control = env("BACKFILL_SCHEMA", "backfill")
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: backfill plan|work|diff|cutover|status [flags]")
		os.Exit(1)
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	c, err := backfill.Open(ctx, dsn, control)
	must(err)
	defer c.Close()

	args := os.Args[2:]
	switch os.Args[1] {
	case "plan":
		plan(ctx, c, args)
	case "work":
		work(ctx, c, log, args)
	case "diff":
		diff(ctx, c, args)
	case "cutover":
		fs := flag.NewFlagSet("cutover", flag.ExitOnError)
		v := fs.Int("version", 0, "version")
		must(fs.Parse(args))
		must(c.Cutover(ctx, *v))
		fmt.Printf("live views now point at version %d\n", *v)
	case "status":
		fs := flag.NewFlagSet("status", flag.ExitOnError)
		v := fs.Int("version", 0, "version")
		must(fs.Parse(args))
		st, err := c.Status(ctx, *v)
		must(err)
		b, _ := json.MarshalIndent(st, "", "  ")
		fmt.Println(string(b))
	default:
		must(fmt.Errorf("unknown command %q", os.Args[1]))
	}
}

func plan(ctx context.Context, c *backfill.Control, args []string) {
	fs := flag.NewFlagSet("plan", flag.ExitOnError)
	topic := fs.String("topic", "", "source topic")
	gap := fs.Int("gap", 1800, "session gap, seconds")
	late := fs.Int("late", 300, "lateness bound, seconds")
	note := fs.String("note", "", "what changed in this version")
	must(fs.Parse(args))
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	must(err)
	defer cl.Close()
	ends, err := kadm.NewClient(cl).ListEndOffsets(ctx, *topic)
	must(err)
	end := map[int32]int64{}
	var total int64
	ends.Each(func(o kadm.ListedOffset) { end[o.Partition] = o.Offset; total += o.Offset })
	v, err := c.Plan(ctx, backfill.Spec{Topic: *topic, SessionGapS: *gap, LateS: *late, Note: *note}, end)
	must(err)
	fmt.Printf("version %d planned: %d partitions, %d offsets\n", v, len(end), total)
}

func work(ctx context.Context, c *backfill.Control, log *slog.Logger, args []string) {
	fs := flag.NewFlagSet("work", flag.ExitOnError)
	version := fs.Int("version", 0, "version")
	worker := fs.String("worker", fmt.Sprintf("w-%d", os.Getpid()), "worker id")
	lease := fs.Duration("lease", 10*time.Second, "lease length; renewed after every batch")
	maxRecs := fs.Int("batch", 2000, "records per batch")
	delay := fs.Duration("delay", 0, "pause after each batch (test knob)")
	must(fs.Parse(args))
	log = log.With("worker", *worker, "version", *version)

	var jobs, batches, fenced, lost int
	for ctx.Err() == nil {
		j, err := c.Claim(ctx, *version, *worker, *lease)
		if errors.Is(err, backfill.ErrNoJob) {
			left, err := c.Remaining(ctx, *version)
			must(err)
			if left == 0 {
				break
			}
			time.Sleep(time.Second) // jobs held by others; take them over if their leases expire
			continue
		}
		must(err)
		jobs++
		log.Info("claimed job", "job", j.ID, "partition", j.Partition, "token", j.Token, "end_offset", j.EndOffset)
		n, outcome := runJob(ctx, c, log, j, *lease, *maxRecs, *delay)
		batches += n
		switch outcome {
		case "fenced":
			fenced++
		case "lease_lost":
			lost++
		}
		log.Info("job ended", "job", j.ID, "outcome", outcome, "batches", n)
	}
	log.Info("worker done", "jobs", jobs, "batches", batches, "fenced", fenced, "lease_lost", lost)
}

// runJob applies one partition's range. It returns the batches applied and why it stopped.
func runJob(ctx context.Context, c *backfill.Control, log *slog.Logger, j backfill.Job, lease time.Duration, maxRecs int, delay time.Duration) (int, string) {
	store, err := consume.Open(ctx, dsn, j.Schema, j.Config)
	must(err)
	defer store.Close()
	gen, next, err := store.Claim(ctx, j.Topic, j.Partition)
	must(err)
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{j.Topic: {j.Partition: kgo.NewOffset().At(next)}}))
	must(err)
	defer cl.Close()

	batches := 0
	for next < j.EndOffset {
		fs := cl.PollRecords(ctx, maxRecs)
		if ctx.Err() != nil {
			return batches, "interrupted"
		}
		var recs []consume.Record
		fs.EachRecord(func(r *kgo.Record) {
			if r.Offset >= j.EndOffset {
				return
			}
			rec := consume.Record{Offset: r.Offset}
			if ev, _, err := codec.Decode(r.Value); err != nil {
				rec.Malformed = true
			} else {
				rec.Event = ev
			}
			recs = append(recs, rec)
		})
		if len(recs) == 0 {
			continue
		}
		if _, err := store.Apply(ctx, j.Topic, j.Partition, gen, recs); errors.Is(err, consume.ErrFenced) {
			log.Warn("fenced", "job", j.ID, "partition", j.Partition, "generation", gen)
			return batches, "fenced"
		} else if err != nil {
			must(err)
		}
		batches++
		next = recs[len(recs)-1].Offset + 1
		if err := c.Heartbeat(ctx, j, lease); errors.Is(err, backfill.ErrLeaseLost) {
			log.Warn("lease lost", "job", j.ID)
			return batches, "lease_lost"
		} else if err != nil {
			must(err)
		}
		if delay > 0 {
			time.Sleep(delay)
		}
	}
	if err := c.Complete(ctx, j); errors.Is(err, backfill.ErrLeaseLost) {
		log.Warn("lease lost before completion", "job", j.ID)
		return batches, "lease_lost"
	} else if err != nil {
		must(err)
	}
	return batches, "done"
}

func diff(ctx context.Context, c *backfill.Control, args []string) {
	fs := flag.NewFlagSet("diff", flag.ExitOnError)
	version := fs.Int("version", 0, "candidate version")
	against := fs.Int("against", 0, "baseline version (default: the live one)")
	out := fs.String("out", "", "write the report here")
	must(fs.Parse(args))
	if *against == 0 {
		v, err := c.Live(ctx)
		must(err)
		*against = v
	}
	cand, err := c.Status(ctx, *version)
	must(err)
	pool, err := pgxpool.New(ctx, dsn)
	must(err)
	defer pool.Close()

	report := map[string]any{"measured_at": time.Now().UTC().Format(time.RFC3339), "candidate": summarize(ctx, pool, c, *version, cand.Schema)}
	if *against > 0 {
		base, err := c.Status(ctx, *against)
		must(err)
		report["baseline"] = summarize(ctx, pool, c, *against, base.Schema)
		a, b := ident(base.Schema), ident(cand.Schema)
		rows := map[string]any{}
		for _, t := range backfill.OutputTables {
			rows[t] = map[string]int64{
				"only_in_baseline":  scalar(ctx, pool, fmt.Sprintf("SELECT count(*) FROM (SELECT * FROM %s.%s EXCEPT ALL SELECT * FROM %s.%s) x", a, t, b, t)),
				"only_in_candidate": scalar(ctx, pool, fmt.Sprintf("SELECT count(*) FROM (SELECT * FROM %s.%s EXCEPT ALL SELECT * FROM %s.%s) x", b, t, a, t)),
			}
		}
		report["rows_differing"] = rows
	} else {
		report["baseline"] = "none: nothing is live yet"
	}
	b, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(b))
	if *out != "" {
		must(os.WriteFile(*out, append(b, '\n'), 0o644))
		must(c.MarkDiffed(ctx, *version, *out))
	}
}

func summarize(ctx context.Context, pool *pgxpool.Pool, c *backfill.Control, v int, schema string) map[string]any {
	s := ident(schema)
	return map[string]any{
		"version":                v,
		"schema":                 schema,
		"events_in_aggregates":   scalar(ctx, pool, "SELECT coalesce(sum(events),0) FROM "+s+".engagement_minute"),
		"late_events":            scalar(ctx, pool, "SELECT coalesce(sum(events),0) FROM "+s+".late_minute"),
		"unique_events":          scalar(ctx, pool, "SELECT count(*) FROM "+s+".seen_events"),
		"closed_sessions":        scalar(ctx, pool, "SELECT count(*) FROM "+s+".sessions"),
		"open_sessions":          scalar(ctx, pool, "SELECT count(*) FROM "+s+".account_state"),
		"median_session_seconds": scalar(ctx, pool, "SELECT coalesce(round(percentile_cont(0.5) WITHIN GROUP (ORDER BY (end_us-start_us)/1e6)),0)::bigint FROM "+s+".sessions"),
		"median_session_events":  scalar(ctx, pool, "SELECT coalesce(round(percentile_cont(0.5) WITHIN GROUP (ORDER BY events)),0)::bigint FROM "+s+".sessions"),
		"config":                 config(ctx, pool, c, v),
	}
}

func config(ctx context.Context, pool *pgxpool.Pool, c *backfill.Control, v int) string {
	var gap, late int
	var note string
	must(pool.QueryRow(ctx, "SELECT session_gap_s, late_s, note FROM "+ident(control)+".versions WHERE version = $1", v).Scan(&gap, &late, &note))
	return "gap " + strconv.Itoa(gap) + "s, late " + strconv.Itoa(late) + "s; " + note
}

func scalar(ctx context.Context, pool *pgxpool.Pool, q string) int64 {
	var n int64
	must(pool.QueryRow(ctx, q).Scan(&n))
	return n
}

func ident(s string) string { return pgx.Identifier{s}.Sanitize() }

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "backfill:", err)
		os.Exit(1)
	}
}
